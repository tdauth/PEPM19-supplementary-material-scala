package tdauth.pepm19

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.Try

sealed trait FixedState[T]
case class FixedStateTry[T](t: Try[T]) extends FixedState[T]
case class FixedStateCallbackEntry[T](c: CallbackEntry) extends FixedState[T]
case class FixedStateLink[T](links: Set[CCASFixedPromiseLinking[T]], c: CallbackEntry) extends FixedState[T]
// This case class helps to reduce the [[Noop]] callback entries.
case class FixedStateLinkWithoutCallback[T](links: Set[CCASFixedPromiseLinking[T]]) extends FixedState[T]

sealed trait AggregatedCallback
case class SingleAggregate(c: CallbackEntry) extends AggregatedCallback
case class LinkedAggregate(c: CallbackEntry, prev: AggregatedCallback) extends AggregatedCallback
case object NoopAggregated extends AggregatedCallback

/**
  * Similar to [[CCASPromiseLinking]] but does not simply move all callbacks to the root promise.
  * It creates a link to the target promise and keeps the current list of callbacks in this link.
  * This prevents all callbacks from being submitted if only one single link is completed.
  * The implementation does still allow one link to link to multiple promises.
  *
  * In the standard case of linking:
  * ```
  * f0 tryCompleteWith f1
  * f1 tryCompleteWith f2
  * ```
  * it will set `f1` and `f2` to links which keep their callbacks separate but know that they have to complete their target, too.
  * When `f2` is finally completed, it will complete `f1` and `f0`, too and collect their callbacks. It will submit all the
  * callbacks at once.
  *
  * Note that this behaviour does not compress the chain since `f2` will still be linked to `f1` and not directly to `f0`. Hence,
  * we have more promises in the chain.
  * However, it can optimize the execution of callbacks and reduce the number of tasks + one callback per `tryCompleteWith` call is reduced.
  */
class CCASFixedPromiseLinking[T](ex: Executor, maxAggregatedCallbacks: Int)
    extends AtomicReference[FixedState[T]](FixedStateCallbackEntry[T](Noop))
    with FP[T] {
  type Self = CCASFixedPromiseLinking[T]

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] with FP[S] = new CCASFixedPromiseLinking[S](ex, maxAggregatedCallbacks)

  override def getC(): Try[T] = getResultWithMVar()

  // TODO Add @tailrec somehow with parent entry
  private def executeAllCallbacksWithParent(v: Try[T], callbacks: CallbackEntry): Unit =
    callbacks match {
      case LinkedCallbackEntry(_, prev) =>
        callbacks.asInstanceOf[LinkedCallbackEntry[T]].c(v)
        executeAllCallbacksWithParent(v, prev)
      case SingleCallbackEntry(_) =>
        callbacks.asInstanceOf[SingleCallbackEntry[T]].c(v)
      case Noop =>
      case ParentCallbackEntry(left, right) =>
        executeAllCallbacksWithParent(v, left)
        executeAllCallbacksWithParent(v, right)
    }

  // TODO Add @tailrec somehow with parent entry
  private def aggregateCallbacks(currentCallbacks: CallbackEntry,
                                 current: CallbackEntry,
                                 currentCounter: Int,
                                 result: AggregatedCallback): (CallbackEntry, Int, AggregatedCallback) =
    currentCallbacks match {
      case LinkedCallbackEntry(c, prev) =>
        if (currentCounter == maxAggregatedCallbacks) {
          aggregateCallbacks(
            prev,
            SingleCallbackEntry(c.asInstanceOf[Callback]),
            1,
            if (current eq Noop) { result } else { if (result eq NoopAggregated) { SingleAggregate(current) } else { LinkedAggregate(current, result) } }
          )
        } else {
          aggregateCallbacks(
            prev,
            if (current eq Noop) { SingleCallbackEntry(c.asInstanceOf[Callback]) } else { LinkedCallbackEntry(c.asInstanceOf[Callback], current) },
            currentCounter + 1,
            result
          )
        }
      case SingleCallbackEntry(c) =>
        if (currentCounter == maxAggregatedCallbacks) {
          (SingleCallbackEntry(c.asInstanceOf[Callback]), 1, if (current eq Noop) { result } else {
            if (result eq NoopAggregated) { SingleAggregate(current) } else { LinkedAggregate(current, result) }
          })
        } else {
          (if (current eq Noop) { SingleCallbackEntry(c.asInstanceOf[Callback]) } else { LinkedCallbackEntry(c.asInstanceOf[Callback], current) },
           currentCounter + 1,
           result)
        }
      // TODO Should never occur here!
      case Noop => throw new RuntimeException("Never pass Noop to aggregate callbacks!") //(current, currentCounter, result)
      case ParentCallbackEntry(left, right) =>
        val (updatedCurrent, updatedCounter, updatedResult) = aggregateCallbacks(left, current, currentCounter, result)
        aggregateCallbacks(right, updatedCurrent, updatedCounter, updatedResult)
    }

  @tailrec @inline private def executeAggregated(v: Try[T], a: AggregatedCallback): Unit = a match {
    case SingleAggregate(c) => getExecutorC.execute(() => executeAllCallbacksWithParent(v, c))
    case LinkedAggregate(c, prev) =>
      getExecutorC.execute(() => executeAllCallbacksWithParent(v, c))
      executeAggregated(v, prev)
    case NoopAggregated =>
  }

  @inline private def aggregateAndExecuteAllCallbacks(v: Try[T], currentCallbacks: CallbackEntry) {
    // TODO Can this return Noop?
    val (updatedCurrent, _, updatedResult) = aggregateCallbacks(currentCallbacks, Noop, 0, NoopAggregated)
    val aggregatedCallbacks = LinkedAggregate(updatedCurrent, updatedResult)
    executeAggregated(v, aggregatedCallbacks)
  }

  /**
    * Collects all callbacks including those of promises referred by links and executes them.
    * @param v The result value each callback gets as parameter.
    * @return Returns whether the promise has been completed by this call or not.
    */
  private def tryCompleteExecuteAllTogether(v: Try[T]) = {
    val callbackEntryAndSuccessfullyCompleted =
      CCASFixedPromiseLinking.tryCompleteAndGetEachCallback[T](this, Noop, v, Set(), false)
    val result = callbackEntryAndSuccessfullyCompleted._1
    if (result) {
      val callbackEntry = callbackEntryAndSuccessfullyCompleted._2
      if (callbackEntry ne Noop) {
        aggregateAndExecuteAllCallbacks(v, callbackEntry)
      }
    }
    result
  }

  override def tryCompleteC(v: Try[T]): Boolean = tryCompleteExecuteAllTogether(v)

  override def onCompleteC(c: Callback): Unit = onCompleteInternal(c)

  override def tryCompleteWith(other: FP[T]): Unit = tryCompleteWithInternal(other)

  @inline @tailrec private def onCompleteInternal(c: Callback): Unit = {
    val s = get
    s match {
      case FixedStateTry(x)           => executeCallback(x, c)
      case FixedStateCallbackEntry(x) => if (!compareAndSet(s, FixedStateCallbackEntry(prependCallback(x, c)))) onCompleteInternal(c)
      // Just replace the callback entry in the current link. Do not move any callbacks to target promises.
      case FixedStateLink(links, callbackEntry) =>
        if (!compareAndSet(s, FixedStateLink(links, LinkedCallbackEntry(c, callbackEntry)))) onCompleteInternal(c)
      case FixedStateLinkWithoutCallback(links) => if (!compareAndSet(s, FixedStateLink(links, SingleCallbackEntry(c)))) onCompleteInternal(c)
    }
  }

  /**
    * If other is this type, this will be added to the set of links of other.
    * Otherwise, the default implementation of [[FP#tryCompleteWith]] will be used.
    */
  @inline @tailrec private final def tryCompleteWithInternal(other: FP[T]): Unit = {
    if (other.isInstanceOf[Self]) {
      val o = other.asInstanceOf[Self]
      val s = o.get
      s match {
        case FixedStateTry(x) => tryComplete(x)
        case FixedStateCallbackEntry(c) =>
          if (c eq Noop) {
            // Replace the callback list by a link to this which has no callback.
            if (!o.compareAndSet(s, FixedStateLinkWithoutCallback[T](Set(this)))) tryCompleteWithInternal(other)
          } else {
            // Replace the callback list by a link to this which still holds the callback.
            if (!o.compareAndSet(s, FixedStateLink[T](Set(this), c))) tryCompleteWithInternal(other)
          }
        // Add this as additional link.
        case FixedStateLink(links, c)             => if (!o.compareAndSet(s, FixedStateLink[T](links + this, c))) tryCompleteWithInternal(other)
        case FixedStateLinkWithoutCallback(links) => if (!o.compareAndSet(s, FixedStateLinkWithoutCallback[T](links + this))) tryCompleteWithInternal(other)
      }
    } else {
      super.tryCompleteWith(other)
    }
  }

  private[pepm19] def isReady(): Boolean =
    get match {
      case FixedStateTry(_) => true
      case _                => false
    }

  /**
    * The following methods exist for tests only.
    * @param primCASPromiseLinking The target promise which this should be a direct link to.
    * @return True if this is a direct link to the target promise. Otherwise, false.
    */
  private[pepm19] def isLinkTo(primCASPromiseLinking: Self): Boolean =
    get match {
      case FixedStateLink(links, _)             => links.contains(primCASPromiseLinking)
      case FixedStateLinkWithoutCallback(links) => links.contains(primCASPromiseLinking)
      case _                                    => false
    }

  private[pepm19] def isLink(): Boolean =
    get match {
      case FixedStateLink(_, _) | FixedStateLinkWithoutCallback(_) => true
      case _                                                       => false
    }

  private[pepm19] def getLinkTo(): Set[Self] =
    get match {
      case FixedStateLink(links, _)             => links
      case FixedStateLinkWithoutCallback(links) => links
      case _                                    => throw new RuntimeException("Invalid usage.")
    }

  private[pepm19] def isListOfCallbacks(): Boolean = get match {
    case FixedStateCallbackEntry(_) => true
    case _                          => false
  }

  private[pepm19] def getNumberOfCallbacks(): Int = get match {
    case FixedStateCallbackEntry(x) => getNumberOfCallbacks(x)
    case FixedStateLink(_, x)       => getNumberOfCallbacks(x)
    case _                          => throw new RuntimeException("Is not a list of callbacks.")
  }

  private def getNumberOfCallbacks(c: CallbackEntry): Int = c match {
    case SingleCallbackEntry(_)           => 1
    case ParentCallbackEntry(left, right) => getNumberOfCallbacks(left) + getNumberOfCallbacks(right)
    case Noop                             => 0
    case LinkedCallbackEntry(_, prev)     => 1 + getNumberOfCallbacks(prev)
  }
}

object CCASFixedPromiseLinking {

  @inline @tailrec private final def tryCompleteAndGetEachCallback[T](current: CCASFixedPromiseLinking[T]#Self,
                                                                      currentCallbackEntry: CallbackEntry,
                                                                      v: Try[T],
                                                                      rest: Set[CCASFixedPromiseLinking[T]#Self],
                                                                      successfullyCompletedFirst: Boolean): (Boolean, CallbackEntry) = {
    val s = current.get()
    s match {
      case FixedStateTry(_) =>
        if (rest.nonEmpty) { tryCompleteAndGetEachCallback(rest.head, currentCallbackEntry, v, rest.tail, successfullyCompletedFirst) } else {
          (successfullyCompletedFirst, currentCallbackEntry)
        }
      case FixedStateCallbackEntry(c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          val updatedCurrentCallbackEntry = if (currentCallbackEntry ne Noop) { ParentCallbackEntry(c, currentCallbackEntry) } else { c }
          // The first successful compareAndSet must be the first promise, therefore return true from here!
          if (rest.nonEmpty) { tryCompleteAndGetEachCallback(rest.head, updatedCurrentCallbackEntry, v, rest.tail, true) } else {
            (true, updatedCurrentCallbackEntry)
          }
        } else { tryCompleteAndGetEachCallback(current, currentCallbackEntry, v, rest, successfullyCompletedFirst) }
      case FixedStateLink(links, c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          val updatedCurrentCallbackEntry = if (currentCallbackEntry ne Noop) { ParentCallbackEntry(c, currentCallbackEntry) } else { c }
          val updatedRest = rest ++ links
          /*
           * Updated rest should never be empty since links should never be empty-
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          tryCompleteAndGetEachCallback(updatedRest.head, updatedCurrentCallbackEntry, v, updatedRest.tail, true)
        } else {
          tryCompleteAndGetEachCallback(current, currentCallbackEntry, v, rest, successfullyCompletedFirst)
        }
      case FixedStateLinkWithoutCallback(links) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          val updatedRest = rest ++ links
          /*
           * Updated rest should never be empty since links should never be empty-
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          tryCompleteAndGetEachCallback(updatedRest.head, currentCallbackEntry, v, updatedRest.tail, true)
        } else {
          tryCompleteAndGetEachCallback(current, currentCallbackEntry, v, rest, successfullyCompletedFirst)
        }
    }
  }
}
