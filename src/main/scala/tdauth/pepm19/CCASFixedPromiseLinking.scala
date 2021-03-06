package tdauth.pepm19

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.Try

sealed trait FixedState[T]
case class FixedStateTry[T](t: Try[T]) extends FixedState[T]
case class FixedStateLink[T](links: Set[CCASFixedPromiseLinking[T]], c: CallbackEntry) extends FixedState[T]

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
  *
  * @param aggregateCallbacks If this value is false, the callbacks will not be aggregated but executed immediately.
  */
class CCASFixedPromiseLinking[T](ex: Executor, aggregateCallbacks: Boolean, maxAggregatedCallbacks: Int)
    extends AtomicReference[FixedState[T]](FixedStateLink[T](Set(), Noop))
    with FP[T] {
  type Self = CCASFixedPromiseLinking[T]

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] with FP[S] = new CCASFixedPromiseLinking[S](ex, aggregateCallbacks, maxAggregatedCallbacks)

  override def getC(): Try[T] = getResultWithMVar()

  // TODO Add @tailrec somehow with parent entry
  private def executeAllCallbacksWithParent(v: Try[T], callbacks: CallbackEntry): Unit =
    callbacks match {
      case LinkedCallbackEntry(_, prev) =>
        callbacks.asInstanceOf[LinkedCallbackEntry[T]].c(v)
        executeAllCallbacksWithParent(v, prev)
      case SingleCallbackEntry(_) =>
        callbacks.asInstanceOf[SingleCallbackEntry[T]].c(v)
      case Noop => throw new RuntimeException("Never pass Noop to aggregate callbacks!")
      case ParentCallbackEntry(left, right) =>
        executeAllCallbacksWithParent(v, left)
        executeAllCallbacksWithParent(v, right)
    }

  /**
    * Aggregates all callbacks and separates them into groups of [[LinkedAggregate]] or returns one single [[SingleAggregate]].
    * Each aggregate of callbacks can be submitted to the executor separately.
    */
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
      case Noop => throw new RuntimeException("Never pass Noop to aggregate callbacks!")
      case ParentCallbackEntry(left, right) =>
        val (updatedCurrent, updatedCounter, updatedResult) = aggregateCallbacks(left, current, currentCounter, result)
        aggregateCallbacks(right, updatedCurrent, updatedCounter, updatedResult)
    }

  @tailrec @inline private def executeAggregated(v: Try[T], a: AggregatedCallback): Unit = a match {
    case SingleAggregate(c) => getExecutorC.execute(() => executeAllCallbacksWithParent(v, c))
    case LinkedAggregate(c, prev) =>
      getExecutorC.execute(() => executeAllCallbacksWithParent(v, c))
      executeAggregated(v, prev)
    case NoopAggregated => throw new RuntimeException("Never pass Noop to execute aggregated callbacks!")
  }

  @inline private def aggregateAndExecuteAllCallbacks(v: Try[T], currentCallbacks: CallbackEntry) {
    val (updatedCurrent, _, updatedResult) = aggregateCallbacks(currentCallbacks, Noop, 0, NoopAggregated)
    val aggregatedCallbacks =
      if (updatedResult ne NoopAggregated) { LinkedAggregate(updatedCurrent, updatedResult) } else { new SingleAggregate(updatedCurrent) }
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

  private def tryCompleteExecuteAllPerLink(v: Try[T]) = CCASFixedPromiseLinking.tryCompleteAndExecuteCallbacks(this, v, Set[Self](), false)

  override def tryCompleteC(v: Try[T]): Boolean = if (aggregateCallbacks) { tryCompleteExecuteAllTogether(v) } else { tryCompleteExecuteAllPerLink(v) }

  override def onCompleteC(c: Callback): Unit = onCompleteInternal(c)

  override def tryCompleteWith(other: FP[T]): Unit = tryCompleteWithInternal(other)

  @inline @tailrec private def onCompleteInternal(c: Callback): Unit = {
    val s = get
    s match {
      case FixedStateTry(x) => executeCallback(x, c)
      case FixedStateLink(links, callbackEntry) =>
        if (!compareAndSet(s, FixedStateLink(links, prependCallback(callbackEntry, c)))) { onCompleteInternal(c) }
    }
  }

  /**
    * If other is this type, this will be added to the set of links of other.
    * Otherwise, the default implementation of [[FP#tryCompleteWith]] will be used.
    */
  @inline @tailrec private final def tryCompleteWithInternal(other: FP[T]): Unit =
    if (other.isInstanceOf[Self]) {
      val o = other.asInstanceOf[Self]
      val s = o.get
      s match {
        case FixedStateTry(x) => tryComplete(x)
        // Add this as additional link.
        case FixedStateLink(links, c) => if (!o.compareAndSet(s, FixedStateLink[T](links + this, c))) { tryCompleteWithInternal(other) }
      }
    } else {
      super.tryCompleteWith(other)
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
      case FixedStateLink(links, _) => links.contains(primCASPromiseLinking)
      case _                        => false
    }

  private[pepm19] def isLink(): Boolean =
    get match {
      case FixedStateLink(_, _) => true
      case _                    => false
    }

  private[pepm19] def getLinkTo(): Set[Self] =
    get match {
      case FixedStateLink(links, _) => links
      case _                        => throw new RuntimeException("Invalid usage.")
    }

  private[pepm19] def getNumberOfCallbacks(): Int = get match {
    case FixedStateLink(_, x) => getNumberOfCallbacks(x)
    case _                    => throw new RuntimeException("Is not a list of callbacks.")
  }

  private def getNumberOfCallbacks(c: CallbackEntry): Int = c match {
    case SingleCallbackEntry(_)           => 1
    case ParentCallbackEntry(left, right) => getNumberOfCallbacks(left) + getNumberOfCallbacks(right)
    case Noop                             => 0
    case LinkedCallbackEntry(_, prev)     => 1 + getNumberOfCallbacks(prev)
  }
}

object CCASFixedPromiseLinking {

  @inline @tailrec private final def tryCompleteAndExecuteCallbacks[T](current: CCASFixedPromiseLinking[T]#Self,
                                                                       v: Try[T],
                                                                       rest: Set[CCASFixedPromiseLinking[T]#Self],
                                                                       successfullyCompletedFirst: Boolean): Boolean = {
    val s = current.get()
    s match {
      case FixedStateTry(_) =>
        if (rest.nonEmpty) { tryCompleteAndExecuteCallbacks(rest.head, v, rest.tail, successfullyCompletedFirst) } else {
          successfullyCompletedFirst
        }
      case FixedStateLink(links, c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          if (c ne Noop) { current.getExecutorC.execute(() => current.executeAllCallbacksWithParent(v, c)) }
          val updatedRest = rest ++ links
          /*
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          if (updatedRest.nonEmpty) { tryCompleteAndExecuteCallbacks(updatedRest.head, v, updatedRest.tail, true) } else {
            true
          }
        } else {
          tryCompleteAndExecuteCallbacks(current, v, rest, successfullyCompletedFirst)
        }
    }
  }

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
      case FixedStateLink(links, c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          val updatedCurrentCallbackEntry = if ((currentCallbackEntry ne Noop) && (c ne Noop)) { ParentCallbackEntry(c, currentCallbackEntry) } else if (c ne Noop) {
            c
          } else { currentCallbackEntry }
          val updatedRest = rest ++ links
          /*
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          if (updatedRest.nonEmpty) { tryCompleteAndGetEachCallback(updatedRest.head, updatedCurrentCallbackEntry, v, updatedRest.tail, true) } else {
            (true, updatedCurrentCallbackEntry)
          }
        } else {
          tryCompleteAndGetEachCallback(current, currentCallbackEntry, v, rest, successfullyCompletedFirst)
        }
    }
  }
}
