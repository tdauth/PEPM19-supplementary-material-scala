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
case class FixStateLinkWithoutCallback[T](links: Set[CCASFixedPromiseLinking[T]]) extends FixedState[T]

/**
  * Similiar to [[CCASPromiseLinking]] but does not simply move all callbacks to the root promise.
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
  * TODO This behaviour does not compress the chain since `f2` will still be linked to `f1` and not directly to `f0`. Hence,
  * we have more promises in the chain.
  */
class CCASFixedPromiseLinking[T](ex: Executor) extends AtomicReference[FixedState[T]](FixedStateCallbackEntry[T](Noop)) with FP[T] {
  type Self = CCASFixedPromiseLinking[T]

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] with FP[S] = new CCASFixedPromiseLinking[S](ex)

  override def getC(): Try[T] = getResultWithMVar()

  // TODO Add @tailrec somehow with parent entry
  private def executeEachCallbackWithParent(v: Try[T], callbacks: CallbackEntry): Unit =
    callbacks match {
      case LinkedCallbackEntry(_, prev) =>
        getExecutorC.execute(() => callbacks.asInstanceOf[LinkedCallbackEntry[T]].c(v))
        executeEachCallbackWithParent(v, prev)
      case SingleCallbackEntry(_) =>
        getExecutorC.execute(() => callbacks.asInstanceOf[SingleCallbackEntry[T]].c(v))
      case Noop =>
      case ParentCallbackEntry(left, right) => {
        executeEachCallbackWithParent(v, left)
        executeEachCallbackWithParent(v, right)
      }
    }

  // TODO Add @tailrec somehow with parent entry
  private def executeAllCallbacksWithParent(v: Try[T], callbacks: CallbackEntry): Unit =
    callbacks match {
      case LinkedCallbackEntry(_, prev) =>
        callbacks.asInstanceOf[LinkedCallbackEntry[T]].c(v)
        executeAllCallbacksWithParent(v, prev)
      case SingleCallbackEntry(_) =>
        callbacks.asInstanceOf[SingleCallbackEntry[T]].c(v)
      case Noop =>
      case ParentCallbackEntry(left, right) => {
        executeAllCallbacksWithParent(v, left)
        executeAllCallbacksWithParent(v, right)
      }
    }

  private def tryCompleteExecuteAllTogether(v: Try[T]) = {
    val callbackEntryAndSuccessfullyCompleted =
      CCASFixedPromiseLinking.tryCompleteAndGetEachCallback[T](this, Noop, v, Set(), false)
    val result = callbackEntryAndSuccessfullyCompleted._1
    val callbackEntry = callbackEntryAndSuccessfullyCompleted._2
    if (callbackEntry ne Noop) getExecutorC.execute(() => executeAllCallbacksWithParent(v, callbackEntry))
    result
  }

  override def tryCompleteC(v: Try[T]): Boolean = tryCompleteExecuteAllTogether(v)
// TODO Execute all together or just each one?
  //CCASFixedPromiseLinking.tryCompleteAndExecuteEachCallback(this, v, Set[Self](), false)

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
      case FixStateLinkWithoutCallback(links) => if (!compareAndSet(s, FixedStateLink(links, SingleCallbackEntry(c)))) onCompleteInternal(c)
    }
  }

  /**
    * If other is this type, this will be added to the set of links of other.
    * @param other
    */
  @inline @tailrec private final def tryCompleteWithInternal(other: FP[T]): Unit = {
    if (other.isInstanceOf[Self]) {
      val o = other.asInstanceOf[Self]
      val s = o.get
      s match {
        case FixedStateTry(x) => tryComplete(x)
        case FixedStateCallbackEntry(x) =>
          if (x eq Noop) {
            // Replace the callback list by a link to this which still holds the callback.
            if (!o.compareAndSet(s, FixStateLinkWithoutCallback[T](Set(this)))) tryCompleteWithInternal(other)
          } else {
            // Replace the callback list by a link to this which still holds the callback.
            if (!o.compareAndSet(s, FixedStateLink[T](Set(this), x))) tryCompleteWithInternal(other)
          }
        // Add this as additional target for the link.
        case FixedStateLink(links, c)           => if (!o.compareAndSet(s, FixedStateLink[T](links + this, c))) tryCompleteWithInternal(other)
        case FixStateLinkWithoutCallback(links) => if (!o.compareAndSet(s, FixStateLinkWithoutCallback[T](links + this))) tryCompleteWithInternal(other)
      }
    } else {
      other.onComplete(tryComplete)
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
      case FixedStateLink(links, _)           => links.contains(primCASPromiseLinking)
      case FixStateLinkWithoutCallback(links) => links.contains(primCASPromiseLinking)
      case _                                  => false
    }

  private[pepm19] def isLink(): Boolean =
    get match {
      case FixedStateLink(_, _) | FixStateLinkWithoutCallback(_) => true
      case _                                                     => false
    }

  private[pepm19] def getLinkTo(): Set[Self] =
    get match {
      case FixedStateLink(links, _)           => links
      case FixStateLinkWithoutCallback(links) => links
      case _                                  => throw new RuntimeException("Invalid usage.")
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

  @inline @tailrec private final def tryCompleteAndExecuteEachCallback[T](current: CCASFixedPromiseLinking[T]#Self,
                                                                          v: Try[T],
                                                                          rest: Set[CCASFixedPromiseLinking[T]#Self],
                                                                          successfullyCompletedFirst: Boolean): Boolean = {
    val s = current.get()
    s match {
      case FixedStateTry(_) =>
        if (!rest.isEmpty) { tryCompleteAndExecuteEachCallback(rest.head, v, rest.tail, successfullyCompletedFirst) } else { successfullyCompletedFirst }
      case FixedStateCallbackEntry(c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          current.executeEachCallbackWithParent(v, c)
          // The first successful compareAndSet must be the first promise, therefore return true from here!
          if (!rest.isEmpty) { tryCompleteAndExecuteEachCallback(rest.head, v, rest.tail, true) } else {
            true
          }
        } else { tryCompleteAndExecuteEachCallback(current, v, rest, successfullyCompletedFirst) }
      case FixedStateLink(links, c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          current.executeEachCallbackWithParent(v, c)
          val updatedRest = rest ++ links
          /*
           * Updated rest should never be empty since links should never be empty.
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          tryCompleteAndExecuteEachCallback(updatedRest.head, v, updatedRest.tail, true)
        } else {
          tryCompleteAndExecuteEachCallback(current, v, rest, successfullyCompletedFirst)
        }
      case FixStateLinkWithoutCallback(links) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          val updatedRest = rest ++ links
          /*
           * Updated rest should never be empty since links should never be empty-
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          tryCompleteAndExecuteEachCallback(updatedRest.head, v, updatedRest.tail, true)
        } else {
          tryCompleteAndExecuteEachCallback(current, v, rest, successfullyCompletedFirst)
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
        if (!rest.isEmpty) tryCompleteAndGetEachCallback(rest.head, currentCallbackEntry, v, rest.tail, successfullyCompletedFirst)
        else { (successfullyCompletedFirst, currentCallbackEntry) }
      case FixedStateCallbackEntry(c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          val updatedCurrentCallbackEntry = ParentCallbackEntry(c, currentCallbackEntry)
          // The first successful compareAndSet must be the first promise, therefore return true from here!
          if (!rest.isEmpty) { tryCompleteAndGetEachCallback(rest.head, updatedCurrentCallbackEntry, v, rest.tail, true) } else {
            (true, updatedCurrentCallbackEntry)
          }
        } else { tryCompleteAndGetEachCallback(current, currentCallbackEntry, v, rest, successfullyCompletedFirst) }
      case FixedStateLink(links, c) =>
        if (current.compareAndSet(s, FixedStateTry(v))) {
          current.executeEachCallbackWithParent(v, c)
          val updatedRest = rest ++ links
          /*
           * Updated rest should never be empty since links should never be empty-
           * The first successful compareAndSet must be the first promise, therefore return true from here!
           */
          tryCompleteAndGetEachCallback(updatedRest.head, currentCallbackEntry, v, updatedRest.tail, true)
        } else {
          tryCompleteAndGetEachCallback(current, currentCallbackEntry, v, rest, successfullyCompletedFirst)
        }
      case FixStateLinkWithoutCallback(links) =>
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
