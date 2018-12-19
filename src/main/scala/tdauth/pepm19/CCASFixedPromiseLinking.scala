package tdauth.pepm19

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.Try

sealed trait FixedState[T]
case class FixedStateTry[T](t: Try[T]) extends FixedState[T]
case class FixedStateCallbackEntry[T](c: CallbackEntry) extends FixedState[T]
case class FixedStateLink[T](links: Set[CCASFixedPromiseLinking[T]], c: CallbackEntry) extends FixedState[T]

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

  override def newC[S](ex: Executor): Core[S] = new CCASFixedPromiseLinking[S](ex)

  override def getC(): Try[T] = getResultWithMVar()

  override def tryCompleteC(v: Try[T]): Boolean = tryCompleteInternal(v)

  override def onCompleteC(c: Callback): Unit = onCompleteInternal(c)

  override def tryCompleteWith(other: FP[T]): Unit = tryCompleteWithInternal(other)

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

  @inline @tailrec private def tryCompleteInternal(v: Try[T]): Boolean = {
    val s = get
    s match {
      case FixedStateTry(_) => false
      case FixedStateCallbackEntry(x) => {
        if (compareAndSet(s, FixedStateTry(v))) {
          executeEachCallbackWithParent(v, x)
          true
        } else {
          tryCompleteInternal(v)
        }
      }
      case FixedStateLink(links, c) => {
        if (compareAndSet(s, FixedStateTry(v))) {

          /**
            * We collect all the callbacks from the target promises by completing them.
            * Already completed promises and promises without callbacks will be ignored.
            */
          val targetCallbacks = tryCompleteAllAndGetCallbacks(links, v)

          /**
            * Submit all collected callbacks together with the ones of this.
            */
          if (targetCallbacks.isDefined) {
            executeEachCallbackWithParent(v, ParentCallbackEntry(c, targetCallbacks.get))
          } else {
            executeEachCallbackWithParent(v, c)
          }
          true
        } else {
          tryCompleteInternal(v)
        }
      }
    }
  }

  /**
    * Completes one promise after another and collects all callbacks from them.
    * Already completed promises or promises without callbacks are ignored.
    * @param promises The set of links which link to the promises which have to be completed.
    * @param v The result value which has to be used for completion.
    * @return The parent callback entry which links all callbacks from all completed promises together.
    */
  private def tryCompleteAllAndGetCallbacks(promises: Set[Self], v: Try[T]): Option[CallbackEntry] = {
    // TODO leads to stack overflows with intensive linking! Try to call this method recursively? Starts already at 1000 promises, so maybe a bug in the implementation?
    // TODO This could be done concurrently!
    val flattened = promises.flatMap(_ tryCompleteAndGetCallback (v))
    if (flattened.isEmpty) { None } else {
      Some(flattened.reduce((c0, c1) => ParentCallbackEntry(c0, c1)))
    }
  }

  /**
    * Tries to complete the promise and returns its callback entry.
    * If it is already completed or has no callback entry/[[Noop]], the result is `None`.
    */
  @inline @tailrec private def tryCompleteAndGetCallback(v: Try[T]): Option[CallbackEntry] = {
    val s = get
    s match {
      case FixedStateTry(_) => None
      case FixedStateCallbackEntry(x) =>
        if (compareAndSet(s, FixedStateTry(v))) {
          if (x ne Noop) {
            Some(x)
          } else { None }
        } else { tryCompleteAndGetCallback(v) }
      case FixedStateLink(links, c) => {
        if (compareAndSet(s, FixedStateTry(v))) {
          val targetCallbacks = tryCompleteAllAndGetCallbacks(links, v)

          if (targetCallbacks.isDefined) {
            Some(ParentCallbackEntry(c, targetCallbacks.get))
          } else {
            Some(c)
          }
        } else {
          tryCompleteAndGetCallback(v)
        }
      }
    }
  }

  @inline @tailrec private def onCompleteInternal(c: Callback): Unit = {
    val s = get
    s match {
      case FixedStateTry(x)           => executeCallback(x, c)
      case FixedStateCallbackEntry(x) => if (!compareAndSet(s, FixedStateCallbackEntry(prependCallback(x, c)))) onCompleteInternal(c)
      // Just replace the callback entry in the current link. Do not move any callbacks to target promises.
      case FixedStateLink(links, callbackEntry) =>
        if (!compareAndSet(s, FixedStateLink(links, LinkedCallbackEntry(c, callbackEntry)))) onCompleteInternal(c)
    }
  }

  @inline @tailrec private def tryCompleteWithInternal(other: FP[T]): Unit = {
    if (other.isInstanceOf[Self]) {
      val o = other.asInstanceOf[Self]
      val s = o.get
      s match {
        case FixedStateTry(x) => tryComplete(x)
        case FixedStateCallbackEntry(x) => {
          // Replace the callback list by a link to this which still holds the callback.
          if (!o.compareAndSet(s, FixedStateLink(Set(this), x))) tryCompleteWithInternal(other)
        }
        // Add this as additional target for the link.
        case FixedStateLink(links, c) => if (!o.compareAndSet(s, FixedStateLink[T](links + this, c))) tryCompleteWithInternal(other)
      }
    } else {
      other.onComplete(this.tryComplete(_))
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
