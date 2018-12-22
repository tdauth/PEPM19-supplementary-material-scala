package tdauth.pepm19

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.Try

sealed trait LinkedState
case class LinkedStateTry[T](t: Try[T]) extends LinkedState
case class LinkedStateCallbackEntry(c: CallbackEntry) extends LinkedState
case class LinkedStateLink[T](l: CCASPromiseLinking[T]) extends LinkedState

/**
  * Similar to [[CCAS]] but implements [[FP#tryCompleteWith]] with the help of promise linking optimization (implemented in Twitter Util and Scala FP).
  * Whenever two promises are equal, all callbacks are moved to one of them.
  *
  * Here is an example:
  * ```
  * f0 tryCompleteWith f1 // f0 <- f1: f1 becomes a link to f0
  * f1 tryCompleteWith f2 // f1 <- f2: f2 becomes a link to f1 which does already link to f0, so it should become a link to f0, f1 could be released by the GC
  * f2 tryCompleteWith f3 // f3 <- f2: f3 becomes a link to f2 which does already link to f0, so it should become a link to f0, f2 could be released by the GC
  * ...
  * ```
  *
  * Therefore, every `tryCompleteWith` call has to compress the chain and make the link directly link to f0 which is the root promise.
  *
  * This implementation is more similiar to Scala FP 2.12.x than to 2.13.x.
  * See [[https://github.com/scala/scala/blob/2.12.x/src/library/scala/concurrent/impl/Promise.scala DefaultPromise.scala]].
  * Scala FP 2.12.x's implementation has been influenced by Twitter Util's implementation.
  * See [[https://github.com/twitter/util/blob/master/util-core/src/main/scala/com/twitter/util/Promise.scala Twitter promise implementation]].
  */
class CCASPromiseLinking[T](ex: Executor) extends AtomicReference[LinkedState](LinkedStateCallbackEntry(Noop)) with FP[T] {

  type Self = CCASPromiseLinking[T]

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] with FP[S] = new CCASPromiseLinking[S](ex)

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
      case LinkedStateTry(_) => false
      case LinkedStateCallbackEntry(x) =>
        if (compareAndSet(s, LinkedStateTry(v))) {
          executeEachCallbackWithParent(v, x)
          true
        } else {
          tryCompleteInternal(v)
        }
      case LinkedStateLink(_) => compressRoot().tryCompleteC(v)
    }
  }

  @inline @tailrec private def onCompleteInternal(c: Callback): Unit = {
    val s = get
    s match {
      case LinkedStateTry(x)           => executeCallback(x.asInstanceOf[Try[T]], c)
      case LinkedStateCallbackEntry(x) => if (!compareAndSet(s, LinkedStateCallbackEntry(prependCallback(x, c)))) onCompleteInternal(c)
      case LinkedStateLink(_)          => compressRoot().onComplete(c)
    }
  }

  /**
    * Creates a link from other to this/the root of this and moves all callbacks to this.
    * The callback list of other is replaced by a link.
    */
  @inline @tailrec private def tryCompleteWithInternal(other: FP[T]): Unit = {
    if (other.isInstanceOf[Self]) {
      val o = other.asInstanceOf[Self]
      val s = o.get
      s match {
        case LinkedStateTry(x) => tryComplete(x.asInstanceOf[Try[T]])
        case LinkedStateCallbackEntry(x) => {
          val root = compressRoot()
          // Replace the callback list by a link to the root of the target and prepend the callbacks to the root.
          if (!o.compareAndSet(s, LinkedStateLink(root))) tryCompleteWithInternal(other) else root.onComplete(x)
        }
        case LinkedStateLink(_) => tryCompleteWithInternal(o.compressRoot())
      }
    } else {
      other.onComplete(tryComplete)
    }
  }

  private def prependCallbacks(callbacks: CallbackEntry, prependedCallbacks: CallbackEntry): CallbackEntry =
    if (callbacks ne Noop) { ParentCallbackEntry(prependedCallbacks, callbacks) } else { prependedCallbacks }

  /**
    * Prepends all callbacks linked with the passed callback entry.
    */
  @inline @tailrec private def onComplete(c: CallbackEntry): Unit = {
    val s = get
    s match {
      case LinkedStateTry(x)           => executeEachCallbackWithParent(x.asInstanceOf[Try[T]], c)
      case LinkedStateCallbackEntry(x) => if (!compareAndSet(s, LinkedStateCallbackEntry(prependCallbacks(x, c)))) onComplete(c)
      case LinkedStateLink(_)          => compressRoot().onComplete(c)
    }
  }

  /**
    * Checks for the root promise in a linked chain which is not a link itself but has stored a list of callbacks or is already completed.
    * On the way through the chain it sets all links to the root promise.
    * This should reduce the number of intermediate promises in the chain which are all the same and make them available for the garbage collection
    * if they are not refered anywhere else except in the chain of links.
    * TODO #32 Split into the three methods `compressedRoot`, `root` and `link` like Scala 12.x does to allow @tailrec?
    */
  @inline private def compressRoot(): Self = {
    val s = get
    s match {
      case LinkedStateTry(_)           => this
      case LinkedStateCallbackEntry(_) => this
      case LinkedStateLink(l) => {
        val root = l.asInstanceOf[Self].compressRoot()
        if (!compareAndSet(s, LinkedStateLink(root))) compressRoot() else root
      }
    }
  }

  private[pepm19] def isReady(): Boolean =
    get match {
      case LinkedStateTry(_)  => true
      case LinkedStateLink(l) => l.isReady()
      case _                  => false
    }

  /**
    * The following methods exist for tests only.
    * @param primCASPromiseLinking The target promise which this should be a direct link to.
    * @return True if this is a direct link to the target promise. Otherwise, false.
    */
  private[pepm19] def isLinkTo(primCASPromiseLinking: Self): Boolean = get match {
    case LinkedStateLink(x) => x == primCASPromiseLinking
    case _                  => false
  }

  private[pepm19] def isLink(): Boolean = get match {
    case LinkedStateLink(_) => true
    case _                  => false
  }

  private[pepm19] def getLinkTo(): Self = get match {
    case LinkedStateLink(x) => x.asInstanceOf[Self]
    case _                  => throw new RuntimeException("Invalid usage.")
  }

  private[pepm19] def isListOfCallbacks(): Boolean = get match {
    case LinkedStateCallbackEntry(_) => true
    case _                           => false
  }

  private[pepm19] def getNumberOfCallbacks(): Int = get match {
    case LinkedStateCallbackEntry(x) => getNumberOfCallbacks(x)
    case _                           => throw new RuntimeException("Is not a list of callbacks.")
  }

  private def getNumberOfCallbacks(c: CallbackEntry): Int = c match {
    case SingleCallbackEntry(_)           => 1
    case ParentCallbackEntry(left, right) => getNumberOfCallbacks(left) + getNumberOfCallbacks(right)
    case Noop                             => 0
    case LinkedCallbackEntry(_, prev)     => 1 + getNumberOfCallbacks(prev)
  }
}
