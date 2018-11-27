package tdauth.futuresandpromises.core

import tdauth.futuresandpromises._

import scala.concurrent.SyncVar
import scala.util.Try

/**
  * Primitive set of (promise/future) features.
  */
trait Core[T] {

  type Callback = (Try[T]) => Unit

  type Value = Either[Try[T], CallbackEntry]

  def newC[S](ex: Executor): Core[S]

  /**
    * The executor is passed on the combined futures.
    */
  def getExecutorC: Executor

  /**
    * Blocks until the future has been completed and returns the successful result value or throws the failing exception.
    */
  def getC(): Try[T]
  def tryCompleteC(v: Try[T]): Boolean
  def onCompleteC(c: Callback): Unit
  def isReadyC(): Boolean

  /**
    * Helper method which uses an MVar to block until the future has been completed and
    * returns its result. Throws an exception if it has failed.
    */
  protected def getResultWithMVar(): Try[T] = {
    val s = new CompletionSyncVar[T]
    this.onCompleteC(s)
    s.take()
  }

  protected def appendCallback(callbacks: CallbackEntry, c: Callback): CallbackEntry =
    if (callbacks ne Noop) LinkedCallbackEntry(c, callbacks)
    else SingleCallbackEntry(c)

  protected def appendCallbacks(callbacks: CallbackEntry, appendedCallbacks: CallbackEntry): CallbackEntry =
    if (callbacks ne Noop) { ParentCallbackEntry(appendedCallbacks, callbacks) } else {
      appendedCallbacks
    }

  protected def dispatchCallback(v: Try[T], c: Callback): Unit =
    getExecutorC.submit(() => c.apply(v))

  /**
    * Dispatches all callbacks together at once to the executor.
    */
  protected def dispatchCallbacks(v: Try[T], callbacks: CallbackEntry): Unit =
    if (callbacks ne Noop)
      getExecutorC.submit(() => applyCallbacks(v, callbacks))

  protected final def applyCallbacks(v: Try[T], callbackEntry: CallbackEntry) {
    callbackEntry match {
      case LinkedCallbackEntry(_, prev) => {
        callbackEntry.asInstanceOf[LinkedCallbackEntry[T]].c.apply(v)
        applyCallbacks(v, prev)
      }
      case SingleCallbackEntry(_) =>
        callbackEntry.asInstanceOf[SingleCallbackEntry[T]].c.apply(v)
      case ParentCallbackEntry(left, right) => {
        applyCallbacks(v, left)
        applyCallbacks(v, right)
      }
      case Noop =>
    }
  }

  /**
    * Dispatches each callback separately to the executor.
    */
  protected final def dispatchCallbacksOneAtATime(v: Try[T], callbacks: CallbackEntry): Unit = if (callbacks ne Noop) {
    callbacks match {
      case LinkedCallbackEntry(_, prev) => {
        getExecutorC.submit(() => callbacks.asInstanceOf[LinkedCallbackEntry[T]].c.apply(v))
        dispatchCallbacksOneAtATime(v, prev)
      }
      case SingleCallbackEntry(_) =>
        getExecutorC.submit(() => callbacks.asInstanceOf[SingleCallbackEntry[T]].c.apply(v))
      case ParentCallbackEntry(left, right) => {
        dispatchCallbacksOneAtATime(v, left)
        dispatchCallbacksOneAtATime(v, right)
      }
      case Noop =>
    }
  }

  /**
    * This version is much simpler than the CompletionLatch from Scala FP's implementation.
    */
  private final class CompletionSyncVar[T] extends SyncVar[Try[T]] with (Try[T] => Unit) {
    override def apply(value: Try[T]): Unit = put(value)
  }
}
