package tdauth.pepm19

import java.util.concurrent.Executor

import scala.annotation.tailrec
import scala.concurrent.SyncVar
import scala.util.Try

/**
  * Primitive set of (promise/future) features.
  */
trait Core[T] {

  type Callback = Try[T] => Unit

  type Value = Either[Try[T], CallbackEntry]

  def newC[S](ex: Executor): Core[S]

  /**
    * The executor is passed on the combined futures.
    * We could also use Scala's `ExecutionContext` here but it is not necessary.
    */
  def getExecutorC: Executor

  /**
    * Blocks until the future has been completed and returns the successful result value or throws the failing exception.
    */
  def getC(): Try[T]
  def tryCompleteC(v: Try[T]): Boolean
  def onCompleteC(c: Callback): Unit

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
    if (callbacks ne Noop) { LinkedCallbackEntry(c, callbacks) } else { SingleCallbackEntry(c) }

  protected def dispatchCallback(v: Try[T], c: Callback): Unit =
    getExecutorC.execute(() => c.apply(v))

  /**
    * Dispatches each callback separately to the executor.
    * This behaviour is intended since in Haskell we use one `forkIO` for each callback call.
    * In Scala we have underlying executor threads instead and do not use one thread per callback call but at least
    * we can prevent that all callbacks are always called together in the same executor thread.
    */
  @inline @tailrec protected final def dispatchCallbacksOneAtATime(v: Try[T], callbacks: CallbackEntry): Unit = if (callbacks ne Noop) {
    callbacks match {
      case LinkedCallbackEntry(_, prev) => {
        getExecutorC.execute(() => callbacks.asInstanceOf[LinkedCallbackEntry[T]].c.apply(v))
        dispatchCallbacksOneAtATime(v, prev)
      }
      case SingleCallbackEntry(_) =>
        getExecutorC.execute(() => callbacks.asInstanceOf[SingleCallbackEntry[T]].c.apply(v))
      case Noop =>
    }
  }

  /**
    * This version is much simpler than the CompletionLatch from Scala FP's implementation.
    */
  private final class CompletionSyncVar[T] extends SyncVar[Try[T]] with ((Try[T]) => Unit) {
    override def apply(value: Try[T]): Unit = put(value)
  }
}
