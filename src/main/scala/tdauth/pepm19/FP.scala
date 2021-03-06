package tdauth.pepm19

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Combines the functionality of futures and promises and relies on some abstract core methods.
  * All concrete methods are derived from these methods of [[Core]].
  */
trait FP[T] extends Core[T] {

  // Basic methods:
  /**
    * `newC` has to be implemented by the concrete types.
    * We have to use this name since the name `new` which is used in Haskell is a keyword in Scala.
    */
  def newFP[S](executor: Executor): FP[S] =
    newC[S](executor)
  def getExecutor: Executor = getExecutorC

  // Basic promise methods:
  def tryComplete(v: Try[T]): Boolean = tryCompleteC(v)

  // Basic future methods:
  def onComplete(c: Callback): Unit = onCompleteC(c)

  /**
    * We have to use the name `getP` since the name `get` which is used in Haskell is already used by `AtomicReference`.
    */
  def getP(): Try[T] = getC()

  // Derived promise methods:
  def trySuccess(v: T): Boolean = tryComplete(Success(v))
  def tryFail(e: Throwable): Boolean = tryComplete(Failure(e))
  def trySuccessWith(other: FP[T]): Unit = other.onSuccess(trySuccess)
  def tryFailWith(other: FP[T]): Unit = other.onFail(tryFail)
  def tryCompleteWith(other: FP[T]): Unit =
    other.onComplete(tryComplete)

  // Derived future methods:
  def future_[S](h: () => Try[S]): FP[S] = {
    val p = newFP[S](getExecutor)
    getExecutor.execute(() => p.tryComplete(h()))
    p
  }
  def future[S](v: Try[S]): FP[S] = future_(() => v)
  def onSuccess(f: T => Unit): Unit =
    onComplete(t => if (t.isSuccess) f(t.get))
  def onFail(f: Throwable => Unit): Unit =
    onComplete(t => if (t.isFailure) f(t.failed.get))
  def transformWith[S](f: Try[T] => FP[S]): FP[S] = {
    val p = newFP[S](getExecutor)
    onComplete(t => p.tryCompleteWith(f(t)))
    p
  }
  def transform[S](f: Try[T] => S): FP[S] = {
    val p = newFP[S](getExecutor)
    onComplete(t => {
      try {
        p.trySuccess(f(t))
      } catch {
        case NonFatal(e) => p.tryFail(e)
      }
    })
    p
  }
  def followedBy[S](f: T => S): FP[S] = transform(t => f(t.get))
  def followedByWith[S](f: T => FP[S]): FP[S] =
    transformWith(t =>
      try {
        f(t.get)
      } catch {
        case NonFatal(x) =>
          val p = newFP[S](getExecutor)
          p.tryFail(x)
          p
    })

  def guard(f: T => Boolean): FP[T] =
    followedBy(v => if (!f(v)) throw new PredicateNotFulfilled else v)

  /**
    * Actually, orElse or fallbackTo in Scala FP.
    * It is called orAlt since we cannot use orElse in Haskell.
    *
    * We have to use transformWith here to prevent deadlocks.
    * If we would use get inside of the callback, it would register another callback for synchronization with MVars.
    * This callback would not be executed if the executor has only one thread, for example.
    * It would have to be marked as "blocking".
    * With transformWith, the future is directly returned and the callback is not blocked.
    * The same is done in Scala FP.
    */
  def orAlt(other: FP[T]): FP[T] =
    transformWith(t =>
      if (t.isSuccess) { this } else {
        other.transform(tt =>
          if (tt.isSuccess) {
            tt.get
          } else {
            t.get
        })
    })

  def first(other: FP[T]): FP[T] = {
    val p = newFP[T](getExecutor)
    p.tryCompleteWith(this)
    p.tryCompleteWith(other)

    p
  }
  def firstSucc(other: FP[T]): FP[T] = {
    val p = newFP[T](getExecutor)
    /*
     * This context is required to store if both futures have failed to prevent starvation.
     */
    val ctx = new AtomicInteger(0)
    val callback = (t: Try[T]) => {
      if (t.isFailure) {
        val c = ctx.incrementAndGet()

        if (c == 2) {
          p.tryComplete(t)
        }
      } else {
        p.tryComplete(t)
      }
    }: Unit

    this.onComplete(callback)
    other.onComplete(callback)

    p
  }
}
