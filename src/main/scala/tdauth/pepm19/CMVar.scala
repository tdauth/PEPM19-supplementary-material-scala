package tdauth.pepm19

import java.util.concurrent.Executor

import scala.concurrent.SyncVar
import scala.util.{Left, Try}

class CMVar[T](ex: Executor) extends SyncVar[FP[T]#Value] with FP[T] {
  put(Right(Noop))

  /*
   * We need a second MVar to signal that the future has a result.
   */
  val sig = new SyncVar[Unit]

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] = new CMVar[S](ex)

  override def getC(): Try[T] = {
    sig.get
    get.left.get
  }

  override def tryCompleteC(v: Try[T]): Boolean = {
    val s = take()
    s match {
      case Left(_) =>
        // Put the value back.
        put(s)
        false
      case Right(x) =>
        put(Left(v))
        sig.put(())
        dispatchCallbacksOneAtATime(v, x)
        true
    }
  }

  override def onCompleteC(c: Callback): Unit = {
    val s = take()
    s match {
      case Left(x) =>
        put(s)
        dispatchCallback(x, c)
      case Right(x) => put(Right(appendCallback(x, c)))
    }
  }
}
