package tdauth.pepm19

import java.util.concurrent.Executor

import scala.concurrent.stm._
import scala.util.Try

class CSTM[T](ex: Executor) extends FP[T] {

  private val state = Ref[State](Right(Noop))

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] with FP[S] = new CSTM[S](ex)

  override def getC(): Try[T] = atomic { implicit txn =>
    state() match {
      case Left(x)  => x
      case Right(_) => retry
    }
  }

  override def tryCompleteC(v: Try[T]): Boolean = atomic { implicit txn =>
    state() match {
      case Left(_) => false
      case Right(x) =>
        state() = Left(v)
        executeEachCallback(v, x)
        true
    }
  }

  override def onCompleteC(c: Callback): Unit = atomic { implicit txn =>
    state() match {
      case Left(x)  => executeCallback(x, c)
      case Right(x) => state() = Right(prependCallback(x, c))
    }
  }
}
