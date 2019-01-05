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

  override def tryCompleteC(v: Try[T]): Boolean =
    atomic { implicit txn =>
      state() match {
        case Left(_) => None
        case Right(x) =>
          state() = Left(v)
          Some(x)
      }
    }
    /*
     * It is important to execute the callbacks outside of the transaction to prevent multiple calls of the callbacks when the transaction fails.
     */ match {
      case None => false
      case Some(x) =>
        executeEachCallback(v, x)
        true
    }

  override def onCompleteC(c: Callback): Unit =
    atomic { implicit txn =>
      state() match {
        case Left(x) => Some(x)
        case Right(x) =>
          state() = Right(prependCallback(x, c))
          None
      }
    }
    /*
     * The result will never change once it is set, so we could execute the callback inside of the transaction theoretically.
     * However, we want to keep the transaction as small as possible.
     */ match {
      case None    =>
      case Some(x) => executeCallback(x, c)
    }
}
