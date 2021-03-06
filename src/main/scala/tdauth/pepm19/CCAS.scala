package tdauth.pepm19

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.{Left, Try}

class CCAS[T](ex: Executor) extends AtomicReference[Core[T]#State](Right(Noop)) with FP[T] {

  override def getExecutorC: Executor = ex

  override def newC[S](ex: Executor): Core[S] with FP[S] = new CCAS[S](ex)

  override def getC(): Try[T] = getResultWithMVar()

  override def tryCompleteC(v: Try[T]): Boolean = tryCompleteInternal(v)

  override def onCompleteC(c: Callback): Unit = onCompleteInternal(c)

  @tailrec private def tryCompleteInternal(v: Try[T]): Boolean = {
    val s = get
    s match {
      case Left(_) => false
      case Right(x) =>
        if (compareAndSet(s, Left(v))) {
          executeEachCallback(v, x)
          true
        } else {
          tryCompleteInternal(v)
        }
    }
  }

  @tailrec private def onCompleteInternal(c: Callback): Unit = {
    val s = get
    s match {
      case Left(x) => executeCallback(x, c)
      case Right(x) =>
        if (!compareAndSet(s, Right(prependCallback(x, c)))) {
          onCompleteInternal(c)
        }
    }
  }
}
