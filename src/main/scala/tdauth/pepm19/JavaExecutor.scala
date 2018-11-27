package tdauth.pepm19

/**
  * Executor based on a Java executor.
  */
class JavaExecutor(val ex: java.util.concurrent.ExecutorService) extends Executor {

  override def submit(f: () => Unit): Unit = {
    ex.execute(() => f.apply())
  }

  override def shutdown(): Unit = ex.shutdownNow()
}
