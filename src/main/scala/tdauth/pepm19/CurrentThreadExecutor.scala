package tdauth.pepm19
import java.util.concurrent.Executor

/**
  * Detect blocking combinators by simply using the current thread.
  */
class CurrentThreadExecutor extends Executor {
  var counter = 0

  def getCounter = counter

  override def execute(r: Runnable): Unit = {
    r.run()
    counter += 1
  }
}
