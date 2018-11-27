package tdauth.pepm19

import java.util.concurrent.Executors

import scala.concurrent.SyncVar

class JavaExecutorTest extends AbstractUnitSpec {
  "JavaExecutor" should "call a function asynchronously" in {
    val executor = new JavaExecutor(Executors.newSingleThreadExecutor())
    val v = new SyncVar[Int]

    executor.submit(() => {
      v.put(1)
    })

    val r = v.take
    r should be(1)
    executor.shutdown()
  }
}
