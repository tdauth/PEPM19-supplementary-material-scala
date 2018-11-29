package tdauth.pepm19

import java.util.concurrent.{Executor, Executors}

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.List
import scala.concurrent.SyncVar
import scala.util.Success

abstract class AbstractFPTest extends FlatSpec with Matchers {
  private val executor = Executors.newSingleThreadExecutor()

  // Basic future methods:
  "get " should "return a successful value" in {
    val p = getFP
    p.trySuccess(10)
    p.getP() should be(Success(10))
  }

  // Basic promise methods:
  "tryComplete" should "not complete a future successfully" in {
    val p = getFP
    p.tryComplete(Success(10)) should be(true)
    p.getP() should be(Success(10))
  }

  it should "not complete a future which is already completed" in {
    val p = getFP
    p.trySuccess(10) should be(true)
    p.tryComplete(Success(11)) should be(false)
    p.getP() should be(Success(10))
  }

  // Derived promise methods:
  "trySuccess" should "should complete a future successfully" in {
    val p = getFP
    p.trySuccess(10) should be(true)
    p.getP() should be(Success(10))
  }

  it should "not complete a future which is already completed" in {
    val p = getFP
    p.trySuccess(10) should be(true)
    p.trySuccess(11) should be(false)
    p.getP() should be(Success(10))
  }

  "tryFail" should "complete a future with an exception" in {
    val p = getFP
    p.tryFail(new RuntimeException("test")) should be(true)
    the[RuntimeException] thrownBy p.getP().get should have message "test"
  }

  it should "not complete a future which is already completed" in {
    val p = getFP
    p.tryFail(new RuntimeException("test")) should be(true)
    p.tryFail(new RuntimeException("test 2")) should be(false)
    the[RuntimeException] thrownBy p.getP().get should have message "test"
  }

  "tryCompleteWith" should "complete a future with the help of another future" in {
    val p0 = getFP
    p0.trySuccess(10)

    val p = getFP
    p.tryCompleteWith(p0)
    p.getP() should be(Success(10))
  }

  "trySuccessWith" should "complete a future successfully with the help of another future" in {
    val p0 = getFP
    p0.trySuccess(10)

    val p = getFP
    p.trySuccessWith(p0)
    p.getP() should be(Success(10))
  }

  it should "not complete a future with the help of a failing future" in {
    val p0 = getFP
    p0.tryFail(new RuntimeException("test"))

    val p = getFP
    p.trySuccessWith(p0)
    p.trySuccess(11) should be(true)
    p.getP() should be(Success(11))
  }

  "tryFailWith" should "complete a future with an exception with the help of another future" in {
    val p0 = getFP
    p0.tryFail(new RuntimeException("test"))

    val p = getFP
    p.tryFailWith(p0)
    the[RuntimeException] thrownBy p.getP().get should have message "test"
  }

  it should "not complete a future with the help of a successful future" in {
    val p0 = getFP
    p0.trySuccess(10)

    val p = getFP
    p.tryFailWith(p0)
    p.tryFail(new RuntimeException("test")) should be(true)
    the[RuntimeException] thrownBy p.getP().get should have message "test"
  }

  // Derived future methods:
  /**
    * Scala FP makes no guarantees about the execution order of callbacks.
    * We can guarantee that they are executed in reverse order.
    */
  "onComplete" should "register multiple callbacks which are all called in the correct order" in {
    val p = getFP
    val s = new SyncVar[List[Int]]
    val l = new SyncVar[Unit]
    s.put(List())
    1 to 10 foreach (i =>
      p.onComplete(_ => {
        val v = s.take()
        s.put(v :+ i)
        if (i == 1) l.put(())
      }))
    p.trySuccess(10) should be(true)
    l.get
    val finalResult = s.get
    finalResult should be(List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
  }

  /**
    * When the promise is already completed, the callbacks will be submitted immediately and therefore executed in the
    * correct order.
    */
  it should "register multiple callbacks on a completed promise which are all called in the correct order" in {
    val p = getFP
    p.trySuccess(10) should be(true)
    val s = new SyncVar[List[Int]]
    val l = new SyncVar[Unit]
    s.put(List())
    1 to 10 foreach (i =>
      p.onComplete(_ => {
        val v = s.take()
        s.put(v :+ i)
        if (i == 10) l.put(())
      }))
    l.get
    val finalResult = s.get
    finalResult should be(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  }

  "future_" should "create a successfully completed future" in {
    val p = getFP.future_(() => Success(10))
    p.getP() should be(Success(10))
  }

  "future" should "create a successfully completed future" in {
    val p = getFP.future(Success(10))
    p.getP() should be(Success(10))
  }

  "onSuccess" should "register a callback which is called" in {
    val p = getFP
    val s = new SyncVar[Int]
    p.onSuccess(v => s.put(v))
    p.trySuccess(10) should be(true)
    s.get should be(10)
  }

  "onFail" should "register a callback which is called" in {
    val p = getFP
    val s = new SyncVar[Throwable]
    p.onFail(v => s.put(v))
    p.tryFail(new RuntimeException("test")) should be(true)
    s.get.getMessage should be("test")
  }

  "transform" should "create a new successful future" in {
    val p = getFP
    val s = p.transform(v => v.get * 10)
    p.trySuccess(10)
    s.getP() should be(Success(100))
  }

  it should "create a failed future" in {
    val p = getFP
    val s = p.transform(v => throw new RuntimeException("test"))
    p.trySuccess(10)
    the[RuntimeException] thrownBy s.getP().get should have message "test"
  }

  "transformWith" should "create a new successful future" in {
    val p = getFP
    val p0 = getFP
    val s = p.transformWith(v => p0)
    p.trySuccess(10)
    p0.trySuccess(11)
    s.getP() should be(Success(11))
  }

  it should "create a failed future" in {
    val p = getFP
    val p0 = getFP
    val s = p.transformWith(v => p0)
    p.trySuccess(10)
    p0.tryFail(new RuntimeException("test"))
    the[RuntimeException] thrownBy s.getP().get should have message "test"
  }

  "followedBy" should "create a new successful future" in {
    val p = getFP
    val s = p.followedBy(_ * 10)
    p.trySuccess(10)
    s.getP() should be(Success(100))
  }

  it should "create a failed future" in {
    val p = getFP
    val s = p.followedBy(v => throw new RuntimeException("test"))
    p.trySuccess(10)
    the[RuntimeException] thrownBy s.getP().get should have message "test"
  }

  "followedByWith" should "create a new successful future" in {
    val p = getFP
    val p0 = getFP
    val s = p.followedByWith(v => p0)
    p.trySuccess(10)
    p0.trySuccess(11)
    s.getP should be(Success(11))
  }

  it should "create a failed future" in {
    val p = getFP
    val p0 = getFP
    val s = p.followedByWith(v => p0)
    p.trySuccess(10)
    p0.tryFail(new RuntimeException("test"))
    the[RuntimeException] thrownBy s.getP().get should have message "test"
  }

  it should "create a failed future by the first one" in {
    val p = getFP
    val p0 = getFP
    val s = p.followedByWith(v => p0)
    p.tryFail(new RuntimeException("test 0"))
    p0.tryFail(new RuntimeException("test 1"))
    the[RuntimeException] thrownBy s.getP().get should have message "test 0"
  }

  "guard" should "throw the exception PredicateNotFulfilled" in {
    val p = getFP
    val future = p.guard(_ != 10)
    p.trySuccess(10)
    the[PredicateNotFulfilled] thrownBy future
      .getP()
      .get should have message null
  }

  it should "not throw any exception" in {
    val p = getFP
    val future = p.guard(_ == 10)
    p.trySuccess(10)
    future.getP() should be(Success(10))
  }

  it should "throw the initial exception" in {
    val p = getFP
    val future = p.guard(_ == 10)
    p.tryFail(new RuntimeException("test"))
    the[RuntimeException] thrownBy future.getP().get should have message "test"
  }

  "orAlt" should "complete the final future with first one over the second one" in {
    val p0 = getFP
    val p1 = getFP
    val f = p0.orAlt(p1)
    p0.trySuccess(10)
    p1.trySuccess(11)
    f.getP() should be(Success(10))

  }

  it should "complete the final future with the second one over the first one" in {
    val p0 = getFP
    p0.tryFail(new RuntimeException("test"))
    val p1 = getFP
    p1.trySuccess(11)
    val f = p0.orAlt(p1)
    f.getP() should be(Success(11))
  }

  it should "complete the final future with the first one over the second one when both are failing" in {
    val p0 = getFP
    p0.tryFail(new RuntimeException("test 0"))
    val p1 = getFP
    p1.tryFail(new RuntimeException("test 1"))
    val f = p0.orAlt(p1)
    the[RuntimeException] thrownBy f.getP().get should have message "test 0"
  }

  "first" should "complete the final future with the first one" in {
    val p0 = getFP
    p0.trySuccess(10)
    val p1 = getFP
    val f = p0.first(p1)
    f.getP() should be(Success(10))
  }

  it should "complete the final future with the second one" in {
    val p1 = getFP
    p1.trySuccess(11)
    val p0 = getFP
    val f = p1.first(p0)
    f.getP() should be(Success(11))
  }

  it should "complete the final future with the second one although it fails" in {
    val p1 = getFP
    p1.tryFail(new RuntimeException("test 1"))
    val p0 = getFP
    val f = p0.first(p1)
    the[RuntimeException] thrownBy f.getP().get should have message "test 1"
  }

  "firstSucc" should "complete the final future with the first one" in {
    val p0 = getFP
    p0.trySuccess(10)
    val p1 = getFP
    val f = p0.firstSucc(p1)
    f.getP() should be(Success(10))
  }

  it should "complete the final future with the second one" in {
    val p0 = getFP
    p0.tryFail(new RuntimeException("test"))
    the[RuntimeException] thrownBy p0.getP().get should have message "test"
    val p1 = getFP
    p1.trySuccess(11)
    val f = p0.firstSucc(p1)
    f.getP() should be(Success(11))
  }

  it should "complete with the exception of the second future" in {
    val p0 = getFP
    p0.tryFail(new RuntimeException("test 0"))
    the[RuntimeException] thrownBy p0.getP().get should have message "test 0"
    val p1 = getFP
    val f = p0.firstSucc(p1)
    p1.tryFail(new RuntimeException("test 1"))
    the[RuntimeException] thrownBy f.getP().get should have message "test 1"
  }

  it should "complete with the exception of the first future" in {
    val p1 = getFP
    p1.tryFail(new RuntimeException("test 1"))
    the[RuntimeException] thrownBy p1.getP().get should have message "test 1"
    val p0 = getFP
    val f = p0.firstSucc(p1)
    p0.tryFail(new RuntimeException("test 0"))
    the[RuntimeException] thrownBy f.getP().get should have message "test 0"
  }

  def getFP: FP[Int]
  def getExecutor: Executor = executor
}
