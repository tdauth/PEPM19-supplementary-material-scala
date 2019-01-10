package tdauth.pepm19

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable.ListBuffer
import scala.util.Success

class CCASFixedPromiseLinkingTest extends AbstractFPTest(false) {
  type FPLinkingType = CCASFixedPromiseLinking[Int]

  def getFPPromiseLinking: FPLinkingType = new FPLinkingType(getExecutor, true, 1)
  override def getFP: FP[Int] = new FPLinkingType(getExecutor, true, 1)

  "transformWith with a link" should "create a new successful future" in {
    val p = getFP
    val p0 = getFP
    val s = p.transformWith(_ => p0)
    p.trySuccess(FirstNumber) shouldEqual true
    p0.asInstanceOf[FPLinkingType].isLink() shouldEqual true
    p0.asInstanceOf[FPLinkingType].isLinkTo(s.asInstanceOf[FPLinkingType]) shouldEqual true
    p0.trySuccess(SecondNumber) shouldEqual true
    s.getP() should be(Success(SecondNumber))
  }

  "Link" should "link to another promise and be completed by it" in {
    val v = new AtomicBoolean(false)
    val p0 = getFPPromiseLinking

    p0.isLink() shouldEqual true
    p0.isLinkTo(p0) shouldEqual false
    p0.getNumberOfCallbacks() shouldEqual 0

    val p1 = getFPPromiseLinking
    p1.onComplete(_ => v.set(true))

    p1.isLink() shouldEqual true
    p1.isLinkTo(p0) shouldEqual false
    p1.getNumberOfCallbacks() shouldEqual 1

    p0.tryCompleteWith(p1)

    p0.isLink() shouldEqual true
    // In this solution the callbacks are not moved to p0.
    p0.getNumberOfCallbacks() shouldEqual 0
    p1.getNumberOfCallbacks() shouldEqual 1
    p1.isLinkTo(p0) shouldEqual true

    p1.trySuccess(10)
    p0.getP shouldEqual Success(10)
    p1.getP shouldEqual Success(10)
    v.get() shouldEqual true
  }

  it should "link to another promise which links to another promise" in {
    val v = new AtomicInteger(0)
    val p0 = getFPPromiseLinking
    val p1 = getFPPromiseLinking
    p1.onComplete(_ => v.incrementAndGet())
    val p2 = getFPPromiseLinking
    p2.onComplete(_ => v.incrementAndGet())
    p0.tryCompleteWith(p1)
    p1.tryCompleteWith(p2)

    p0.isLink() shouldEqual true
    // the callbacks are not moved to p0
    p0.getNumberOfCallbacks() shouldEqual 0
    p1.getNumberOfCallbacks() shouldEqual 1
    p2.getNumberOfCallbacks() shouldEqual 1
    p1.isLinkTo(p0) shouldEqual true
    // There is no compression in this solution. p2 is still a link to p1 and p1 links to p0.
    p2.isLinkTo(p1) shouldEqual true
    p1.isLinkTo(p0) shouldEqual true

    p2.trySuccess(10)
    p0.getP shouldEqual Success(10)
    p1.getP shouldEqual Success(10)
    p2.getP shouldEqual Success(10)
    v.get() shouldEqual 2
  }

  /**
    * Creates n + 1 promises.
    * n of them are links to p.
    * The final linked promise will be completed with 10 which completes p.
    * p should collect all the callbacks of all the n promises.
    */
  it should "create a chain of links" in {
    val n = 100
    val counter = new AtomicInteger(0)
    val links = ListBuffer[FPLinkingType]()

    def createChainElement(i: Int, current: FPLinkingType): FPLinkingType = {
      assert(i > 0)
      val p = getFPPromiseLinking
      p.onComplete(_ => counter.incrementAndGet())
      links += p
      current.tryCompleteWith(p)
      if (i == 1) p else createChainElement(i - 1, p)
    }

    // p should not be a link and collect all the callbacks.
    val p = getFPPromiseLinking
    val finalLink = createChainElement(n, p)

    links.size shouldEqual n
    p.isLink() shouldEqual true
    // In this solution the callbacks are not moved to p.
    p.getNumberOfCallbacks() shouldEqual 0
    finalLink.isLink() shouldEqual true
    // In this solution there is no compression. The final link still links to its next element.
    finalLink.isLinkTo(p) shouldEqual false

    def assertUncompletedChain(links: ListBuffer[FPLinkingType], c: Int): Unit = {
      if (links.nonEmpty) {
        val l = links.head
        l.isLink shouldEqual true
        // In this solution there is no compression. All links still link to the next element. Only the first link links to p.
        val expectLinkToP = if (c < n) { false } else { true }
        l.isLinkTo(p) shouldEqual expectLinkToP
        if (links.size > 1) assertUncompletedChain(links.tail, c - 1)
      }
    }

    assertUncompletedChain(links, n)

    finalLink.trySuccess(10)

    p.isLink() shouldEqual false
    p.isReady shouldEqual true
    p.getP shouldEqual Success(10)
    counter.get() shouldEqual n

    def assertCompletedChain(links: ListBuffer[FPLinkingType]): Unit = {
      if (links.nonEmpty) {
        val l = links.head
        // In this solution all links will get the result value.
        l.isReady shouldEqual true
        l.isLink() shouldEqual false
        l.getP shouldEqual Success(10)
        if (links.size > 1) assertCompletedChain(links.tail)
      }
    }

    assertCompletedChain(links)
  }

  it should "produce correct behaviour by calling only the callbacks of the one linked promise which is completed" in {
    val counter = new AtomicInteger(0)
    val p = getFPPromiseLinking
    p.onComplete(_ => counter.incrementAndGet())
    val f = getFPPromiseLinking
    f.onComplete(_ => counter.incrementAndGet())
    val g = getFPPromiseLinking
    g.onComplete(_ => counter.incrementAndGet())
    p.tryCompleteWith(f)
    p.tryCompleteWith(g)

    p.isLink() shouldEqual true
    // In this solution the callbacks are not moved to p.
    p.getNumberOfCallbacks() shouldEqual 1
    f.isLinkTo(p) shouldEqual true
    g.isLinkTo(p) shouldEqual true

    g.trySuccess(10)

    g.getP shouldEqual Success(10)
    p.getP shouldEqual Success(10)

    // only the callbacks of p and f are called
    counter.get() shouldEqual 2
  }

  it should "create 1 task for 200 callbacks" in {
    val ex = new CurrentThreadExecutor
    val p = new CCASFixedPromiseLinking[Int](ex, true, 1)
    val f = new CCASFixedPromiseLinking[Int](ex, true, 200)
    p tryCompleteWith f

    f.isLinkTo(p) shouldEqual true
    val counter = new AtomicInteger(0)

    1 to 100 foreach { _ =>
      f.onCompleteC(_ => counter.incrementAndGet())
    }
    1 to 100 foreach { _ =>
      p.onCompleteC(_ => counter.incrementAndGet())
    }
    f.trySuccess(1)

    f.getC() shouldEqual Success(1)
    p.getC() shouldEqual Success(1)
    counter.get() shouldEqual 200
    ex.getCounter shouldEqual 3 // 1 task for all the callbacks + 2 tasks for 2 getC calls.
  }

  it should "create 200 tasks for 200 callbacks" in {
    val ex = new CurrentThreadExecutor
    val p = new CCASFixedPromiseLinking[Int](ex, true, 1)
    val f = new CCASFixedPromiseLinking[Int](ex, true, 1)
    p tryCompleteWith f

    f.isLinkTo(p) shouldEqual true
    val counter = new AtomicInteger(0)

    1 to 100 foreach { _ =>
      f.onCompleteC(_ => counter.incrementAndGet())
    }
    1 to 100 foreach { _ =>
      p.onCompleteC(_ => counter.incrementAndGet())
    }
    f.trySuccess(1)

    f.getC() shouldEqual Success(1)
    p.getC() shouldEqual Success(1)
    counter.get() shouldEqual 200
    ex.getCounter shouldEqual 202 // 200 tasks for all the callbacks + 2 tasks for 2 getC calls.
  }
}
