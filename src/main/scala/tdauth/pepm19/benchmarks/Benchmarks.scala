package tdauth.pepm19.benchmarks

import java.io.{File, FileWriter}
import java.util.Locale
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executor, ExecutorService, Executors, ThreadFactory}

import tdauth.pepm19.{CSTM, _}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.Success
import scala.util.control.NonFatal

/**
  * Compares the performance of our FP implementation to the performance of Scala FP and Twitter Util.
  * We have five different performance tests but test 1 and test 2 use the same method `Benchmarks#perf1Prim`.
  * Therefore, we only have four different `perf<n>` methods.
  */
object Benchmarks extends App {
  val Tests = 1 to 5
  val TwitterImplementationName = "Twitter Util"
  val TwitterPlotFileSuffix = "twitterutil"
  val ScalaImplementationName = "Scala FP"
  val ScalaPlotFileSuffix = "scalafp"
  val PrimCASImplementationName = "Prim CAS"
  val PrimCASPlotFileSuffix = "cas"
  val PrimMVarImplementationName = "Prim MVar"
  val PrimMVarPlotFileSuffix = "mvar"
  val PrimSTMImplementationName = "Prim STM"
  val PrimSTMPlotFileSuffix = "stm"
  val PrimCASPromiseLinkingImplementationName = "Prim CAS Promise Linking"
  val PrimCASPromiseLinkingPlotFileSuffix = "caspromiselinking"
  val PrimCASFixedPromiseLinkingImplementationName = "Prim CAS Fixed Promise Linking"
  val PrimCASFixedPromiseLinkingPlotFileSuffix = "casfixedpromiselinking"
  val ImplementationNames =
    Vector(
      TwitterImplementationName,
      ScalaImplementationName,
      PrimCASImplementationName,
      PrimMVarImplementationName,
      PrimSTMImplementationName,
      PrimCASPromiseLinkingImplementationName,
      PrimCASFixedPromiseLinkingImplementationName
    )
  val PlotFileSuffixes = Vector(
    TwitterPlotFileSuffix,
    ScalaPlotFileSuffix,
    PrimCASPlotFileSuffix,
    PrimMVarPlotFileSuffix,
    PrimSTMPlotFileSuffix,
    PrimCASPromiseLinkingPlotFileSuffix,
    PrimCASFixedPromiseLinkingPlotFileSuffix
  )
  val Iterations = 10

  /**
    * Bear in mind that there is always the main thread.
    */
  val ExecutorThreads = Vector(1, 2, 4, 8)

  // test 1
  val Test1N = 10000
  val Test1M = 100
  val Test1K = 200
  // test 2
  val Test2N = 100000
  val Test2M = 20
  val Test2K = 2
  // test 3
  val Test3N = 2000000
  // test 4
  val Test4N = 2000000
  // test 5
  val Test5N = 100
  val Test5M = 100000

  deletePlotAllFiles()

  printf("We have %d available processors.\n", Runtime.getRuntime().availableProcessors())
  runAllTests

  private implicit def intWithTimes[T](n: Int) = new {
    def times(f: => T) = 1 to n map { _ =>
      f
    }
  }

  private def getPlotFileName(testNumber: Int, plotFileSuffix: String): String =
    "test" + testNumber + "_scala_" + plotFileSuffix + ".dat"

  private def deletePlotFile(testNumber: Int, plotFileSuffix: String): Unit = {
    val f = new File(getPlotFileName(testNumber, plotFileSuffix))
    if (f.exists) f.delete
  }

  private def deletePlotAllFiles() {
    for (testNumber <- Tests;
         plotFileSuffix <- PlotFileSuffixes) {
      deletePlotFile(testNumber, plotFileSuffix)
    }
  }

  private def writeEntryIntoPlotFile(plotFilePath: String, executorThreads: Int, time: Double) {
    val fileWriter = new FileWriter(plotFilePath, true)
    try {
      fileWriter.append("%d  %.2f\n".formatLocal(Locale.US, executorThreads, time))
    } catch {
      case NonFatal(t) => println(s"Exception: $t")
    } finally fileWriter.close()
  }

  private type TestFunction = () => Unit
  private class Test(val implementationName: String, val plotFileSuffix: String, val t: TestFunction)
  private class TestTwitterUtil(t: TestFunction) extends Test(TwitterImplementationName, TwitterPlotFileSuffix, t)
  private class TestScala(t: TestFunction) extends Test(ScalaImplementationName, ScalaPlotFileSuffix, t)
  private class TestPrimCAS(t: TestFunction) extends Test(PrimCASImplementationName, PrimCASPlotFileSuffix, t)
  private class TestPrimMVar(t: TestFunction) extends Test(PrimMVarImplementationName, PrimMVarPlotFileSuffix, t)
  private class TestPrimSTM(t: TestFunction) extends Test(PrimSTMImplementationName, PrimSTMPlotFileSuffix, t)
  private class TestPrimCASPromiseLinking(t: TestFunction) extends Test(PrimCASPromiseLinkingImplementationName, PrimCASPromiseLinkingPlotFileSuffix, t)
  private class TestPrimCASFixedPromiseLinking(t: TestFunction)
      extends Test(PrimCASFixedPromiseLinkingImplementationName, PrimCASFixedPromiseLinkingPlotFileSuffix, t)

  /**
    * @param t The test function which returns time which has to be substracted from the exectuion time since it should not be measured.
    */
  private def execTest(t: TestFunction): Double = {
    System.gc()
    val start = System.nanoTime()
    t()
    val fin = System.nanoTime()
    val result = fin - start
    val seconds = result.toDouble / 1000000000.0
    printf("Time: %.2fs\n", seconds)
    seconds
  }

  private def runTest(plotFileSuffix: String, testNumber: Int, executorThreads: Int, t: TestFunction) {
    val rs = (1 to Iterations).map(_ => execTest(t))
    val xs = rs.sorted
    val low = xs.head
    val high = xs.last
    val m = xs.length.toDouble
    val av = xs.sum / m
    printf("low: %.2fs high: %.2fs avrg: %.2fs\n", low, high, av)
    writeEntryIntoPlotFile(getPlotFileName(testNumber, plotFileSuffix), executorThreads, av)
  }

  private def runTest(testNumber: Int, executorThreads: Int, t: Test): Unit = {
    println(t.implementationName)
    runTest(t.plotFileSuffix, testNumber, executorThreads, t.t)
  }

  private def test1(executorThreads: Int) {
    val n = Test1N
    val m = Test1M
    val k = Test1K
    val testNumber = 1
    runTest(testNumber, executorThreads, new TestTwitterUtil(() => perf1TwitterUtil(n, m, k, executorThreads)))
    runTest(testNumber, executorThreads, new TestScala(() => perf1ScalaFP(n, m, k, executorThreads)))
    runTest(testNumber, executorThreads, new TestPrimCAS(() => perf1Prim(n, m, k, executorThreads, ex => new CCAS(ex))))
    runTest(testNumber, executorThreads, new TestPrimMVar(() => perf1Prim(n, m, k, executorThreads, ex => new CMVar(ex))))
    runTest(testNumber, executorThreads, new TestPrimSTM(() => perf1Prim(n, m, k, executorThreads, ex => new CSTM(ex))))
  }

  private def test2(executorThreads: Int) {
    val n = Test2N
    val m = Test2M
    val k = Test2K
    val testNumber = 2
    runTest(testNumber, executorThreads, new TestTwitterUtil(() => perf1TwitterUtil(n, m, k, executorThreads)))
    runTest(testNumber, executorThreads, new TestScala(() => perf1ScalaFP(n, m, k, executorThreads)))
    runTest(testNumber, executorThreads, new TestPrimCAS(() => perf1Prim(n, m, k, executorThreads, ex => new CCAS(ex))))
    runTest(testNumber, executorThreads, new TestPrimMVar(() => perf1Prim(n, m, k, executorThreads, ex => new CMVar(ex))))
    runTest(testNumber, executorThreads, new TestPrimSTM(() => perf1Prim(n, m, k, executorThreads, ex => new CSTM(ex))))
  }

  private def test3(executorThreads: Int) {
    val n = Test3N
    val testNumber = 3
    runTest(testNumber, executorThreads, new TestTwitterUtil(() => perf2TwitterUtil(n, executorThreads)))
    runTest(testNumber, executorThreads, new TestScala(() => perf2ScalaFP(n, executorThreads)))
    runTest(testNumber, executorThreads, new TestPrimCAS(() => perf2Prim(n, executorThreads, ex => new CCAS(ex))))
    runTest(testNumber, executorThreads, new TestPrimMVar(() => perf2Prim(n, executorThreads, ex => new CMVar(ex))))
    runTest(testNumber, executorThreads, new TestPrimSTM(() => perf2Prim(n, executorThreads, ex => new CSTM(ex))))
  }

  private def test4(executorThreads: Int) {
    val n = Test4N
    val testNumber = 4
    runTest(testNumber, executorThreads, new TestTwitterUtil(() => perf3TwitterUtil(n, executorThreads)))
    runTest(testNumber, executorThreads, new TestScala(() => perf3ScalaFP(n, executorThreads)))
    runTest(testNumber, executorThreads, new TestPrimCAS(() => perf3Prim(n, executorThreads, ex => new CCAS(ex))))
    runTest(testNumber, executorThreads, new TestPrimMVar(() => perf3Prim(n, executorThreads, ex => new CMVar(ex))))
    runTest(testNumber, executorThreads, new TestPrimSTM(() => perf3Prim(n, executorThreads, ex => new CSTM(ex))))
  }

  private def test5(executorThreads: Int): Unit = {
    val n = Test5N
    val m = Test5M
    val testNumber = 5
    runTest(testNumber, executorThreads, new TestTwitterUtil(() => perf4TwitterUtil(n, m, executorThreads)))
    runTest(testNumber, executorThreads, new TestPrimCAS(() => perf4Prim(n, m, executorThreads, ex => new CCAS(ex))))
    runTest(testNumber, executorThreads, new TestPrimCASFixedPromiseLinking(() => perf4Prim(n, m, executorThreads, ex => new CCASFixedPromiseLinking(ex))))
  }

  private def runTestForExecutorThreads(name: String, t: Int => Unit) {
    val nameSeparator = "=" * 40
    println(nameSeparator)
    println(name)
    println(nameSeparator)
    val executorThreadsSeparator = "-" * 40

    ExecutorThreads.foreach(c => {
      println(executorThreadsSeparator)
      println("Executor threads: " + c)
      println(executorThreadsSeparator)
      t(c)
    })
  }

  private def runTest1() = runTestForExecutorThreads("Test 1", test1)
  private def runTest2() = runTestForExecutorThreads("Test 2", test2)
  private def runTest3() = runTestForExecutorThreads("Test 3", test3)
  private def runTest4() = runTestForExecutorThreads("Test 4", test4)
  private def runTest5() = runTestForExecutorThreads("Test 5", test5)

  private def runAllTests {
    runTest1()
    runTest2()
    runTest3()
    runTest4()
    runTest5()
  }

  /**
    * Renames the threads with a prefix.
    * This helps to distinguish them when analyzing profiling data.
    */
  private class SimpleThreadFactory(prefix: String) extends ThreadFactory {
    var c = 0
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, "%s - %d".format(prefix, c))
      c += 1
      t
    }
  }

  /**
    * Waits until the counter has reached max.
    */
  private class Synchronizer(max: Int) {
    var lock = new ReentrantLock()
    var condition = lock.newCondition()
    var counter = 0

    def increment() {
      lock.lock()
      try {
        counter = counter + 1
        condition.signal()
      } finally {
        lock.unlock()
      }
    }

    def await() {
      var notFull = true
      do {
        lock.lock()
        try {
          notFull = counter < max
          if (notFull) condition.await()
        } finally {
          lock.unlock()
        }
      } while (notFull)
      assert(counter == max)
    }
  }

  private def threadFactory(prefix: String): ThreadFactory =
    new SimpleThreadFactory(prefix)

  private def fixedThreadPool(n: Int, prefix: String): ExecutorService =
    Executors.newFixedThreadPool(n, threadFactory(prefix))

  private def getTwitterUtilExecutor(n: Int, suffix: String) =
    com.twitter.util.FuturePool(fixedThreadPool(n, "twitterutil - %s".format(suffix)))

  private def getScalaFPExecutor(n: Int, suffix: String) = {
    val executionService = fixedThreadPool(n, "scalafp - %s".format(suffix))
    (executionService, ExecutionContext.fromExecutorService(executionService))
  }

  private def getPrimExecutor(n: Int, suffix: String): ExecutorService = fixedThreadPool(n, "prim - %s".format(suffix))

  private def perf1TwitterUtil(n: Int, m: Int, k: Int, executorThreads: Int) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getTwitterUtilExecutor(executorThreads, "perf 1 (n: %d,  m: %d, k: %d, executorThreads: %d)".format(n, m, k, executorThreads))
    val promises = n times com.twitter.util.Promise[Int]

    promises.foreach(p => {
      m times ex.executor.execute(
        /*
         * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
         * Without transform the benchmark performs much faster.
         */
        () => p.transform(_ => ex(counter.increment()))
      )
      k times ex.executor.execute(() => {
        p.updateIfEmpty(com.twitter.util.Return(1))
        counter.increment()
      })
    })

    // get ps
    promises.foreach(p => com.twitter.util.Await.result(p))

    counter.await()
  }

  private def perf2TwitterUtil(n: Int, executorThreads: Int) {
    val counter = new Synchronizer(n)
    var ex = getTwitterUtilExecutor(executorThreads, "perf 1 (n: %d, executorThreads: %d)".format(n, executorThreads))
    val promises = n times com.twitter.util.Promise[Int]

    def registerOnComplete(rest: Seq[com.twitter.util.Promise[Int]]) {
      if (rest.size > 0) {
        /*
         * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
         */
        rest(0).transform(_ =>
          ex({
            if (rest.size > 1) {
              rest(1).setValue(1)
              registerOnComplete(rest.tail)
            }
            counter.increment()
          }))
      }
    }

    registerOnComplete(promises)

    promises(0).setValue(1)
    counter.await()
  }

  private def perf3TwitterUtil(n: Int, executorThreads: Int) {
    val counter = new Synchronizer(n)
    var ex = getTwitterUtilExecutor(executorThreads, "perf 3 (n: %d,  executorThreads: %d)".format(n, executorThreads))
    val promises = n times com.twitter.util.Promise[Int]

    def registerOnComplete(rest: Seq[com.twitter.util.Promise[Int]]) {
      if (rest.size > 0) {
        /*
         * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
         */
        rest(0).transform(_ =>
          ex({
            if (rest.size > 1) {
              rest(1).setValue(1)
            }
            counter.increment()
          }))

        registerOnComplete(rest.tail)
      }
    }

    registerOnComplete(promises)

    promises(0).setValue(1)
    counter.await()
  }

  private def perf4TwitterUtil(n: Int, m: Int, executorThreads: Int) {
    val counter = new Synchronizer(n * m)
    var ex = getTwitterUtilExecutor(executorThreads, "perf 4 (n: %d,  executorThreads: %d)".format(n, executorThreads))
    val promises = n times com.twitter.util.Promise[Int]

    def linkPromises(promises: Seq[com.twitter.util.Promise[Int]]) {
      /*
       * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
       */
      m times promises(0).transform(_ => ex({ counter.increment() }))
      if (promises.size == 1) {
        promises(0).setValue(1)
      } else {
        promises(0).become(promises(1))
        linkPromises(promises.drop(1))
      }
    }

    linkPromises(promises)
    counter.await()
  }

  private def perf1ScalaFP(n: Int, m: Int, k: Int, executorThreads: Int) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getScalaFPExecutor(executorThreads, "perf 1 (n: %d,  m: %d, k: %d, executorThreads: %d)".format(n, m, k, executorThreads))
    val executionService = ex._1
    val executionContext = ex._2
    val promises = n times scala.concurrent.Promise[Int]

    promises.foreach(p => {
      m times executionService.execute(() => p.future.onComplete(_ => counter.increment())(executionContext))

      k times executionService.execute(() => {
        p.tryComplete(Success(1))
        counter.increment()
      })
    })

    // get ps
    promises.foreach(p => Await.result(p.future, Duration.Inf))

    counter.await()
  }

  private def perf2ScalaFP(n: Int, executorThreads: Int) {
    val counter = new Synchronizer(n)
    var ex = getScalaFPExecutor(executorThreads, "perf 2 (n: %d, executorThreads: %d)".format(n, executorThreads))
    val executionContext = ex._2
    val promises = n times scala.concurrent.Promise[Int]

    def registerOnComplete(rest: Seq[scala.concurrent.Promise[Int]]) {
      if (rest.size > 0) {
        rest(0).future.onComplete(_ => {
          if (rest.size > 1) {
            rest(1).trySuccess(1)
            registerOnComplete(rest.tail)
          }
          counter.increment()
        })(executionContext)
      }
    }

    registerOnComplete(promises)

    promises(0).trySuccess(1)
    counter.await()
  }

  private def perf3ScalaFP(n: Int, executorThreads: Int) {
    val counter = new Synchronizer(n)
    var ex = getScalaFPExecutor(executorThreads, "perf 3 (n: %d, executorThreads: %d)".format(n, executorThreads))
    val executionService = ex._1
    val executionContext = ex._2
    val promises = n times scala.concurrent.Promise[Int]

    def registerOnComplete(rest: Seq[scala.concurrent.Promise[Int]]) {
      if (rest.size > 0) {
        rest(0).future.onComplete(_ => {
          if (rest.size > 1) {
            rest(1).trySuccess(1)
          }
          counter.increment()
        })(executionContext)

        registerOnComplete(rest.tail)
      }
    }

    registerOnComplete(promises)

    promises(0).trySuccess(1)
    counter.await()
  }

  private def perf1Prim(n: Int, m: Int, k: Int, executorThreads: Int, f: Executor => Core[Int]) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getPrimExecutor(executorThreads, "perf 1 (n: %d,  m: %d, k: %d, executorThreads: %d)".format(n, m, k, executorThreads))
    val promises = n times f(ex)

    promises.foreach(p => {
      m times ex.execute(() => p.onCompleteC(_ => counter.increment()))
      k times ex.execute(() => {
        p.tryCompleteC(Success(1))
        counter.increment()
      })
    })

    // get ps
    promises.foreach(_.getC())

    counter.await()
  }

  private def perf2Prim(n: Int, executorThreads: Int, f: Executor => Core[Int]) {
    val counter = new Synchronizer(n)
    var ex = getPrimExecutor(executorThreads, "perf 2 (n: %d, executorThreads: %d)".format(n, executorThreads))
    val promises = n times f(ex)

    def registerOnCompleteC(rest: Seq[Core[Int]]) {
      if (rest.size > 0) {
        rest(0).onCompleteC(_ => {
          if (rest.size > 1) {
            rest(1).tryCompleteC(Success(1))
            registerOnCompleteC(rest.tail)
          }
          counter.increment()
        })
      }
    }

    registerOnCompleteC(promises)

    promises(0).tryCompleteC(Success(1))
    counter.await()
  }

  private def perf3Prim(n: Int, executorThreads: Int, f: Executor => Core[Int]) {
    val counter = new Synchronizer(n)
    var ex = getPrimExecutor(executorThreads, "perf 3 (n: %d, executorThreads: %d)".format(n, executorThreads))
    val promises = n times f(ex)

    def registerOnCompleteC(rest: Seq[Core[Int]]) {
      if (rest.size > 0) {
        rest(0).onCompleteC(_ => {
          if (rest.size > 1) rest(1).tryCompleteC(Success(1))
          counter.increment()
        })

        registerOnCompleteC(rest.tail)
      }
    }

    registerOnCompleteC(promises)

    promises(0).tryCompleteC(Success(1))
    counter.await()
  }

  private def perf4Prim(n: Int, m: Int, executorThreads: Int, f: Executor => FP[Int]) {
    val counter = new Synchronizer(n * m)
    var ex = getPrimExecutor(executorThreads, "perf 4 (n: %d,  m: %d, executorThreads: %d)".format(n, m, executorThreads))
    val promises = n times f(ex)

    def linkPromises(promises: Seq[FP[Int]]) {
      m times promises(0).onComplete(_ => counter.increment())
      if (promises.size == 1) {
        promises(0).trySuccess(1)
      } else {
        promises(0).tryCompleteWith(promises(1))
        linkPromises(promises.drop(1))
      }
    }

    linkPromises(promises)
    counter.await()
  }
}
