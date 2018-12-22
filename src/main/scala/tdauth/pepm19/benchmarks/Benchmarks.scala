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
  * We have four different performance tests but test 1 and test 2 use the same method `Benchmarks#perf1Prim`.
  * Therefore, we only have four different `perf<n>` methods.
  */
object Benchmarks extends App {
  val Tests = 1 to 4
  val ImplementationNames =
    Vector("Twitter Util", "Scala FP", "Prim CAS", "Prim MVar", "Prim STM", "Prim CAS Promise Linking", "Prim CAS Fixed Promise Linking")
  val PlotFileSuffixes = Vector("twitterutil", "scalafp", "cas", "mvar", "stm", "caspromiselinking", "casfixedpromiselinking")

  val Iterations = 10

  /**
    * Bear in mind that there is always the main thread.
    */
  val ExecutorThreads = Vector(1) //Vector(1, 2, 4, 8)

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
  val Test5N = 100000
  val Test5M = 100

  deletePlotFiles()

  printf("We have %d available processors.\n", Runtime.getRuntime().availableProcessors())
  runTest5()
  //runAllTests

  private implicit def intWithTimes[T](n: Int) = new {
    def times(f: => T) = 1 to n map { _ =>
      f
    }
  }

  private def getPlotFileName(testNumber: Int, plotFileSuffix: String): String =
    "test" + testNumber + "_scala_" + plotFileSuffix + ".dat"

  private def deletePlotFiles() {
    val files = for {
      testNumber <- Tests
      plotFileSuffix <- PlotFileSuffixes

    } yield new File(getPlotFileName(testNumber, plotFileSuffix))
    files.filter(_.exists).foreach(_.delete)
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

  private def runAll(testNumber: Int,
                     executorThreads: Int,
                     t0: TestFunction,
                     t1: TestFunction,
                     t2: TestFunction,
                     t3: TestFunction,
                     t4: TestFunction,
                     t5: TestFunction,
                     t6: TestFunction): Unit = {
    Vector(t0, t1, t2, t3, t4, t5, t6).zipWithIndex.foreach {
      case (t, n) => {
        println(ImplementationNames(n))
        runTest(PlotFileSuffixes(n), testNumber, executorThreads, t)
      }
    }
  }

  private def test1(executorThreads: Int) {
    val n = Test1N
    val m = Test1M
    val k = Test1K
    runAll(
      1,
      executorThreads,
      () => perf1TwitterUtil(n, m, k, executorThreads),
      () => perf1ScalaFP(n, m, k, executorThreads),
      () => perf1Prim(n, m, k, executorThreads, ex => new CCAS(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CMVar(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CSTM(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CCASPromiseLinking(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CCASFixedPromiseLinking(ex))
    )
  }

  private def test2(executorThreads: Int) {
    val n = Test2N
    val m = Test2M
    val k = Test2K
    runAll(
      2,
      executorThreads,
      () => perf1TwitterUtil(n, m, k, executorThreads),
      () => perf1ScalaFP(n, m, k, executorThreads),
      () => perf1Prim(n, m, k, executorThreads, ex => new CCAS(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CMVar(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CSTM(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CCASPromiseLinking(ex)),
      () => perf1Prim(n, m, k, executorThreads, ex => new CCASFixedPromiseLinking(ex))
    )
  }

  private def test3(executorThreads: Int) {
    val n = Test3N
    runAll(
      3,
      executorThreads,
      () => perf2TwitterUtil(n, executorThreads),
      () => perf2ScalaFP(n, executorThreads),
      () => perf2Prim(n, executorThreads, ex => new CCAS(ex)),
      () => perf2Prim(n, executorThreads, ex => new CMVar(ex)),
      () => perf2Prim(n, executorThreads, ex => new CSTM(ex)),
      () => perf2Prim(n, executorThreads, ex => new CCASPromiseLinking(ex)),
      () => perf2Prim(n, executorThreads, ex => new CCASFixedPromiseLinking(ex))
    )
  }

  private def test4(executorThreads: Int) {
    val n = Test4N
    runAll(
      4,
      executorThreads,
      () => perf3TwitterUtil(n, executorThreads),
      () => perf3ScalaFP(n, executorThreads),
      () => perf3Prim(n, executorThreads, ex => new CCAS(ex)),
      () => perf3Prim(n, executorThreads, ex => new CMVar(ex)),
      () => perf3Prim(n, executorThreads, ex => new CSTM(ex)),
      () => perf3Prim(n, executorThreads, ex => new CCASPromiseLinking(ex)),
      () => perf3Prim(n, executorThreads, ex => new CCASFixedPromiseLinking(ex))
    )
  }

  private def test5(executorThreads: Int): Unit = {
    val n = Test5N
    val m = Test5M
    runAll(
      5,
      executorThreads,
      () => (), // TODO Add perf4 for Twitter FP
      () => (), // TODO Add perf4 for Scala FP
      () => perf4Prim(n, m, executorThreads, ex => new CCAS(ex)),
      () => perf4Prim(n, m, executorThreads, ex => new CMVar(ex)),
      () => perf4Prim(n, m, executorThreads, ex => new CSTM(ex)),
      () => perf4Prim(n, m, executorThreads, ex => new CCASPromiseLinking(ex)),
      () => perf4Prim(n, m, executorThreads, ex => new CCASFixedPromiseLinking(ex))
    )
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
    runTest1
    runTest2
    runTest3
    runTest4
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
    }
  }

  private def threadFactory(prefix: String): ThreadFactory =
    new SimpleThreadFactory(prefix)

  private def fixedThreadPool(n: Int, prefix: String): ExecutorService =
    Executors.newFixedThreadPool(n, threadFactory(prefix))

  private def getTwitterUtilExecutor(n: Int) =
    com.twitter.util.FuturePool(fixedThreadPool(n, "twitterutil"))

  private def getScalaFPExecutor(n: Int) = {
    val executionService = fixedThreadPool(n, "scalafp")
    (executionService, ExecutionContext.fromExecutorService(executionService))
  }

  private def getPrimExecutor(n: Int): ExecutorService = fixedThreadPool(n, "prim")

  private def perf1TwitterUtil(n: Int, m: Int, k: Int, executorThreads: Int) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getTwitterUtilExecutor(executorThreads)
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
    var ex = getTwitterUtilExecutor(executorThreads)
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
    var ex = getTwitterUtilExecutor(executorThreads)
    val promises = n times com.twitter.util.Promise[Int]

    def registerOnComplete(rest: Seq[com.twitter.util.Promise[Int]]) {
      if (rest.size > 0) {
        /*
         * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
         */
        rest(0).transform(_ => {
          ex({
            if (rest.size > 1) {
              rest(1).setValue(1)
            }
            counter.increment()
          })
        })

        registerOnComplete(rest.tail)
      }
    }

    registerOnComplete(promises)

    promises(0).setValue(1)
    counter.await()
  }

  private def perf1ScalaFP(n: Int, m: Int, k: Int, executorThreads: Int) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getScalaFPExecutor(executorThreads)
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
    var ex = getScalaFPExecutor(executorThreads)
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
    var ex = getScalaFPExecutor(executorThreads)
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
    var ex = getPrimExecutor(executorThreads)
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
    var ex = getPrimExecutor(executorThreads)
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
    var ex = getPrimExecutor(executorThreads)
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

  /**
    * Creates a chain of promises by linking them with [[tdauth.pepm19.FP.transformWith]] which uses
    * [[tdauth.pepm19.FP.tryCompleteWith]].
    * The second promise is completed with the first one, the third with the second one etc.
    * The transformation uses an already completed promise, so the transformation takes place as soon as the final promise
    * of the chain is completed.
    * The final promise is a successful promise which will complete the whole chain.
    *
    * Each intermediate promise gets m callbacks which will all be moved to the first promise if it uses promise linking.
    *
    * We cannot use [[tdauth.pepm19.FP.tryCompleteWith]] directly since neither Scala FP nor Twitter
    * Util implement promise linking for this method but for `transformWith` for Scala FP and `transform` for Twitter Util.
    *
    * This benchmark is similiar to [[https://github.com/scala/scala/blob/2.12.x/test/files/run/t7336.scala t7336]] but without creating an array in the closure or trying to
    * exceed the memory and with additional m callbacks.
    * Note that the exhausting memory was due to a bug in the Scala compiler.
    */
  def perf4Prim(n: Int, m: Int, cores: Int, f: Executor => FP[Int]) {
    val counter = new Synchronizer(n * m)
    var ex = getPrimExecutor(cores)

    def linkPromises(i: Int): FP[Int] = {
      val successfulP = f(ex)
      successfulP.trySuccess(10)
      val result = successfulP.transformWith(_ =>
        if (i == 0) {
          val successfulP = f(ex)
          successfulP.trySuccess(1)
          successfulP
        } else linkPromises(i - 1))

      m times result.onComplete(_ => counter.increment())
      result
    }

    linkPromises(n)

    counter.await()
  }
}
