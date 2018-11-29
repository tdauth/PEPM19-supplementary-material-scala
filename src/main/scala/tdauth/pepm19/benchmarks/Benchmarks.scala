package tdauth.pepm19.benchmarks

import java.io.{File, FileWriter}
import java.util.Locale
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executor, ExecutorService, Executors, ThreadFactory}

import tdauth.pepm19.{CSTM, _}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Success
import scala.util.control.NonFatal

/**
  * Compares the performance of our FP implementation to the performance of Scala FP and Twitter Util.
  * We have four different performance tests but test 1 and test 2 use the same method [[Benchmarks#perf1Prim]].
  * Therefore, we only have four different `perf<n>` methods.
  */
object Benchmarks extends App {
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

  deletePlotFiles

  printf("We have %d available processors.\n", Runtime.getRuntime().availableProcessors())
  runAllTests

  def getPlotFileName(testNumber: Int, plotFileSuffix: String): String =
    "test" + testNumber + "_scala_" + plotFileSuffix + ".dat"

  def deletePlotFiles() {
    val files = for {
      testNumber <- Vector(1, 2, 3, 4)
      plotFileSuffix <- Vector("twitterutil", "scalafp", "cas", "mvar", "stm")

    } yield new File(getPlotFileName(testNumber, plotFileSuffix))
    files.filter(_.exists).foreach(_.delete)
  }

  def writeEntryIntoPlotFile(plotFilePath: String, cores: Int, time: Double) {
    val fileWriter = new FileWriter(plotFilePath, true)
    try {
      fileWriter.append("%d  %.2f\n".formatLocal(Locale.US, cores, time))
    } catch {
      case NonFatal(t) => println(s"Exception: ${t}")
    } finally fileWriter.close()
  }

  type TestFunction = () => Unit

  /**
    * @param t The test function which returns time which has to be substracted from the exectuion time since it should not be measured.
    */
  def execTest(t: TestFunction): Double = {
    System.gc
    val start = System.nanoTime()
    t()
    val fin = System.nanoTime()
    val result = (fin - start)
    val seconds = result.toDouble / 1000000000.0
    printf("Time: %.2fs\n", seconds)
    seconds
  }

  def runTest(plotFileSuffix: String, testNumber: Int, cores: Int, t: TestFunction) {
    val rs = for (i <- (1 to Iterations)) yield execTest(t)
    val xs = rs.sorted
    val low = xs.head
    val high = xs.last
    val m = xs.length.toDouble
    val av = xs.sum / m
    printf("low: %.2fs high: %.2fs avrg: %.2fs\n", low, high, av)
    writeEntryIntoPlotFile(getPlotFileName(testNumber, plotFileSuffix), cores, av)
  }

  def runAll(testNumber: Int, cores: Int, t0: TestFunction, t1: TestFunction, t2: TestFunction, t3: TestFunction, t4: TestFunction): Unit = {
    println("Twitter Util")
    runTest("twitterutil", testNumber, cores, t0)
    println("Scala FP")
    runTest("scalafp", testNumber, cores, t1)
    println("Prim CAS")
    runTest("cas", testNumber, cores, t2)
    println("Prim MVar")
    runTest("mvar", testNumber, cores, t3)
    println("Prim STM")
    runTest("stm", testNumber, cores, t4)
  }

  def test1(cores: Int) {
    val n = Test1N
    val m = Test1M
    val k = Test1K
    runAll(
      1,
      cores,
      () => perf1TwitterUtil(n, m, k, cores),
      () => perf1ScalaFP(n, m, k, cores),
      () => perf1Prim(n, m, k, cores, ex => new CCAS(ex)),
      () => perf1Prim(n, m, k, cores, ex => new CMVar(ex)),
      () => perf1Prim(n, m, k, cores, ex => new CSTM(ex))
    )
  }

  def test2(cores: Int) {
    val n = Test2N
    val m = Test2M
    val k = Test2K
    runAll(
      2,
      cores,
      () => perf1TwitterUtil(n, m, k, cores),
      () => perf1ScalaFP(n, m, k, cores),
      () => perf1Prim(n, m, k, cores, ex => new CCAS(ex)),
      () => perf1Prim(n, m, k, cores, ex => new CMVar(ex)),
      () => perf1Prim(n, m, k, cores, ex => new CSTM(ex))
    )
  }

  def test3(cores: Int) {
    val n = Test3N
    runAll(
      3,
      cores,
      () => perf2TwitterUtil(n, cores),
      () => perf2ScalaFP(n, cores),
      () => perf2Prim(n, cores, ex => new CCAS(ex)),
      () => perf2Prim(n, cores, ex => new CMVar(ex)),
      () => perf2Prim(n, cores, ex => new CSTM(ex))
    )
  }

  def test4(cores: Int) {
    val n = Test4N
    runAll(
      4,
      cores,
      () => perf3TwitterUtil(n, cores),
      () => perf3ScalaFP(n, cores),
      () => perf3Prim(n, cores, ex => new CCAS(ex)),
      () => perf3Prim(n, cores, ex => new CMVar(ex)),
      () => perf3Prim(n, cores, ex => new CSTM(ex))
    )
  }

  def runTestForCores(name: String, t: (Int) => Unit) {
    val nameSeparator = "=" * 40
    println(nameSeparator)
    println(name)
    println(nameSeparator)
    val coresSeparator = "-" * 40

    ExecutorThreads.foreach(c => {
      println(coresSeparator)
      println("Cores: " + c)
      println(coresSeparator)
      t(c)
    })
  }

  def runTest1 = runTestForCores("Test 1", test1)
  def runTest2 = runTestForCores("Test 2", test2)
  def runTest3 = runTestForCores("Test 3", test3)
  def runTest4 = runTestForCores("Test 4", test4)

  def runAllTests {
    runTest1
    runTest2
    runTest3
    runTest4
  }

  /**
    * Renames the threads with a prefix.
    * This helps to distinguish them when analyzing profiling data.
    */
  class SimpleThreadFactory(prefix: String) extends ThreadFactory {
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
  class Synchronizer(max: Int) {
    var lock = new ReentrantLock()
    var condition = lock.newCondition()
    var counter = 0

    def increment {
      lock.lock()
      try {
        counter = counter + 1
        condition.signal
      } finally {
        lock.unlock();
      }
    }

    def await {
      var notFull = true
      do {
        lock.lock()
        try {
          notFull = counter < max
          if (notFull) condition.await
        } finally {
          lock.unlock();
        }
      } while (notFull)
    }
  }

  def threadFactory(prefix: String): ThreadFactory =
    new SimpleThreadFactory(prefix)

  def fixedThreadPool(n: Int, prefix: String): ExecutorService =
    Executors.newFixedThreadPool(n, threadFactory(prefix))

  def getTwitterUtilExecutor(n: Int) =
    com.twitter.util.FuturePool(fixedThreadPool(n, "twitterutil"))

  def getScalaFPExecutor(n: Int): Tuple2[ExecutorService, ExecutionContext] = {
    val executionService = fixedThreadPool(n, "scalafp")
    (executionService, ExecutionContext.fromExecutorService(executionService))
  }

  def getPrimExecutor(n: Int): ExecutorService = fixedThreadPool(n, "prim")

  def perf1TwitterUtil(n: Int, m: Int, k: Int, cores: Int) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getTwitterUtilExecutor(cores)

    val promises = (1 to n).map(_ => com.twitter.util.Promise[Int])

    promises.foreach(p => {
      1 to m foreach (_ => {
        ex.executor.submit(new Runnable {
          /*
           * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
           * Without transform the benchmark performs much faster.
           */
          override def run(): Unit = p.transform(t => ex(counter.increment))
        })
      })
      1 to k foreach (_ => {
        ex.executor.submit(new Runnable {
          override def run(): Unit = {
            p.updateIfEmpty(com.twitter.util.Return(1))
            counter.increment
          }
        })
      })
    })

    // get ps
    promises.foreach(p => com.twitter.util.Await.result(p))

    counter.await
  }

  def perf2TwitterUtil(n: Int, cores: Int) {
    val counter = new Synchronizer(n)
    var ex = getTwitterUtilExecutor(cores)

    val promises = (1 to n).map(_ => com.twitter.util.Promise[Int])

    def registerOnComplete(rest: Seq[com.twitter.util.Promise[Int]]) {
      val p1 = if (rest.size > 0) rest(0) else null
      val p2 = if (rest.size > 1) rest(1) else null
      if (p1 ne null) {
        /*
         * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
         */
        p1.transform(t =>
          ex({
            if (p2 ne null) {
              p2.setValue(1)
              registerOnComplete(rest.tail)
            }
            counter.increment
          }))
      }
    }

    registerOnComplete(promises)

    promises(0).setValue(1)
    counter.await
  }

  def perf3TwitterUtil(n: Int, cores: Int) {
    val counter = new Synchronizer(n)
    var ex = getTwitterUtilExecutor(cores)

    val promises = (1 to n).map(_ => com.twitter.util.Promise[Int])

    def registerOnComplete(rest: Seq[com.twitter.util.Promise[Int]]) {
      val p1 = if (rest.size > 0) rest(0) else null
      val p2 = if (rest.size > 1) rest(1) else null
      if (p1 ne null) {
        /*
         * We cannot use respond for Twitter Util since there is no way of specifying the executor for the callback.
         */
        p1.transform(t => {
          ex({
            if (p2 ne null) {
              p2.setValue(1)
            }
            counter.increment
          })
        })

        if (p2 ne null) {
          registerOnComplete(rest.tail)
        }
      }
    }

    registerOnComplete(promises)

    promises(0).setValue(1)
    counter.await
  }

  def perf1ScalaFP(n: Int, m: Int, k: Int, cores: Int) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getScalaFPExecutor(cores)
    val executionService = ex._1
    val executionContext = ex._2

    val promises = (1 to n).map(_ => scala.concurrent.Promise[Int])

    promises.foreach(p => {
      1 to m foreach (_ => {
        executionService.submit(new Runnable {
          override def run(): Unit =
            p.future.onComplete(t => counter.increment)(executionContext)
        })
      })
      1 to k foreach (_ => {
        executionService.submit(new Runnable {
          override def run(): Unit = {
            p.tryComplete(Success(1))
            counter.increment
          }
        })
      })
    })

    // get ps
    promises.foreach(p => Await.result(p.future, Duration.Inf))

    counter.await
  }

  def perf2ScalaFP(n: Int, cores: Int) {
    val counter = new Synchronizer(n)
    var ex: Tuple2[ExecutorService, ExecutionContext] = null
    val difference = getScalaFPExecutor(cores)
    val executionService = ex._1
    val executionContext = ex._2

    val promises = (1 to n).map(_ => scala.concurrent.Promise[Int])

    def registerOnComplete(rest: Seq[scala.concurrent.Promise[Int]]) {
      val p1 = if (rest.size > 0) rest(0) else null
      val p2 = if (rest.size > 1) rest(1) else null
      if (p1 ne null) {
        p1.future.onComplete(t => {
          if (p2 ne null) {
            p2.trySuccess(1)
            registerOnComplete(rest.tail)
          }
          counter.increment
        })(executionContext)
      }
    }

    registerOnComplete(promises)

    promises(0).trySuccess(1)
    counter.await
  }

  def perf3ScalaFP(n: Int, cores: Int) {
    val counter = new Synchronizer(n)
    var ex = getScalaFPExecutor(cores)
    val executionService = ex._1
    val executionContext = ex._2

    val promises = (1 to n).map(_ => scala.concurrent.Promise[Int])

    def registerOnComplete(rest: Seq[scala.concurrent.Promise[Int]]) {
      val p1 = if (rest.size > 0) rest(0) else null
      val p2 = if (rest.size > 1) rest(1) else null
      if (p1 ne null) {
        p1.future.onComplete(t => {
          if (p2 ne null) {
            p2.trySuccess(1)
          }
          counter.increment
        })(executionContext)

        if (p2 ne null) {
          registerOnComplete(rest.tail)
        }
      }
    }

    registerOnComplete(promises)

    promises(0).trySuccess(1)
    counter.await
  }

  def perf1Prim(n: Int, m: Int, k: Int, cores: Int, f: (Executor) => FP[Int]) {
    val counter = new Synchronizer(n * (m + k))
    var ex = getPrimExecutor(cores)
    val promises = (1 to n).map(_ => f(ex))

    promises.foreach(p => {
      1 to m foreach (_ => ex.execute(() => p.onComplete(t => counter.increment)))
      1 to k foreach (_ =>
        ex.execute(() => {
          p.trySuccess(1)
          counter.increment
        }))
    })

    // get ps
    promises.foreach(p => p.getP)

    counter.await
  }

  def perf2Prim(n: Int, cores: Int, f: (Executor) => FP[Int]) {
    val counter = new Synchronizer(n)
    var ex = getPrimExecutor(cores)
    val promises = (1 to n).map(_ => f(ex))

    def registerOnComplete(rest: Seq[FP[Int]]) {
      val p1 = if (rest.size > 0) rest(0) else null
      val p2 = if (rest.size > 1) rest(1) else null
      if (p1 ne null) {
        p1.onComplete(t => {
          if (p2 ne null) {
            p2.trySuccess(1)
            registerOnComplete(rest.tail)
          }
          counter.increment
        })
      }
    }

    registerOnComplete(promises)

    promises(0).trySuccess(1)
    counter.await
  }

  def perf3Prim(n: Int, cores: Int, f: (Executor) => FP[Int]) {
    val counter = new Synchronizer(n)
    var ex = getPrimExecutor(cores)
    val promises = (1 to n).map(_ => f(ex))

    def registerOnComplete(rest: Seq[FP[Int]]) {
      val p1 = if (rest.size > 0) rest(0) else null
      val p2 = if (rest.size > 1) rest(1) else null
      if (p1 ne null) {
        p1.onComplete(t => {
          if (p2 ne null) p2.trySuccess(1)
          counter.increment
        })

        registerOnComplete(rest.tail)
      }
    }

    registerOnComplete(promises)

    promises(0).trySuccess(1)
    counter.await
  }
}
