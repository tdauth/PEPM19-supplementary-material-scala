package tdauth.pepm19.presentation
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

object Presentation extends App {

  // Futures:
  {
    val f = Future { 10 } // Asynchronous task ...
    f onComplete {
      _ match {
        case Success(v) => println(s"Result: $v")
        case Failure(e) => handleError(e)
      }
    }
  }

  // Promises:
  {
    val p = Promise[Int]
    val f = p.future
    f onComplete {
      _ match {
        case Success(v) => println(s"Result: $v")
        case Failure(e) => handleError(e)
      }
    }

    // Thread 1:
    global.execute(() => {
      p trySuccess 10
    })
    // Thread 2:
    global.execute(() => {
      p trySuccess 11
    })
  }

  {
    val x1 = holidayLocationSwitzerland()
      .flatMap(chf => exchangeRateCHFToEUR().map(_ * chf))
      .filter { _ <= 1000.0 }
    val x2 = holidayLocationUSA()
      .flatMap(usd => exchangeRateUSDToEUR().map(_ * usd))
      .filter { _ <= 1000.0 } // The same for the USA as for Switzerland ...
    val x3 = x1 fallbackTo x2
    x3 foreach bookHoliday

    Await.ready(x3, Duration.Inf)
  }

  private def handleError(e: Throwable): Unit = {}
  private def holidayLocationSwitzerland() = Future { 400.0 }
  private def holidayLocationUSA() = Future { 200.0 }
  private def exchangeRateCHFToEUR() = Future { 0.88 }
  private def exchangeRateUSDToEUR() = Future { 0.87 }
  private def bookHoliday(eur: Double) { println(s"Booked for $eur EUR") }
}
