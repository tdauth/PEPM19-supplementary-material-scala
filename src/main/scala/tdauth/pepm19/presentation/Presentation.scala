package tdauth.pepm19.presentation
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

object Presentation extends App {

  // Scala Futures:
  {
    val f = Future { getHotel() }
    // ...
    f onComplete { hotel =>
      bookHotel(hotel)
    }
    // ...
    f onComplete { hotel =>
      informFriends(hotel)
    }
  }

  // Scala Promises:
  {
    val p = Promise[Hotel]
    // ...
    p.future onComplete { hotel =>
      bookHotel(hotel)
    }
    // ...

    // Thread 1:
    global.execute(() => {
      p trySuccess HotelA
    })
    // Thread 2:
    global.execute(() => {
      p trySuccess HotelB
    })
  }

  // Extended Example: Holiday Planning in Scala
  {
    val switzerland = Future { getHotelSwitzerland() }
    val usa = Future { getHotelUSA() }
    val hotel = switzerland fallbackTo usa
    hotel onComplete { hotel =>
      bookHotel(hotel)
    }

    Await.ready(hotel, Duration.Inf)
  }

  sealed trait Hotel
  case object HotelA extends Hotel
  case object HotelB extends Hotel

  private def getHotel(): Hotel = HotelA
  private def bookHotel(hotel: Try[Hotel]) = println(s"Book hotel $hotel.")
  private def informFriends(hotel: Try[Hotel]) = println(s"Dear, friends, I have booked hotel $hotel.")
  private def getHotelSwitzerland() = HotelA
  private def getHotelUSA() = HotelB
}
