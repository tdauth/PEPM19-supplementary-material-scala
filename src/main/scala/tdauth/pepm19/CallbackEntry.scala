package tdauth.pepm19

import scala.util.Try

/**
  * Abstract type for the current value stored by a future/promise.
  */
sealed trait CallbackEntry

/**
  * Indicates that there is no callback.
  */
case object Noop extends CallbackEntry

/**
  * Single backwards linked list of callbacks.
  * When appending a callback, it will only be reversed link and the current link will be replaced by the new element.
  * This should improve the performance on appending elements compared to storing the whole list.
  * This does also mean that when the callbacks are called, they will be called in reverse order.
  */
case class LinkedCallbackEntry[T](final val c: Try[T] => Unit, final val prev: CallbackEntry) extends CallbackEntry

/**
  * If there is no link to previous callback entry yet, only the callback has to be stored.
  */
case class SingleCallbackEntry[T](final val c: Try[T] => Unit) extends CallbackEntry
