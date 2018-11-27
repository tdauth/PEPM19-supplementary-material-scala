package tdauth.pepm19

trait Executor {
  def submit(f: () => Unit): Unit
  def shutdown: Unit = {}
}
