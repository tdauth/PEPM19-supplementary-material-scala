package tdauth.pepm19

class CCASTest extends AbstractFPTest(true) {
  override def getFP: FP[Int] = new CCAS[Int](getExecutor)
}
