package tdauth.pepm19

class CMVarTest extends AbstractFPTest(true) {
  override def getFP: FP[Int] = new CMVar[Int](getExecutor)
}
