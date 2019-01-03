package tdauth.pepm19

class CSTMTest extends AbstractFPTest(true) {
  override def getFP: FP[Int] = new CSTM[Int](getExecutor)
}
