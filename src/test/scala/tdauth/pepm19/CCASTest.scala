package tdauth.pepm19

class CCASTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CCAS[Int](getExecutor)
}
