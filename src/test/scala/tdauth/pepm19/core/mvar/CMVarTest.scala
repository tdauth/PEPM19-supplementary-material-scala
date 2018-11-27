package tdauth.pepm19.core.mvar

import tdauth.pepm19.AbstractFPTest
import tdauth.pepm19.core.FP

class CMVarTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CMVar[Int](getExecutor)
}
