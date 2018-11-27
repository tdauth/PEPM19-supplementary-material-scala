package tdauth.pepm19.core.cas

import tdauth.pepm19.AbstractFPTest
import tdauth.pepm19.core.FP

class CCASTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CCAS[Int](getExecutor)
}
