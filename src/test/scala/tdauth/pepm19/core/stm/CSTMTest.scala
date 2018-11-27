package tdauth.pepm19.core.stm

import tdauth.pepm19.AbstractFPTest
import tdauth.pepm19.core.FP

class CSTMTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CSTM[Int](getExecutor)
}
