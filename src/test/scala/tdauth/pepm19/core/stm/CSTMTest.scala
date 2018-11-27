package tdauth.pepm19.core.stm

import java.util.concurrent.Executors

import tdauth.pepm19.{AbstractFPTest, JavaExecutor}
import tdauth.pepm19.core.FP

class CSTMTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CSTM[Int](executor)

  private val executor = new JavaExecutor(Executors.newSingleThreadExecutor())
}