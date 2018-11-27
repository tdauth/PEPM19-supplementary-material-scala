package tdauth.futuresandpromises.core.stm

import java.util.concurrent.Executors

import tdauth.futuresandpromises.{AbstractFPTest, JavaExecutor}
import tdauth.futuresandpromises.core.FP

class CSTMTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CSTM[Int](executor)

  private val executor = new JavaExecutor(Executors.newSingleThreadExecutor())
}
