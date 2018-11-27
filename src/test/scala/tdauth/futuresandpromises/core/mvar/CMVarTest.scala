package tdauth.futuresandpromises.core.mvar

import java.util.concurrent.Executors

import tdauth.futuresandpromises.{AbstractFPTest, JavaExecutor}
import tdauth.futuresandpromises.core.FP

class CMVarTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CMVar[Int](executor)

  private val executor = new JavaExecutor(Executors.newSingleThreadExecutor())
}
