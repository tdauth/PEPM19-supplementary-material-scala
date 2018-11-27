package tdauth.pepm19.core.mvar

import java.util.concurrent.Executors

import tdauth.pepm19.{AbstractFPTest, JavaExecutor}
import tdauth.pepm19.core.FP

class CMVarTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CMVar[Int](executor)

  private val executor = new JavaExecutor(Executors.newSingleThreadExecutor())
}
