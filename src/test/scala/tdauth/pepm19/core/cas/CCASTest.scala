package tdauth.pepm19.core.cas

import java.util.concurrent.Executors

import tdauth.pepm19.{AbstractFPTest, JavaExecutor}
import tdauth.pepm19.core.FP

class CCASTest extends AbstractFPTest {
  override def getFP: FP[Int] = new CCAS[Int](executor)

  private val executor = new JavaExecutor(Executors.newSingleThreadExecutor())
}
