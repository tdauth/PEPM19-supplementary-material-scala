package tdauth.pepm19

class CCASFixedPromiseLinkingNoAggregateTest extends AbstractFPTest(false) {
  type FPLinkingType = CCASFixedPromiseLinking[Int]

  override def getFP: FP[Int] = new FPLinkingType(getExecutor, false, 0)
}
