package in.tap.base.spark.graph

import in.tap.base.spark.BaseSpec

class ConnectedComponentsSpec extends BaseSpec with ConnectedComponentsFixtures {

  it should "connect vertices that are transitively connected" in {
    import spark.implicits._

    ConnectedComponents[String, Int](vertices.toDS, edges1.toDS).collect.toSeq.sortBy(_._1) shouldBe {
      Seq(
        1L -> 1L,
        2L -> 1L,
        3L -> 1L,
        4L -> 4L,
        5L -> 5L
      )
    }

    // non-canonical edges should not be a problem
    ConnectedComponents[String, String](vertices.toDS, edges2.toDS).collect.toSeq.sortBy(_._1) shouldBe {
      Seq(
        1L -> 1L,
        2L -> 1L,
        3L -> 1L,
        4L -> 4L,
        5L -> 5L
      )
    }
  }

}
