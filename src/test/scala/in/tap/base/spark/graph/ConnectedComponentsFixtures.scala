package in.tap.base.spark.graph

import org.apache.spark.graphx.{Edge, VertexId}

trait ConnectedComponentsFixtures {

  val vertices: Seq[(VertexId, String)] = {
    Seq(
      1L -> "A",
      2L -> "B",
      3L -> "C",
      4L -> "D",
      5L -> "E"
    )
  }

  val edges1: Seq[Edge[Int]] = {
    Seq(
      Edge(srcId = 1L, dstId = 2L, 1),
      Edge(srcId = 2L, dstId = 3L, 1)
    )
  }

}
