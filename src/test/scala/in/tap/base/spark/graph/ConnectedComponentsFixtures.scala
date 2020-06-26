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

  val edges2: Seq[Edge[String]] = {
    Seq(
      Edge(srcId = 1L, dstId = 2L, "12"),
      Edge(srcId = 2L, dstId = 1L, "21"),
      Edge(srcId = 2L, dstId = 1L, "21"),
      Edge(srcId = 3L, dstId = 2L, "32"),
      Edge(srcId = 5L, dstId = 5L, "55")
    )
  }

  val edges3: Seq[Edge[Double]] = {
    Seq.empty[Edge[Double]]
  }

}
