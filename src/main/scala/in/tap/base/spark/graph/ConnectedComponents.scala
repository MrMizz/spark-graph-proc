package in.tap.base.spark.graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

object ConnectedComponents {

  /**
    * Connected Components from [[Graph]].
    * Every Vertex Id will be preserved, and they will be unique. This is the Left [[VertexId]].
    * The Right [[VertexId]] is the minimum [[VertexId]] belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    *
    * @param graph with your vertices & edges
    * @return connected vertex ids
    */
  def apply[V: ClassTag, E: ClassTag](graph: Graph[V, E])(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId)] = {
    val cc: Graph[VertexId, E] = {
      graph
        .convertToCanonicalEdges()
        .connectedComponents()
    }

    cc.outerJoinVertices(graph.vertices)(
        mapFunc =
          (uniqueVertexId: VertexId, connectedComponent: VertexId, _: Option[V]) => uniqueVertexId -> connectedComponent
      )
      .vertices
      .map {
        case (_, (uniqueVertexId: VertexId, connectedComponent: VertexId)) =>
          uniqueVertexId -> connectedComponent
      }
  }

  /**
    * Connected Components from Vertices & Edges.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left [[VertexId]].
    * The Right [[VertexId]] is the minimum [[VertexId]] belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    *
    * @param vertices of [[Graph]]
    * @param edges of [[Graph]]
    * @return connected vertex ids
    */
  def apply[V: ClassTag, E: ClassTag](
    vertices: RDD[(VertexId, V)],
    edges: RDD[Edge[E]]
  )(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId)] = {
    ConnectedComponents(Graph(vertices, edges))
  }

  /**
    * Connected Components from Vertices & Edges.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left [[VertexId]].
    * The Right [[VertexId]] is the minimum [[VertexId]] belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    *
    * @param vertices of [[Graph]]
    * @param edges of [[Graph]]
    * @return connected vertex ids
    */
  def apply[V: ClassTag, E: ClassTag](
    vertices: Dataset[(VertexId, V)],
    edges: Dataset[Edge[E]]
  )(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId)] = {
    ConnectedComponents(Graph(vertices.rdd, edges.rdd))
  }

}
