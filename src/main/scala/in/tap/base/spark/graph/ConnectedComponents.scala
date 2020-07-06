package in.tap.base.spark.graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

object ConnectedComponents {

  /**
    * Connected Components from Graph.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left VertexId.
    * The Right VertexId is the minimum VertexId belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    *
    * @param graph with your vertices & edges
    * @return connected vertex ids
    */
  def apply[V: ClassTag, E: ClassTag](graph: Graph[V, E])(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId)] = {
    graph
      .convertToCanonicalEdges()
      .connectedComponents()
      .vertices
  }

  /**
    * Connected Components from Vertices & Edges.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left VertexId.
    * The Right VertexId is the minimum VertexId belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    *
    * @param vertices of Graph
    * @param edges of Graph
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
    * Every Vertex Id will be preserved, and they will be unique. This is the Left VertexId.
    * The Right VertexId is the minimum VertexId belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    *
    * @param vertices of Graph
    * @param edges of Graph
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

  /**
    * Connected Components from Graph.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left VertexId.
    * The Right VertexId is the minimum VertexId belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    * The Option Vertex Attr belongs to the original graph, and uniquely corresponds to the Left Vertex Id.
    *
    * @param graph with your vertices & edges
    * @return connected vertex ids with attr preserved
    */
  def withVertexAttr[V: ClassTag, E: ClassTag](graph: Graph[V, E])(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId, Option[V])] = {
    val cc: Graph[VertexId, E] = {
      graph
        .convertToCanonicalEdges()
        .connectedComponents()
    }

    cc.outerJoinVertices(graph.vertices)(
        mapFunc = (_: VertexId, connectedComponent: VertexId, maybeVertexAttr: Option[V]) =>
          connectedComponent -> maybeVertexAttr
      )
      .vertices
      .map {
        case (uniqueVertexId: VertexId, (connectedComponent: VertexId, maybeVertexAttr: Option[V])) =>
          (uniqueVertexId, connectedComponent, maybeVertexAttr)
      }
  }

  /**
    * Connected Components from Graph.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left VertexId.
    * The Right VertexId is the minimum VertexId belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    * The Option Vertex Attr belongs to the original graph, and uniquely corresponds to the Left Vertex Id.
    *
    * @param vertices of Graph
    * @param edges of Graph
    * @return connected vertex ids with attr preserved
    */
  def withVertexAttr[V: ClassTag, E: ClassTag](
    vertices: RDD[(VertexId, V)],
    edges: RDD[Edge[E]]
  )(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId, Option[V])] = {
    withVertexAttr(Graph(vertices, edges))
  }

  /**
    * Connected Components from Graph.
    * Every Vertex Id will be preserved, and they will be unique. This is the Left VertexId.
    * The Right VertexId is the minimum VertexId belonging to the Connected Component.
    * While the Left is Unique, the Right will (hopefully & intentionally) have duplicates.
    * The Option Vertex Attr belongs to the original graph, and uniquely corresponds to the Left Vertex Id.
    *
    * @param vertices of Graph
    * @param edges of Graph
    * @return connected vertex ids with attr preserved
    */
  def withVertexAttr[V: ClassTag, E: ClassTag](
    vertices: Dataset[(VertexId, V)],
    edges: Dataset[Edge[E]]
  )(
    implicit spark: SparkSession
  ): RDD[(VertexId, VertexId, Option[V])] = {
    withVertexAttr(Graph(vertices.rdd, edges.rdd))
  }

}
