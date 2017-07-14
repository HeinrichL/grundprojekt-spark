import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object GermanCities {
  def main(args: Array[String]) { 

      val sc = new SparkContext( "local", "GraphX German Cities", "/usr/local/spark") 
		
      // Create an RDD for the vertices
      val cities: RDD[(VertexId, String)] =
        sc.parallelize(Array((1L, "hamburg"), (2L, "berlin"),
                             (3L, "bremen"), (4L, "erfurt"),
                             (5L, "köln"), (6L, "stuttgart"),
                             (7L, "münchen")))
      // Create an RDD for edges
      val distances: RDD[Edge[Int]] =
        sc.parallelize(Array(Edge(1L, 2L, 300), Edge(1L, 3L, 200),
                             Edge(2L, 4L, 400),
                             Edge(3L, 5L, 400), Edge(3L,4L, 400),
                             Edge(4L, 5L, 500),Edge(4L, 6L, 500),Edge(4L, 7L, 500),
                             Edge(5L, 6L, 400),
                             Edge(6L, 7L, 300)))
      // Define a default user in case there are relationship with missing user
      val defaultUser = ("EmptyCity")
      // Build the initial Graph
      val graph = Graph(cities, distances, defaultUser)
      
      println(graph.edges.count)
      
      // Count all the edges where src > dst
      graph.edges.filter(e => e.srcId > e.dstId).count
   } 
}