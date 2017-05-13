import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object GermanCitiesParsed {
  def main(args: Array[String]) { 

      val sc = new SparkContext( "local", "GraphX German Cities", "/usr/local/spark") 
		  val inputVertices = sc.textFile("/home/heinrich/Schreibtisch/graph-vertices.txt") 
		  val inputEdges = sc.textFile("/home/heinrich/Schreibtisch/graph-edges.txt") 
      // Create an RDD for the vertices
      val vertices = inputVertices.map 
        {line => 
          val vertex = line.split(",")
          (vertex(0).toLong, vertex(1))
        }
                      
      val edges = inputEdges.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, edge(2).toInt)
      }
      
		  
      // Define a default user in case there are relationship with missing user
      val defaultCity = ("EmptyCity")
      // Build the initial Graph
      val graph = Graph(vertices, edges, defaultCity).cache()
      
      
      // Count all users which are postdocs
      val noPostdocs = graph.edges.filter { e => e.dstId == 4L}.count
      
      println(noPostdocs)
      println(graph.edges.count)
 
      graph.triplets.foreach(println(_))
   } 
}