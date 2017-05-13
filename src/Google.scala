import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object Google {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "GraphX German Cities", "/usr/local/spark")

//    val input = sc.textFile("/home/heinrich/Schreibtisch/web-Google.txt")
//
//    // Create an RDD for the vertices
//    val vertices = input.map { line =>
//      val vertex = line.split("\t")
//      (vertex(0).toLong, vertex(0))
//    }.distinct()
//
//    val edges = input.map {
//      line =>
//        val edge = line.split("\t")
//        Edge(edge(0).toLong, edge(1).toLong, "e")
//    }
//
//    // Define a default user in case there are relationship with missing user
//    val defaultCity = ("Empty")
//
//    // Build the initial Graph
    
    val graph = GraphLoader.edgeListFile(sc, "/home/heinrich/Schreibtisch/web-Google.txt")

        val timeddd = time {
          println(graph.edges.count)
        }
        
        val timedd = time {
          println(graph.vertices.count)
        }

        println(timedd)
        println(timeddd)

    println(graph.triangleCount().vertices.collect().mkString("\n"))

    val dd = readLine()
  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + ((t1 - t0) / 1000000) + "ms")
    result
  }
}