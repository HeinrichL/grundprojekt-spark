import org.apache.spark._
import org.apache.spark.graphx._ //{ Graph, VertexRDD }
import org.apache.spark.graphx.util.GraphGenerators

object LargeGraphExample {
  def main(args: Array[String]) {
    val conf = new SparkConf()//.setAppName("GraphX Pregel Example")//.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 1000000).mapVertices((id, _) => id.toDouble).cache()

    println(graph.inDegrees.reduce(max))
    println(graph.outDegrees.reduce(max))
    val dd = readLine()
  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }
}