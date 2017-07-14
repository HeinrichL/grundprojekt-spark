import org.apache.spark._
import org.apache.spark.graphx._ //{ Graph, VertexRDD }
import org.apache.spark.graphx.util.GraphGenerators

object PregelExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphX Pregel Example").setMaster("local")
    val sc = new SparkContext(conf)

    val nodes = 30000//args(0).toInt
    val graph: Graph[(Int, Int), Int] =
      GraphGenerators
        .logNormalGraph(sc, numVertices = nodes)
        .mapVertices((_, _) => {
          val rand = math.random
          ((rand * nodes).toInt, (rand * nodes).toInt)
        }).cache()

    // get highest value of vertex
    val computed = graph.pregel(0)(
      (id, ownVal, recVal) => (ownVal._1, math.max(ownVal._2, recVal)), // Apply
      t => {
        if (t.srcAttr._2 > t.dstAttr._2) {
          t.attr = 500
          Iterator((t.dstId, t.srcAttr._2))
        } else {
          Iterator.empty
        }
      }, // Scatter
      (a, b) => math.max(a, b)) // Gather

    println(computed.edges.collect.mkString("\n"))

    val dd = readLine()
  }
}