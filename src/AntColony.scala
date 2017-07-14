import org.apache.spark._
import org.apache.spark.graphx._ //{ Graph, VertexRDD }
import org.apache.spark.graphx.util.GraphGenerators



object AntColony {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphX Pregel Example").setMaster("local")
    val sc = new SparkContext(conf)

    val g = GraphLoader.edgeListFile(sc, "C:\\Users\\Heinrich\\Desktop\\roadNet-PA.txt")
    
    println(g.vertices.collect().count(e => true))
    println(g.edges.collect().count(e => true))
    
//    val nodes = 3000
//    val graph =
//      GraphGenerators.logNormalGraph(sc, 3000)
//      .mapVertices((_, _) => (0,0,0))
//      .mapEdges(e => (0,0))
//      .cache()
//        .logNormalGraph(sc, numVertices = nodes)
//        .mapVertices((_, _) => {
//          val rand = math.random
//          ((rand * nodes).toInt, (rand * nodes).toInt)
//        }).cache()
        
    

    val dd = readLine()
  }
}