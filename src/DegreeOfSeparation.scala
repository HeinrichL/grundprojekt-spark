import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators


object DegreeOfSeparation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphX Pregel Example").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val numberOfNodes = 3000

    // create graph
    // set values of all vertices to infinity, only srcVertex is 0
    //    val graph: Graph[Int, Int] =
    //      GraphGenerators
    //        .logNormalGraph(sc, numVertices = numberOfNodes)
    //        .mapVertices((id, lo) => {
    //          if (id != srcId) {
    //            Integer.MAX_VALUE
    //          } else {
    //            0
    //          }
    //        }).cache()

    val rmat =//: Graph[Int, Int] =
      GraphGenerators.logNormalGraph(sc, 50000).cache()
        //.rmatGraph(sc, 10000, 200000).cache()
    
    val srcId = rmat.vertices.first()._1

    val graph = rmat.mapVertices((id, lo) => {
          if (id.toLong != srcId) {
            Integer.MAX_VALUE
          } else {
            0
          }
    }).cache()
    
//    val srcId = 214328887L
//    val graph = GraphLoader.edgeListFile(sc, "C:\\Users\\Heinrich\\Desktop\\twitter_combined.txt")
//      .mapVertices((id, lo) => {
//        if (id != srcId) {
//          Integer.MAX_VALUE
//        } else {
//          0
//        }
//      }).cache()

    // get degree of separation
    // 1. start with infinity as initial message
    // 2. if own value is not infinity then send own value incremented by 1 to all neighbors
    // 3. from all received messages, choose the minimum
      
//    val computed = graph.pregel(Integer.MAX_VALUE)(
//      (id, ownVal, recVal) => math.min(ownVal, recVal), // Apply
//      t => {
//        if (t.srcAttr != Integer.MAX_VALUE && t.srcAttr + 1 < t.dstAttr) {
//          Iterator((t.dstId, t.srcAttr + 1))
//        } else if (t.dstAttr != Integer.MAX_VALUE && t.dstAttr + 1 < t.srcAttr) {
//          Iterator((t.srcId, t.dstAttr + 1))
//        } else {
//          Iterator.empty
//        }
//      }, // Scatter
//      (a, b) => math.min(a, b)) // Gather
    
    val computed = graph.triangleCount()

    //println(computed.triplets.collect().filter(t => t.srcId == srcId || t.dstId == srcId).mkString("\n"))

    computed.vertices.count()
    println(computed.vertices.collect().mkString("\n"))
  }
}