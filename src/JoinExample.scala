import org.apache.spark._
import org.apache.spark.graphx._ //{ Graph, VertexRDD }
import org.apache.spark.graphx.util.GraphGenerators

object JoinExample {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "GraphX German Cities", "/usr/local/spark")

    val vertexProperties = sc.textFile("/home/heinrich/Schreibtisch/g.vertices").map(line => {
      val vertex = line.split(",")
      (vertex(0).toLong, (vertex(1), vertex(2).toInt))
    })
    //val inputEdges = sc.textFile("/home/heinrich/Schreibtisch/graph-edges.txt") 

    val graph = GraphLoader.edgeListFile(sc, "/home/heinrich/Schreibtisch/g.edges")
      .mapVertices((id, value) => (value.toString(), value))
      .cache()

    println(graph.edges.collect().mkString("\n"))

    graph.triplets.foreach(println(_))

    val joined = graph.joinVertices(vertexProperties)((id, default, attrs) => (attrs._1, attrs._2))
    
    joined.triplets.foreach(t => println(s"(${t.srcAttr._1} -- ${t.dstAttr._1})"))
    
    val tricount = joined.triangleCount()
    tricount.vertices.foreach(println(_))
    
    val undirected = GraphLoader.edgeListFile(sc, "/home/heinrich/Schreibtisch/g.edges-undirected")
      .mapVertices((id, value) => (value.toString(), value))
      .cache()
    val joinedUndir = undirected.joinVertices(vertexProperties)((id, default, attrs) => (attrs._1, attrs._2))
    val rank = joinedUndir.pageRank(0.0001)
    
    rank.triplets.foreach(println(_))
    
  }
}