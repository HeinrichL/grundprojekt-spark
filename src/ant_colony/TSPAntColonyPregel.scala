package ant_colony

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.Iterator
import org.apache.spark.graphx.util.GraphGenerators

object TSPAntColonyPregel {
  val Q = 500 // pheromon deposit constant
  val alpha = -1.5 // influence of attractiveness factor
  val beta = 1.5 // influence of trail factor
  val evaporation = 0.5
  val antFactor = 0.8

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GraphX TSP Ant Colony Pregel").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val hdfsLocation = args(0)
    sc.setCheckpointDir(hdfsLocation + "/spark/checkpoint")

    val rand = new Random()

    val tspInstance = "st70" //args(1)
    val antDist = "random"

    val g: Graph[(List[Ant], Boolean), (Int, Double)] = //Graph[List[((List[Long], Int), (List[Long], Int))], (Int, Double)] =
      (if (tspInstance.equals("linear"))
        CityData.linearGraph(sc, hdfsLocation)
      else if (tspInstance == "german") {
        CityData.germanCityGraph(sc)
      } else if (tspInstance == "bavaria") {
        CityData.bays29(sc, hdfsLocation)
      } else if (tspInstance == "berlin") {
        CityData.berlin52(sc, hdfsLocation)
      } else if (tspInstance == "sahara") {
        CityData.sahara(sc, hdfsLocation)
      } else if (tspInstance == "st70") {
        CityData.st70(sc, hdfsLocation)
      } else if (tspInstance == "big") {
        CityData.big(sc, hdfsLocation)
      } else {
        GraphGenerators
          .logNormalGraph(sc, 1000)
          .mapVertices((id, _) => id.toString())
          .mapEdges(e => (rand.nextInt(50) + 100, 1.0))
      })
        .mapVertices((id, ddd) =>
          (List[Ant](), false))
        .mapEdges(e => (e.attr._1, 1.0))
        .cache()

    val numCities = g.numVertices.toInt
    val numAnts = (numCities * antFactor).toInt
    //    var ants: RDD[(VertexId, List[((List[Long], Int), (List[Long], Int))])] =
    //      sc.range(0, numAnts).map(t => {
    //        val randCity = rand.nextInt(numCities).toLong
    //        (randCity, List(((List[Long](randCity), 0), (List[Long](), Integer.MAX_VALUE))))
    //      }).reduceByKey((a1, a2) => a1.union(a2)) //.cache()

    var ants: RDD[(VertexId, List[Ant])] =
      sc.range(0, numAnts).map(t => {
        val randCity = rand.nextInt(numCities).toLong
        var ant = new Ant(t)
        ant.currentCity = randCity
        ant.tour = List(randCity)
        (randCity, List(ant))
      }).reduceByKey((a1, a2) => a1.union(a2)) //.cache()

    var workGraph = g.joinVertices(ants)((id, hh, dd) => (dd, false)).cache()

    //println(workGraph.vertices.collect().mkString("\n"))

    val numIterations = 5
    
    var prevGraph = workGraph

    // message : Tuple of List<Ant>, (Long, Double), Boolean 
    // where List<Ant> is the list of ants
    // (long, double) is id of ant and tau^beta * eta^alpha
    // boolean is mode of iteration 
    //   false => calculate probs
    //   true => send ants     
    for (i <- 1 to numIterations) {
      workGraph = workGraph.pregel[(List[Ant], (Long, Double), Boolean)]((List[Ant](), (-1, 0.0), false), numCities)(
        (vertexId, vertexAnts, receivedMessage) => {
          val sendAnts = receivedMessage._3
          if (sendAnts)
            (receivedMessage._1, false)
          else // calculate prob for each ant
            vertexAnts
        },
        triplet => Iterator((triplet.dstId, (triplet.srcAttr._1, (-1, 0.0), false)), (triplet.srcId, (triplet.srcAttr._1, (-1, 0.0), false))),
        (msg1, msg2) => msg1).cache()

      workGraph.vertices.count()
      workGraph.edges.count()
      
      workGraph.checkpoint()

    }

    println(workGraph.vertices.collect().mkString("\n"))
    //g.edges.collect().sortBy(e => e.attr._2).foreach(v => println(v))
    // ants.collect().sortBy(a => a._2._4._2).foreach(t => println(t))

    //val dd = readLine()
  }
}