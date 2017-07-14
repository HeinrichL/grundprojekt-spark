import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.collection.Map.WithDefault

object GermanCitiesAnt {
  val pheromon = 500
  val alpha = 1
  val beta = 5
  val verdunstung = 0.5
  val ameisenFaktor = 0.8

  val randomCityProp = 0.01

  val numIterations = 2000

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "GraphX German Cities", "/usr/local/spark")

    // Create an RDD for the vertices
    val cities: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L, "hamburg"), (1L, "berlin"),
        (2L, "bremen"), (3L, "erfurt"),
        (4L, "köln"), (5L, "stuttgart"),
        (6L, "münchen")))

    // Create an RDD for edges (distance, pheromone)
    val distances: RDD[Edge[(Int, Double)]] =
      sc.parallelize(Array(Edge(0L, 1L, (300, 0.0)), Edge(0L, 2L, (200, 0.0)),
        Edge(1L, 3L, (400, 0.0)),
        Edge(2L, 4L, (400, 0.0)), Edge(2L, 3L, (400, 0.0)),
        Edge(3L, 4L, (500, 0.0)), Edge(3L, 5L, (500, 0.0)), Edge(3L, 6L, (500, 0.0)),
        Edge(4L, 5L, (400, 0.0)),
        Edge(5L, 6L, (300, 0.0))))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("EmptyCity")

    // Build the initial Graph
    val graph = Graph(cities, distances, defaultUser)

    // Ant Colony Algo
    val numCities = graph.vertices.collect().count(v => true)
    val numAnts = (numCities * ameisenFaktor).toInt

    println("numCities", numCities)
    println("numAnts", numAnts)

    val rand = new Random()

    // Ameisen auf Städte verteilen
    var antMap = scala.collection.mutable.HashMap[Long, Int]().withDefaultValue(0)
    for (i: Int <- 1 to numAnts) {
      val iCity = rand.nextInt(numCities.toInt).toLong

      if (antMap.contains(iCity)) {
        antMap(iCity) += 1
      } else {
        antMap += (iCity -> 1)
      }
    }

    println(antMap.mkString("\n"))

    if (antMap.foldLeft(0)(_ + _._2) != numAnts) {
      throw new RuntimeException("Illegal placement of ants. Number of available ants and number of placed ants are different.")
    }

    val withAnts = graph.mapVertices((id, t) => (t, antMap(id)))

    withAnts.vertices.collect().foreach(v => println(v))

    val res = withAnts.pregel(0, 1)(
      (id, vVal, rec) => (vVal._1, vVal._2 + rec),
      t => {
        if (t.srcAttr._2 > 0) {
          Iterator((t.dstId, 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a + b)

      
    res.vertices.collect().foreach(v => println(v))

    //    withAnts.vertices.collect().foreach(v => println(v))
    //    withAnts.edges.collect().foreach(v => println(v))
    //
    //    val upd = withAnts.mapTriplets(t => {
    //
    //      var src = t.srcAttr
    //      var dst = t.dstAttr
    //      val edgeVal = t.attr
    //
    //      if (src._2 > 0) {
    //        ((t.srcAttr._1, t.srcAttr._2 - 1), (t.dstAttr._1, t.dstAttr._2 + 1), (t.attr, 200.0))
    //      } else {
    //        t
    //      }
    //
    //    })
    //    upd.vertices.collect().foreach(v => println(v))
    //    upd.edges.collect().foreach(v => println(v))
    //
    //    for (i <- 0 to numIterations) {
    //      // move Ants
    //      //withAnts.
    //    }
  }
}