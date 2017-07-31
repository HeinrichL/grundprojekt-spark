package ant_colony

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.Iterator
import org.apache.spark.graphx.util.GraphGenerators

object TSPAntColony {
  val Q = 500 // pheromon deposit constant
  val alpha = -1.5 // influence of attractiveness factor
  val beta = 1.5 // influence of trail factor
  val evaporation = 0.5
  val antFactor = 5

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GraphX TSP Ant Colony Optimization").setMaster("local[4]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("checkpoint/")

    val rand = new Random()

    val tspInstance = "st70"
    val antDist = "random"

    var g: Graph[String, (Int, Double, Vector[Int])] =
      if (tspInstance.equals("linear"))
        CityData.linearGraph(sc).mapEdges(e => (e.attr._1, 1.0, Vector.empty[Int]))
      else if (tspInstance == "german") {
        CityData.germanCityGraph(sc).mapEdges(e => (e.attr._1, 1.0, Vector.empty[Int]))
      } else if (tspInstance == "bavaria") {
        CityData.bays29(sc).mapEdges(e => (e.attr._1, 1.0, Vector.empty[Int]))
      } else if (tspInstance == "berlin") {
        CityData.berlin52(sc).mapEdges(e => (e.attr._1, 1.0, Vector.empty[Int]))
      } else if (tspInstance == "sahara") {
        CityData.sahara(sc).mapEdges(e => (e.attr._1, 1.0, Vector.empty[Int]))
      } else if (tspInstance == "st70") {
        CityData.st70(sc).mapEdges(e => (e.attr._1, 1.0, Vector.empty[Int]))
      } else {
        GraphGenerators
          .logNormalGraph(sc, 1000)
          .mapVertices((id, _) => id.toString())
          .mapEdges(e => (rand.nextInt(50) + 100, 1.0, Vector.empty[Int]))
      } //.cache()

    val numCities = g.numVertices.toInt
    val numAnts = (numCities * antFactor).toInt
    var ants: RDD[(VertexId, (Long, Long, (List[Long], Int), (List[Long], Int)))] =
      if (antDist == "fixed")
        sc.range(0, numCities).map(l =>
          (if (Seq(0).contains(l)) l else -1L,
            (-1L, l, (List[Long](l), 0), (List[Long](), 0))))
          .filter(f => f._1 > -1).cache()
      else
        sc.range(0, numAnts).map(t => {
          val randCity = rand.nextInt(numCities).toLong
          (randCity,
            (randCity, t, (List[Long](randCity), 0), (List[Long](), Integer.MAX_VALUE)))
        }) //.cache()

    g = g.mapEdges(e => (e.attr._1, e.attr._2, Vector.fill(numAnts)(0))) //.cache()

    val numIterations = 1
    for (i <- 1 to numIterations) {

      // let all ants make a tour trough all cities
      for (j <- 1 to numCities) {
        // collect all incident edges for each vertex
        val vIncidentEdges: VertexRDD[Array[Edge[(Int, Double, Vector[Int])]]] = g.collectEdges(EdgeDirection.Either)

        // calculate propability for each edge (except edges to previously visited cities)
        val vIncidentEdgesWithPropability = vIncidentEdges
          .join(ants)
          .map {
            case (idCurrentCity, (incidentEdges, (antStart, antId, (antVisited, antTourlength), bestTour))) =>
              (idCurrentCity, (incidentEdges, (antStart, antId, (antVisited, antTourlength), bestTour)),
                //e.filter(e => if (e.srcId == id) e.dstId != ant._1 else e.srcId != ant._1) // previously traversed edge is not considered
                incidentEdges.filter(edge =>
                  if (edge.srcId == idCurrentCity)
                    !antVisited.contains(edge.dstId.toLong) || (antVisited.length == numCities && edge.dstId == antStart)
                  else
                    !antVisited.contains(edge.srcId.toLong) || (antVisited.length == numCities && edge.srcId == antStart))
                .map(e => math.pow(e.attr._1, alpha) * Math.pow(e.attr._2, beta)) // calculate propability for every edge
                .reduce( // get the sum 
                  (a, b) => {
                    (a + b)
                  }))
          }
          .map { // calculate relative propability for edge to be traversed
            case (id, (edges, (antStart, antId, (antVisited, antTourlength), bestTour)), prop) => {
              (id, edges.map(e => {
                (e.srcId,
                  e.dstId,
                  (e.attr._1, e.attr._2,
                    // city visited -> do not visit again
                    // tour finished -> go back to start
                    if ((e.srcId == id && (antVisited.length < numCities && antVisited.contains(e.dstId.toLong) || antVisited.length == numCities && e.dstId != antStart)) ||
                      (e.dstId == id && (antVisited.length < numCities && antVisited.contains(e.srcId.toLong) || antVisited.length == numCities && e.srcId != antStart)))
                      0
                    else
                      math.pow(e.attr._1, alpha) * Math.pow(e.attr._2, beta) / prop),
                    e.attr._3)
              }), (antStart, antId, (antVisited, antTourlength), bestTour))
            }
          } //----------.cache()

        //vIncidentEdgesWithPropability.collect().foreach(a => println(a._1 + " " + a._2.mkString(",")))

        // select edge with highest propability
        val vSelectedEdge = vIncidentEdgesWithPropability
          .map {
            case (id, edges, ant) =>
              (id, (Seq((edges.maxBy(e => e._3._3), ant))))
          } //.cache()

        // reduce selected edges to vertex (for traversion to next edge)
        val vSelectedEdgeReducedToVertex = vSelectedEdge.map(t => (t._1, t._2)).reduceByKey((ant1, ant2) => ant1.union(ant2))

        // write selected edges to the graph
        val graphWithChoosenEdges = g.outerJoinVertices(vSelectedEdgeReducedToVertex)(
          (id, vVal, joined) => (vVal, joined)) //.cache()

        // traverse edges
        g = graphWithChoosenEdges.mapTriplets(triplet => {
          val srcEdges = triplet.srcAttr._2
          val dstEdges = triplet.dstAttr._2
          val edge = triplet.attr

          val allEdges =
            if (srcEdges.isDefined && dstEdges.isDefined)
              srcEdges.get.union(dstEdges.get)
            else if (srcEdges.isDefined && !dstEdges.isDefined)
              srcEdges.get
            else if (!srcEdges.isDefined && dstEdges.isDefined)
              dstEdges.get
            else
              Seq()

          // only ants that traversed the edge in this triplet
          val filteredEdges = allEdges.filter(e => (e._1._1 == triplet.srcId && e._1._2 == triplet.dstId || e._1._1 == triplet.dstId && e._1._2 == triplet.srcId))

          // write ants that traversed this edge to vector
          var newVector = edge._3

          for (e <- filteredEdges) {
            newVector = newVector.updated(e._2._2.toInt, 1)
          }

          (edge._1, edge._2, newVector)
        }).mapVertices((id, tuple) => tuple._1) //.cache()

        // update Ant-RDD
        // set current vertex to target and add current vertex to tour
        ants = vSelectedEdge.map(ggg => {
          val idFrom = ggg._1
          val (edgeSrc, edgeDst, (dist, pheromone, prop), vector) = ggg._2.head._1
          val (antFrom, antId, (currentTour, currentLength), (bestTour, bestLength)) = ggg._2.head._2

          val idTo = if (idFrom == edgeSrc) edgeDst else edgeSrc

          (idTo, (antFrom, antId, (idTo :: currentTour, currentLength + dist), (bestTour, bestLength)))
        }) //.cache()

        // truncate object DAG occasionally (else StackOverflow error will be thrown)
        if (j % 25 == 0) {

          g.checkpoint()

          // materialize vertices and edges
          g.vertices.count()
          g.edges.count()

          ants.checkpoint()

          ants.count()
        }

      }
      // all ants performed one tour

      // calculate amount of pheromone to deposit for every ant.
      // use formula deltaT = Q / L where Q is deposit constant and L is length of tour
      val antPheromones = Vector.empty ++ ants.sortBy(f => f._2._2).map(a => (Q.toDouble / a._2._3._2)).collect()

      // update pheromone value for each edge
      // tNew = tOld * evaporation + sum(deltaT from all ants that traversed this edge)
      g = g.mapEdges(e => {
        val resultPheromone = e.attr._3.zip(antPheromones).map(t => t._1 * t._2).reduce((a, b) => a + b)
        (e.attr._1, e.attr._2 * evaporation + resultPheromone, Vector.fill(e.attr._3.length)(0))
      }) //.cache()

      // reset ant and save best tour
      ants = ants.map(f => {
        val (curr, (antFrom, antId, (currentTour, currentLength), (bestTour, bestLength))) = f
        (curr, (antFrom, antId, (List(antFrom), 0), if (bestLength > currentLength) (currentTour, currentLength) else (bestTour, bestLength)))
      }) //.cache()

    }

    //g.edges.collect().sortBy(e => e.attr._2).foreach(v => println(v))
    ants.collect().sortBy(a => a._2._4._2).foreach(t => println(t))

    val dd = readLine()
  }
}