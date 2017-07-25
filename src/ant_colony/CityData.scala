package ant_colony

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.Iterator

object CityData {
  def germanCityGraph(sc: SparkContext): Graph[String, (Int, Double)] = {
    // Create an RDD for the vertices
    val cities: RDD[(VertexId, String)] =
      sc.parallelize(Array((0L, "hamburg"), (1L, "berlin"),
        (2L, "bremen"), (3L, "erfurt"),
        (4L, "köln"), (5L, "stuttgart"),
        (6L, "münchen")))

    // Create an RDD for edges (distance, pheromone)
    val distances: RDD[Edge[(Int, Double)]] =
      sc.parallelize(Array(Edge(0L, 1L, (300, 1.0)), Edge(0L, 2L, (200, 1.0)),
        Edge(1L, 3L, (400, 1.0)),
        Edge(2L, 4L, (400, 1.0)), Edge(2L, 3L, (400, 1.0)),
        Edge(3L, 4L, (500, 1.0)), Edge(3L, 5L, (500, 1.0)), Edge(3L, 6L, (500, 1.0)),
        Edge(4L, 5L, (400, 1.0)),
        Edge(5L, 6L, (300, 1.0))))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("EmptyCity")

    // Build the initial Graph
    Graph(cities, distances, defaultUser)
  }
  
  def bays29(sc: SparkContext) : Graph[String, (Int, Double)] = {
    val input = sc.textFile("input/complete_bays29.tsp")
    val edges = input.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, (edge(2).toInt, 0.0))
      }
    return Graph.fromEdges(edges, "Empty")
  }
  
  def berlin52(sc: SparkContext) : Graph[String, (Int, Double)] = {
    val input = sc.textFile("input/complete_berlin52.tsp")
    val edges = input.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, (edge(2).toInt, 0.0))
      }
    return Graph.fromEdges(edges, "Empty")
  }
  
  def att48(sc: SparkContext) : Graph[String, (Int, Double)] = {
    val input = sc.textFile("input/complete_att48.tsp")
    val edges = input.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, (edge(2).toInt, 0.0))
      }
    return Graph.fromEdges(edges, "Empty")
  }
  
  def sahara(sc: SparkContext) : Graph[String, (Int, Double)] = {
    val input = sc.textFile("input/complete_sahara.tsp")
    val edges = input.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, (edge(2).toInt, 0.0))
      }
    return Graph.fromEdges(edges, "Empty")
  }
  
  def st70(sc: SparkContext) : Graph[String, (Int, Double)] = {
    val input = sc.textFile("input/complete_st70.tsp")
    val edges = input.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, (edge(2).toInt, 0.0))
      }
    return Graph.fromEdges(edges, "Empty")
  }
  
  def linearGraph(sc: SparkContext) : Graph[String, (Int, Double)] = {
    val input = sc.textFile("input/linear.txt") 
    val edges = input.map
      {
        line => 
          val edge = line.split(",")
          Edge(edge(0).toLong, edge(1).toLong, (edge(2).toInt, 0.0))
      }
    return Graph.fromEdges(edges, "Empty")
  }
}