import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._  

object SparkWordCount { 
   def main(args: Array[String]) { 

      val conf = new SparkConf()//.setAppName("GraphX Pregel Example")//.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)
		
      /* local = master URL; Word Count = application name; */  
      /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */ 
      /* Map = variables to work nodes */ 
      /*creating an inputRDD to read text file (in.txt) through Spark context*/ 
      val input = sc.textFile("/home/heinrich/Schreibtisch/input.txt") 
      /* Transform the inputRDD into countRDD */ 
		
      val count = input.flatMap(line ⇒ line.split(" ")) 
      .map(word ⇒ (word, 1)) 
      //.reduceByKey(_ + _) 
       
      /* saveAsTextFile method is an action that effects on the RDD */  
      count.foreach(word => println(word._1 + " " + word._2))
      //count.saveAsTextFile("outfile") 
      System.out.println("OK"); 
   } 
} 