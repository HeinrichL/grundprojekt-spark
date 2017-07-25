object Test {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet

  var vector = Vector.fill(10)(0)                 //> vector  : scala.collection.immutable.Vector[Int] = Vector(0, 0, 0, 0, 0, 0, 0
                                                  //| , 0, 0, 0)
  
  for (i <- 0 to 4) {
    vector = vector.updated(i * 2, 5)
  }

	vector                                    //> res0: scala.collection.immutable.Vector[Int] = Vector(5, 0, 5, 0, 5, 0, 5, 0
                                                  //| , 5, 0)

	
	val transit = Vector(0,0,0,1,0,0,1,0,1,1) //> transit  : scala.collection.immutable.Vector[Int] = Vector(0, 0, 0, 1, 0, 0,
                                                  //|  1, 0, 1, 1)
	val phero = Vector(1321,3132,12321,5331,51,84,9841,6516,4684,651,4165)
                                                  //> phero  : scala.collection.immutable.Vector[Int] = Vector(1321, 3132, 12321, 
                                                  //| 5331, 51, 84, 9841, 6516, 4684, 651, 4165)
	
	transit.zip(phero).map(t => t._1 * t._2).reduce((a,b) => a+b)
                                                  //> res1: Int = 20507
}