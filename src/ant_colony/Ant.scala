package ant_colony

class Ant(var id: Long) extends Serializable{
  
  var currentCity = -1L
  
  var nextCity = -1L
  
  var tour = List[Long]()
  var tourlength = 0
  
  var bestTour = List[Long]()
  var bestLength = Integer.MAX_VALUE

  override def toString(): String = {
    id.toString()
  }
  
}