package ant_colony

class Ant(var current: Long) extends Serializable{
  
  var currentCity = current
  var lastCity = -1
  var tour = Array()

  override def toString(): String = {
    currentCity.toString()
  }
  
}