import scala.collection.mutable

/**
 *
 * @auther hage
 * @creat 2020-08-24 1:33
 */
object mutableHashMap {

  def main(args: Array[String]): Unit = {
    val map: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]

    map.+("aa1"->1L)
    map.+("aa2"->2L)
    map.+("aa3"->3L)
    for((k,v) <- map){
      println("11")
      println(k,v)
    }
  }

}
