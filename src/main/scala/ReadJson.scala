import org.json4s._
import org.json4s.jackson.JsonMethods._

object ReadJson {

  def main(args: Array[String]):Unit={

    val ipFile="F:/Files/emp1.json"
    val opFile="F:/Files/emp11.csv"
    implicit val formats = DefaultFormats

    val json = """{"name":"John", "age":30, "city":"New York"}"""

    val parsedJson = parse(json)

    val name = (parsedJson \ "name").extract[String]
    val age = (parsedJson \ "age").extract[Int]
    val city = (parsedJson \ "city").extract[String]

    println(name) // prints "John"
    println(age) // prints 30
    println(city) // prints "New York"

  }

}
