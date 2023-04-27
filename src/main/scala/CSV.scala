import java.io.File
import java.io.PrintWriter

import org.json4s._
import scala.util.parsing.json.JSONType

//import play.api.libs.json._
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.stringLiteralSequence_return
//import net.liftweb.json._
//import net.liftweb.json.JsonDSL._
//import org.json4s.native.JsonMethods._

object CSV {

  case class Person(name: String, age: Int, email: String, street: String, city: String, state: String, zip: String, hobbies: List[Hobby])
  case class Hobby(name: String, level: String)

  def main(args: Array[String]): Unit = {
    val inputFile = new File("F:/Files/input.json")
    val outputFile = new File("F:/Files/emp11.csv")

    val jsonString = scala.io.Source.fromFile(inputFile).mkString
   // val json = Json.parse(jsonString)

    // val people = (json \ "people").extract[List[String]]
   // val people = (json \ "people").extract[List[Person]]


   // val csvString = convertToCsv(people)

    val writer = new PrintWriter(outputFile)
    //writer.write(csvString)
    writer.close()

    println("Conversion completed successfully!")
  }

  def convertToCsv(people: List[Person]): String = {
    val header = "Name,Age,Email,Street,City,State,Zip,Hobbies\n"
    val rows = people.map(p => {
      val hobbies = p.hobbies.map(h => s"${h.name} (${h.level})").mkString("; ")
      s"${p.name},${p.age},${p.email},${p.street},${p.city},${p.state},${p.zip},${hobbies}"
    }).mkString("\n")

    header + rows
  }

}
