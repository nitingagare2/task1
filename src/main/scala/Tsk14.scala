
import java.io.FileReader
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.JavaConverters._
import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.opencsv.CSVReader
import com.opencsv.CSVParser
import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder


object Tsk14 {

  def main(args: Array[String]): Unit = {



    // Load the configuration file
    implicit val formats: DefaultFormats.type = DefaultFormats
    val config: Map[String, String] = parse(Source.fromFile("F:/Files/tasks/config1.json").mkString).extract[Map[String, String]]

    // Connect to the MySQL database
    Class.forName("com.mysql.cj.jdbc.Driver")
    val cnx: Connection = DriverManager.getConnection(
      s"jdbc:mysql://${config("host")}:${config("port")}/${config("database")}",
      config("username"),
      config("password")
    )

    // Create a prepared statement
    val insertQuery: String = s"INSERT INTO ${config("target_table")} (Sr,fname,eage,esal) VALUES (?, ?,?,?)"
    val pstmt: PreparedStatement = cnx.prepareStatement(insertQuery)

    // Read the CSV file
    val reader = new FileReader(config("source_file"))
    val data: List[Array[String]] = new com.opencsv.CSVReader(reader).readAll().asScala.toList.tail
    data.foreach { row =>
      val Sr = row.head.toInt
      val fname=row(1).toString
      val eage=row(2).toInt
      val esal = row.last.toDouble

      // Insert the data into the MySQL table
      pstmt.setInt(1, Sr)
      pstmt.setString(2,fname)
      pstmt.setInt(3,eage)
      pstmt.setDouble(4, esal)
      pstmt.executeUpdate()
    }

    println("data inserted successfully")

    // Close the prepared statement and the database connection
    pstmt.close()
    cnx.close()

  }

}

