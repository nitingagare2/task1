import scala.io.Source
import java.io.File
import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.util.{Failure, Success, Try}
object SSv {

  def main(args: Array[String]): Unit = {

    val confFilePath="F:/Files/tasks/source_to_target_config.txt"

    val columnMapping=Source.fromFile(confFilePath).getLines()
      .map(line => line.split(","))
      .map(cols => (cols(0), cols(1)))
      .toMap



    // for(i<-configFile)
    // {
    //     println(i)
    // }

    val csvFilePath="F:/Files/tasks/empex.csv"
    //val csvFile=Source.fromFile(csvFilePath).getLines()
    val csvfile1=new File(csvFilePath)



    val url = "jdbc:mysql://localhost:3306/emp"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "root"


    var connection:Connection=null

    try {

      Class.forName(driver)

      connection=DriverManager.getConnection(url,username,password)

      val statement=connection.createStatement()

      val csvLines=Source.fromFile(csvfile1).getLines()

      for(i<-csvLines)
      {
//        val cols = i.split(",").map(_.trim)
//        val Sr = cols(columnMapping("Sr"))
//        val fname = cols(columnMapping("fname"))
//        val eage= cols(columnMapping("eage"))
//        val esal=cols(columnMapping("esal"))
//
//        val sql = "INSERT INTO emp1 (Sr, fname, eage,esal) VALUES (?, ?, ?,?)"
//        val pstmt = connection.prepareStatement(sql)
//        pstmt.setInt(1, Sr)
//        pstmt.setString(2, fname)
//        pstmt.setInt(3, eage)
//        pstmt.setDouble(4,esal)
//        pstmt.executeUpdate()

      }


    }

    catch {
      case e: Exception => e.printStackTrace
    }

    // for(i<-csvFile)
    // {
    //     println(i)
    // }
  }

}

