import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{when,col}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source


object Rdd2 {

  def main(args: Array[String]): Unit = {

  // Load the configuration file
  implicit val formats: DefaultFormats.type = DefaultFormats
  val config: Map[String, String] = parse(Source.fromFile("F:/Files/tasks/config1.json").mkString).extract[Map[String, String]]

  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("CSV to MySQL")
    .master("local[*]")
    .getOrCreate()

  // Read the CSV file
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(config("source_file"))
    .withColumnRenamed("id", "Sr")
    .withColumnRenamed("ename", "fname")
    .withColumnRenamed("age","eage")
    .withColumnRenamed("sal","esal")
    .withColumn("newsal", when(col("esal") > 20, col("esal") * 1.1).otherwise(col("esal"))) // apply the transformation



  // Insert the data into the MySQL table
  df.write.format("jdbc")
    .option("url", s"jdbc:mysql://${config("host")}:${config("port")}/${config("database")}")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", config("target_table"))
    .option("user", config("username"))
    .option("password", config("password"))
    .mode("append")
    .save()

  // Stop the SparkSession
    println("*****************************************************")
    println("Data inserted Successfully")
    println("******************************************************")


  spark.stop()
}}

