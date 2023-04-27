import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, lit, when,concat}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source


object RDDwithTransform {

  def main(args: Array[String]): Unit = {

    // Load the configuration file
    implicit val formats: DefaultFormats.type = DefaultFormats
    val config: Map[String, String] = parse(Source.fromFile("F:/Files/tasks/emptask/trans_emp_config.json").mkString).extract[Map[String, String]]

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV to MySQL")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("fname", StringType),
        StructField("lname", StringType),
        StructField("doj", StringType),
        StructField("salary", IntegerType)
      )
    )

    // Read the CSV file
    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(config("source_file"))
      .withColumnRenamed("id", "eid")
      .withColumnRenamed("salary","sal")
      .withColumn("fname", concat(col("fname"), lit(" "), col("lname")))
      .withColumn("doj", to_date(col("doj"), "dd/MM/yyyy"))
      .withColumn("exp", datediff(current_date(), col("doj")) / 365) // calculate the experience in years
      .drop("doj")
      .withColumn("nsal", when(col("exp") > 10, col("sal") * 1.10)
        .when(col("exp") > 5, col("sal") * 1.05)
        .otherwise(col("sal")))
       // apply the transformation


    val finalDF = df.drop( "lname")

    // Insert the data into the MySQL table
    finalDF.write.format("jdbc")
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


