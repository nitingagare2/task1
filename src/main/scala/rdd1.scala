import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
object rdd1 {





    def main(args: Array[String]): Unit = {

      val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("SparkByExamples.com")
        .getOrCreate()

      //returns DataFrame
      val df: DataFrame = spark.read.text("F:/Files/tasks/emptask/emp_data.csv")
      df.printSchema()
      df.show(false)

      //converting to columns by splitting
//      import spark.implicits._
//      val df2 = df.map(f => {
//        val elements = f.getString(0).split(",")
//        (elements(0), elements(1),elements(2),elements(3),elements(4))
//      })
//
//      df2.printSchema()
//      df2.show(false)
//
//      // returns Dataset[String]
//      val ds: Dataset[String] = spark.read.textFile("F:/Files/tasks/emptask/emp_data.csv")
//      ds.printSchema()
//      ds.show(false)
//
//      //converting to columns by splitting
//      import spark.implicits._
//      val ds2 = ds.map(f => {
//        val elements = f.split(",")
//        (elements(0), elements(1),elements(2),elements(3),elements(4))
//      })
//
//      ds2.printSchema()
//      ds2.show(false)
//    }


}}
