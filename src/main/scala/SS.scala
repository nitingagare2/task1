import org.apache.spark.sql.SparkSession
object SS extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate();
  println(spark)
  println("Spark Version : "+spark.version)
  println("program is working fine")
  println("program is progress*****************************************************")

  val s1=SparkSession.getActiveSession
  println(s1)

  ///reading data from text file
  val rddf=spark.sparkContext.textFile("F:/Files/tasks/STTC.txt")
  rddf.collect().foreach(f=>{
    println(f)
  })
}
