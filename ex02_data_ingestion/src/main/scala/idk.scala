
import org.apache.spark.sql.SparkSession
import java.net.{URL, HttpURLConnection}
import java.nio.file.Files
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.InputStream
import org.apache.spark.sql.functions._
//mb put a main (cleaner)
object idk extends App {
  val spark = SparkSession.builder()
    .appName("DownloadToMinio")
    .master("local")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/") // Change at deployement
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  //For the test i use only one parquet
  val inputPath = "s3a://nyc-raw/yellow_tripdata_2025-11.parquet"

  // load data from mionio
  // Problem some parquet don't use the same type for a column so gemini use this (it doesnt work !!!)
  /*
  val rawDataDF = spark.read
    .option("mergeSchema", "true")
    .parquet("s3a://nyc-raw/*.parquet")
  rawDataDF.describe("passenger_count", "trip_distance", "fare_amount").show()
  */
    */

  val rawDataDF = spark.read.parquet(inputPath)
  // Simple filter to start mb add constraints
  val cleanRawDataDF = rawDataDF.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime") &&
    col("passenger_count") > 0 &&
    col("trip_distance") > 0.0 &&
    col("fare_amount") > 0.0 &&
    col("total_amount") > 0.0)




  rawDataDF.printSchema()

  println(s"before clean ${rawDataDF.count}")
  println(s"after clean ${cleanRawDataDF.count}")
  //println(s"cc clean ${cleanRawDataDF.head(1)}")
  val test = cleanRawDataDF.drop("all")
  println(s"test ${test.show()}")
  println(s"test ${test.select("total_amount","trip_distance","passenger_count").show}")

  val outputPath = "s3a://nyc-raw-branch-1/data"


  //keep overwrute in dev but not in prod
  cleanRawDataDF.write.mode("overwrite").parquet(outputPath)

}