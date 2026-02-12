
import org.apache.spark.sql.SparkSession
import java.net.{URL, HttpURLConnection}
import java.nio.file.Files
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.InputStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import java.util.Properties

object Branch2 extends App {
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

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val inputPath = "s3a://nyc-raw-branch-1/data"
  val dataDF = spark.read.parquet(inputPath)

  val jdbcUrl = "jdbc:postgresql://localhost:5432/nyc_warehouse"
  val connectionProperties = new Properties()
  connectionProperties.setProperty("user", "postgres")
  connectionProperties.setProperty("password", "postgres")
  connectionProperties.setProperty("driver", "org.postgresql.Driver")

  println(s"Nombre de lignes lues dans Minio : ${dataDF.count()}")
  dataDF.show(10)
  val cleanedDF = dataDF.select(
    col("VendorID").cast("int"),
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("RatecodeID").cast("int"),
    col("store_and_fwd_flag"),
    col("PULocationID").cast("int"),
    col("DOLocationID").cast("int"),
    col("payment_type").cast("int"),
    col("fare_amount").cast("double"),
    col("extra").cast("double"),
    col("mta_tax").cast("double"),
    col("tip_amount").cast("double"),
    col("tolls_amount").cast("double"),
    col("improvement_surcharge").cast("double"),
    col("total_amount").cast("double"),
    col("congestion_surcharge").cast("double"),
    col("Airport_fee").cast("double") // Correction de la casse
  ).distinct()



  // Extraction de Dim_Time à partir des deux colonnes de timestamp
  val pickupTimes = cleanedDF.select(col("tpep_pickup_datetime").alias("full_datetime"))
  val dropoffTimes = cleanedDF.select(col("tpep_dropoff_datetime").alias("full_datetime"))

  val dimTimeDF = pickupTimes.union(dropoffTimes).distinct()
    .withColumn("hour", hour($"full_datetime"))
    .withColumn("day", dayofmonth($"full_datetime"))
    .withColumn("month", month($"full_datetime"))
    .withColumn("year", year($"full_datetime"))
    .withColumn("quarter", quarter($"full_datetime"))

  try {
    println("Début de l'ingestion dans PostgreSQL...")
    //println(s"Lignes à insérer dans Dim_Time : ${dimTimeDF.count()}")
    //println(s"Lignes à insérer dans Fact_Trips : ${cleanedDF.count()}")




    dimTimeDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "dim_time", connectionProperties)
    println("- Table Dim_Time mise à jour.")



    cleanedDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "fact_trips", connectionProperties)
    println("- Table Fact_Trips mise à jour.")

    println("Succès : Ingestion Branche 2 terminée.")

  } catch {
    case e: Exception =>
      println(s"ERREUR lors de l'ingestion : ${e.getMessage}")
      e.printStackTrace()
  } finally {
    spark.stop()
  }



}