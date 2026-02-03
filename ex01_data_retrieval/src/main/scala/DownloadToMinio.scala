
import org.apache.spark.sql.SparkSession
import java.net.{URL, HttpURLConnection}
import java.nio.file.Files
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.InputStream



object DownloadToMinio extends App {

  val spark = SparkSession.builder()
    .appName("DownloadToMinio")
    .master("local")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/") // A changer lors du déploiement
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  val hadoopConf = spark.sparkContext.hadoopConfiguration
  hadoopConf.set("fs.s3a.access.key", "minio")
  hadoopConf.set("fs.s3a.secret.key", "minio123")
  hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000")
  hadoopConf.set("fs.s3a.path.style.access", "true")
  hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  hadoopConf.set("fs.s3a.connection.ssl.enable", "false")

  def getParquetFileName(year: Int)(month: Int): String = {
    "yellow_tripdata_" + year + "-" + String.format("%02d", month) + ".parquet"
  }

  val urlPrefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
  val tempDirPath = "..data/external/"
  val minioDirPath = "s3a://nyc-raw/"

  val years = (2022 to 2025).toList
  val months = (1 to 12).toList

  for (y <- years) {
    for (m <- months) {
      // get paths
      val fileName = getParquetFileName(y)(m)
      val url = urlPrefix + fileName  //"../data/raw/yellow_tripdata_2025-01.parquet"
      val targetPath = new Path(minioDirPath + fileName)
      val fs = targetPath.getFileSystem(hadoopConf)
      // download
      try {
        // solution from: https://stackoverflow.com/questions/11302727/server-returning-403-for-url-openstream, useless??
        val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("GET")
        conn.setRequestProperty("User-Agent", "Mozilla/5.0")
        conn.setConnectTimeout(10_000)
        conn.setReadTimeout(60_000)

        val in: InputStream = conn.getInputStream()
        val out = fs.create(targetPath, true)

        val buffer = new Array[Byte](8 * 1024)
        var bytesRead = in.read(buffer)
        while (bytesRead >= 0) {
          out.write(buffer, 0, bytesRead)
          bytesRead = in.read(buffer)
        }

        in.close()
        out.close()
        //val bytes = new URL(url).openStream().readAllBytes()
        //val tmp = Files.createTempFile("tmp_nyc_data", ".parquet")
        //Files.write(tmp, bytes)
        //// read
        //val df = spark.read.parquet(tmp.toString())
        //// write
        //df.write.mode("overwrite").parquet(targetPath)
        //// clean
        //Files.delete(tmp)
        println("uploaded " + fileName + " to minio")
      } catch {
        case e: Exception => 
          println("could not download or write file: " + fileName)
          e.printStackTrace()

      }
    }
  }
  spark.stop()
}
