package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

object StreamingDataFrame {

  val spark = SparkSession.builder()
    .appName("First Stream")
    .config("spark.master", "local[2]")
    .getOrCreate()


  def readFromSocket() = {
    //Reading DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localHost")
      .option("port", 12345)
      .load()

    //Transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    println(shortLines.isStreaming)
    // Consuming DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination(15000)
  }

  def readFromFiles()= {
    val stockDF = spark.readStream
      .format("csv")
      .option("header", false)
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stockDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localHost")
      .option("port", 12345)
      .load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) //run the query every 2 seconds
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    demoTriggers()
  }

}
