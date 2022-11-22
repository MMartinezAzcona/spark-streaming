package part3lowlevel

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Date
import java.text.SimpleDateFormat
import java.io.{File, FileWriter}

object DStreams {

  val spark = SparkSession
    .builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*Entry point to DStreams API*/
  val ssc = new StreamingContext(sparkContext = spark.sparkContext, batchDuration = Seconds(1))

  /* Define input sources (DStreams)*/
  /* Define transformations on DStreams*/
  /* Start computation */
  /* Await termination*/
  def readFromSocket(): Unit = {

    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    wordsStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile():Unit = {
    new Thread(() => {
      Thread.sleep(5000)
      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")

      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,May 1 2000,21
          |AAPL,Jun 1 2000,26.19
          |AAPL,Jul 1 2000,25.41
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
        """.stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile(): Unit = {

    createNewFile()

    val stocksFile = "src/main/resources/data/stocks"
    val textStream = ssc.textFileStream(stocksFile)

    val dateFormat = new SimpleDateFormat("MMM d yyyy")
    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    stocksStream.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    readFromFile()
  }

}
