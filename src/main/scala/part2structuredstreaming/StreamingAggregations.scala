package part2structuredstreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder().appName("StreagmAgg").master("local[2]").getOrCreate()

  def streamingCount()= {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localHost")
      .option("port", 12345)
      .load()

    val lineCount = lines.selectExpr("count(*) as lineCount")

    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def numericalAggregation(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localHost")
      .option("port", 12345)
      .load()

    val numbers = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localHost")
      .option("port", 12345)
      .load()

    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    groupNames()

  }


}
