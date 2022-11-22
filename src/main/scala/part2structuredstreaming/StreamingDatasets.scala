package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._


object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def readCars(): Dataset[Car] = {

    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")
      .as[Car]
    //When converting a DF to Dataset we need an encoder
    //An encoder is a data structure that tells Spark how a row should be converted to a jvm object
    //We can either pass a encoder explicitly or let the compiler fetch an encoder implicitly

  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformation
    val carNamesDF: DataFrame = carsDS.select(col("Name")) //DF

    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  /**
    * Exercises
    * 1) Count how many POWERFUL cares we have in the DS (HP > 140)
    * 2) Average HP for the entire dataset (use complete output mode)
    * 3) Count cars by Origin Field
    */

  def count_powerful(): Unit = {
    val carsDS: Dataset[Car] = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) >140).select(count(col("Name")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def average_hp(): Unit = {
    val carsDS: Dataset[Car] = readCars()

    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def count_by_origin(): Unit = {
    val carsDS: Dataset[Car] = readCars()
    carsDS.groupByKey(car => car.Origin).count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    count_by_origin()
  }

}
