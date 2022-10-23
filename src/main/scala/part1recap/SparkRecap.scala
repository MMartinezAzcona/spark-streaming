package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkRecap{

  // entry Point to the Spark Structured API
  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars/cars.json")

  // select
  val wight_df = cars.select(
    col("Name"),
    (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"),
    expr("Weight_in_lbs/2.2*1000").alias("Weight_in_grams")
  )
  val carsWeights = cars.selectExpr("Weight_in_lbs/2.2")

  // filter
  val europeanCars = cars.where(col("Origin") === "Europe")

  // aggregations
  val averageHP_country = cars.select("Origin", "Horsepower").
    groupBy("Origin").avg("Horsepower").alias("Abg HP")

  // joins
  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristBands = guitarPlayers.join(bands,
    guitarPlayers.col("band") === bands.col("id"),
    joinType = "right")

  //datasets = typed distributed collection of objects
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  import spark.implicits._

  val guitarPlayersDS = guitarPlayers.as[GuitarPlayer]


  // spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars = spark.sql(
    """
      |select Name from cars where Origin= 'USA'
      |""".stripMargin)

  // low-level API: RDDs
  val sc = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 100000)

  // functional operators
  val doubles = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("number")

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD)

  def main(args: Array[String]): Unit = {
    print(numbersDS.show(50))
  }

}
