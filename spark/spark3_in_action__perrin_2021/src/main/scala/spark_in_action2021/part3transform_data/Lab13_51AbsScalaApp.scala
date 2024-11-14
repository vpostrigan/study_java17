package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions => F}

/**
 * abs function.
 *
 * @author rambabu.posa
 */
object Lab13_51AbsScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("abs function")
      .master("local[*]")
      .getOrCreate

    var df: Dataset[Row] = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/functions/functions.csv")

    df = df.withColumn("abs", F.abs(F.col("val")))

    df.show(5)

    spark.stop
  }

}
/*
+---+-----+----+
|key|  val| abs|
+---+-----+----+
|  A|   -5| 5.0|
|  B|    4| 4.0|
|  C| 3.99|3.99|
|  D|-8.75|8.75|
+---+-----+----+
 */