package spark_in_action2021.part3transform_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, col, sum, when}

/**
 * Orders analytics.
 *
 * @author rambabu.posa
 */
object Lab15_31PointsPerOrderScalaApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Orders loyalty point")
      .master("local[*]")
      .getOrCreate

    val pointsUdf = new Lab15_31PointAttributionScalaUdaf
    spark.udf.register("pointAttribution", pointsUdf)

    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("data/chapter15/orders/orders.csv")

    // Calculating the points for each customer, not each order
    val pointDf = df
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), callUDF("pointAttribution", col("quantity")).as("point"))

    pointDf.show(20)

    // Alternate way: calculate order by order
    val max = Lab15_31PointAttributionUdaf.MAX_POINT_PER_ORDER
    val eachOrderDf = df
      .withColumn("point", when(col("quantity").$greater(max), max).otherwise(col("quantity")))
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("point").as("point"))

    eachOrderDf.show(20)

    spark.stop
  }
}
/*
+------------+--------+-----+-------------+-----+
|   firstName|lastName|state|sum(quantity)|point|
+------------+--------+-----+-------------+-----+
|       Ginni| Rometty|   NY|            7|    3|
|Jean-Georges|  Perrin|   CA|            4|    3|
|      Holden|   Karau|   CA|           10|    6|
|Jean-Georges|  Perrin|   NC|            3|    3|
+------------+--------+-----+-------------+-----+

+------------+--------+-----+-------------+-----+
|   firstName|lastName|state|sum(quantity)|point|
+------------+--------+-----+-------------+-----+
|       Ginni| Rometty|   NY|            7|    3|
|Jean-Georges|  Perrin|   CA|            4|    3|
|      Holden|   Karau|   CA|           10|    6|
|Jean-Georges|  Perrin|   NC|            3|    3|
+------------+--------+-----+-------------+-----+
 */