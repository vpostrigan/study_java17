package spark_in_action2021.part3transform_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum}

/**
 * Orders analytics.
 *
 * @author rambabu.posa
 */
object Lab15_11OrderStatisticsScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Orders analytics")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("data/chapter15/orders/orders.csv")

    // Calculating the orders info using the dataframe API
    val apiDf = df
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("revenue"), avg("revenue"))

    apiDf.show(20)

    // Calculating the orders info using SparkSQL
    df.createOrReplaceTempView("orders")

    val sqlQuery = "SELECT firstName,lastName,state,SUM(quantity),SUM(revenue),AVG(revenue) " +
      "FROM orders " +
      "GROUP BY firstName, lastName, state"

    val sqlDf = spark.sql(sqlQuery)
    sqlDf.show(20)

    spark.stop
  }

}

/*
+------------+--------+-----+-------------+------------+------------+
|   firstName|lastName|state|sum(quantity)|sum(revenue)|avg(revenue)|
+------------+--------+-----+-------------+------------+------------+
|       Ginni| Rometty|   NY|            7|          91|        91.0|
|Jean-Georges|  Perrin|   CA|            4|          75|        75.0|
|      Holden|   Karau|   CA|           10|         190|        95.0|
|Jean-Georges|  Perrin|   NC|            3|         420|       210.0|
+------------+--------+-----+-------------+------------+------------+

+------------+--------+-----+-------------+------------+------------+
|   firstName|lastName|state|sum(quantity)|sum(revenue)|avg(revenue)|
+------------+--------+-----+-------------+------------+------------+
|       Ginni| Rometty|   NY|            7|          91|        91.0|
|Jean-Georges|  Perrin|   CA|            4|          75|        75.0|
|      Holden|   Karau|   CA|           10|         190|        95.0|
|Jean-Georges|  Perrin|   NC|            3|         420|       210.0|
+------------+--------+-----+-------------+------------+------------+
 */