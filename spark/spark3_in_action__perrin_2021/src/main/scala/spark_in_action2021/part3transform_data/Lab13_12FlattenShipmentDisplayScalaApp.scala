package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author rambabu.posa
 */
object Lab13_12FlattenShipmentDisplayScalaApp {

  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Flatenning JSON doc describing shipments")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    // Dataset[Row] == DataFrame
    val df: DataFrame = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/chapter13/json/shipment.json")

    val df2 = df
      .withColumn("supplier_name", F.col("supplier.name"))
      .withColumn("supplier_city", F.col("supplier.city"))
      .withColumn("supplier_state", F.col("supplier.state"))
      .withColumn("supplier_country", F.col("supplier.country"))
      .drop("supplier")
      .withColumn("customer_name", F.col("customer.name"))
      .withColumn("customer_city", F.col("customer.city"))
      .withColumn("customer_state", F.col("customer.state"))
      .withColumn("customer_country", F.col("customer.country"))
      .drop("customer")
      .withColumn("items", F.explode(F.col("books")))

    val df3 = df2
      .withColumn("qty", F.col("items.qty"))
      .withColumn("title", F.col("items.title"))
      .drop("items")
      .drop("books")

    // Shows at most 5 rows from the dataframe (there's only one anyway)
    df3.show(5, false)
    df3.printSchema()

    df3.createOrReplaceTempView("shipment_detail")

    val sqlQuery = "SELECT COUNT(*) AS bookCount FROM shipment_detail"
    val bookCountDf = spark.sql(sqlQuery)

    bookCountDf.show

    spark.stop
  }
/*
+----------+----------+--------------------+--------------+--------------+----------------+-------------------+-------------+--------------+----------------+---+----------------------------+
|date      |shipmentId|supplier_name       |supplier_city |supplier_state|supplier_country|customer_name      |customer_city|customer_state|customer_country|qty|title                       |
+----------+----------+--------------------+--------------+--------------+----------------+-------------------+-------------+--------------+----------------+---+----------------------------+
|2019-10-05|458922    |Manning Publications|Shelter Island|New York      |USA             |Jean Georges Perrin|Chapel Hill  |North Carolina|USA             |2  |Spark with Java             |
|2019-10-05|458922    |Manning Publications|Shelter Island|New York      |USA             |Jean Georges Perrin|Chapel Hill  |North Carolina|USA             |25 |Spark in Action, 2nd Edition|
|2019-10-05|458922    |Manning Publications|Shelter Island|New York      |USA             |Jean Georges Perrin|Chapel Hill  |North Carolina|USA             |1  |Spark in Action, 1st Edition|
+----------+----------+--------------------+--------------+--------------+----------------+-------------------+-------------+--------------+----------------+---+----------------------------+

root
 |-- date: string (nullable = true)
 |-- shipmentId: long (nullable = true)
 |-- supplier_name: string (nullable = true)
 |-- supplier_city: string (nullable = true)
 |-- supplier_state: string (nullable = true)
 |-- supplier_country: string (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- customer_city: string (nullable = true)
 |-- customer_state: string (nullable = true)
 |-- customer_country: string (nullable = true)
 |-- qty: long (nullable = true)
 |-- title: string (nullable = true)

+---------+
|bookCount|
+---------+
|        3|
+---------+
 */
}
