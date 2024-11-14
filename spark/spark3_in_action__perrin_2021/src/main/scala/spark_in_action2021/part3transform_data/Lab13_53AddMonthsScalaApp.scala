package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * add_months function
 *
 * @author rambabu.posa
 */
object Lab13_53AddMonthsScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("add_months function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("imferSchema", true)
      .load("data/chapter13/functions/dates.csv")

    val df2 = df
        .withColumn("add_months+2",
          F.add_months(F.col("date_time"), 2))
        .withColumn("add_months+val",
          F.add_months(F.col("date_time"), F.col("val")))

    df2.show(5, false)
    df2.printSchema()

    spark.stop
  }

}
/*
+-------------------------+---+------------+--------------+
|date_time                |val|add_months+2|add_months+val|
+-------------------------+---+------------+--------------+
|2020-01-01T00:01:15-03:00|1  |2020-03-01  |2020-02-01    |
|1971-10-05T16:45:00+01:00|12 |1971-12-05  |1972-10-05    |
|2019-11-08T09:30:00-05:00|3  |2020-01-08  |2020-02-08    |
|1970-01-01T00:00:00+00:00|4  |1970-03-01  |1970-05-01    |
|2020-01-06T13:29:08+00:00|5  |2020-03-06  |2020-06-06    |
+-------------------------+---+------------+--------------+

root
 |-- date_time: string (nullable = true)
 |-- val: string (nullable = true)
 |-- add_months+2: date (nullable = true)
 |-- add_months+val: date (nullable = true)
 */