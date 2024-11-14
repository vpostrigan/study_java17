package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * week of year function.
 *
 * @author rambabu.posa
 */
object Lab13_57WeekOfYearScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("weekofyear function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("imferSchema", true)
      .load("data/chapter13/functions/dates.csv")

    val df2 = df.withColumn("weekofyear", F.weekofyear(F.col("date_time")))

    df2.show(5, false)
    df2.printSchema()

    spark.stop
  }

}
/*
+-------------------------+---+----------+
|date_time                |val|weekofyear|
+-------------------------+---+----------+
|2020-01-01T00:01:15-03:00|1  |1         |
|1971-10-05T16:45:00+01:00|12 |40        |
|2019-11-08T09:30:00-05:00|3  |45        |
|1970-01-01T00:00:00+00:00|4  |1         |
|2020-01-06T13:29:08+00:00|5  |2         |
+-------------------------+---+----------+

root
 |-- date_time: string (nullable = true)
 |-- val: string (nullable = true)
 |-- weekofyear: integer (nullable = true)
 */