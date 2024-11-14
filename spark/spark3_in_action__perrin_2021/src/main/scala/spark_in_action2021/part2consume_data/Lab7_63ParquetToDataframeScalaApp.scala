package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * Parquet ingestion in a dataframe.
 *
 * Source of file: Apache Parquet project -
 * https://github.com/apache/parquet-testing
 *
 * @author rambabu.posa
 */
object Lab7_63ParquetToDataframeScalaApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Parquet to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a Parquet file, stores it in a dataframe
    val df = spark.read
      .format("parquet")
      .load("data/chapter7/alltypes_plain.parquet")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()
    println(s"The dataframe has ${df.count} rows.")

    spark.stop
  }

  /**
   * +---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
   * | id|bool_col|tinyint_col|smallint_col|int_col|bigint_col|float_col|double_col|     date_string_col|string_col|      timestamp_col|
   * +---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
   * |  4|    true|          0|           0|      0|         0|      0.0|       0.0|[30 33 2F 30 31 2...|      [30]|2009-03-01 02:00:00|
   * |  5|   false|          1|           1|      1|        10|      1.1|      10.1|[30 33 2F 30 31 2...|      [31]|2009-03-01 02:01:00|
   * |  6|    true|          0|           0|      0|         0|      0.0|       0.0|[30 34 2F 30 31 2...|      [30]|2009-04-01 03:00:00|
   * |  7|   false|          1|           1|      1|        10|      1.1|      10.1|[30 34 2F 30 31 2...|      [31]|2009-04-01 03:01:00|
   * |  2|    true|          0|           0|      0|         0|      0.0|       0.0|[30 32 2F 30 31 2...|      [30]|2009-02-01 02:00:00|
   * |  3|   false|          1|           1|      1|        10|      1.1|      10.1|[30 32 2F 30 31 2...|      [31]|2009-02-01 02:01:00|
   * |  0|    true|          0|           0|      0|         0|      0.0|       0.0|[30 31 2F 30 31 2...|      [30]|2009-01-01 02:00:00|
   * |  1|   false|          1|           1|      1|        10|      1.1|      10.1|[30 31 2F 30 31 2...|      [31]|2009-01-01 02:01:00|
   * +---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
   * *
   * root
   * |-- id: integer (nullable = true)
   * |-- bool_col: boolean (nullable = true)
   * |-- tinyint_col: integer (nullable = true)
   * |-- smallint_col: integer (nullable = true)
   * |-- int_col: integer (nullable = true)
   * |-- bigint_col: long (nullable = true)
   * |-- float_col: float (nullable = true)
   * |-- double_col: double (nullable = true)
   * |-- date_string_col: binary (nullable = true)
   * |-- string_col: binary (nullable = true)
   * |-- timestamp_col: timestamp (nullable = true)
   */
}