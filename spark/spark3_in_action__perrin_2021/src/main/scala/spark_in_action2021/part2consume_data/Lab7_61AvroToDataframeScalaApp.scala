package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * Avro ingestion in a dataframe.
 *
 * Source of file: Apache Avro project -
 * https://github.com/apache/orc/tree/master/examples
 *
 * @author rambabu.posa
 */
object Lab7_61AvroToDataframeScalaApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Avro to Dataframe")
      //.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:2.4.5")
      .master("local[*]")
      .getOrCreate

    // Reads an Avro file, stores it in a dataframe
    val df = spark.read
      .format("avro")
      .load("data/chapter7/weather.avro")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()
    println(s"The dataframe has ${df.count} rows.")

    spark.stop
  }

  /**
   * +------------+-------------+----+
   * |     station|         time|temp|
   * +------------+-------------+----+
   * |011990-99999|-619524000000|   0|
   * |011990-99999|-619506000000|  22|
   * |011990-99999|-619484400000| -11|
   * |012650-99999|-655531200000| 111|
   * |012650-99999|-655509600000|  78|
   * +------------+-------------+----+
   *
   * root
   * |-- station: string (nullable = true)
   * |-- time: long (nullable = true)
   * |-- temp: integer (nullable = true)
   */
}