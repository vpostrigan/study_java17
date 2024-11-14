package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * Text ingestion in a dataframe.
 *
 * Source of file: Rome & Juliet (Shakespeare) -
 * http://www.gutenberg.org/cache/epub/1777/pg1777.txt
 *
 * @author rambabu.posa
 */
object Lab7_51TextToDataframeScalaApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Text to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
    val df = spark.read
      .format("text")
      .load("data/chapter7/romeo-juliet-pg1777.txt")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema

    spark.stop
  }

  /**
   * +--------------------+
   * |               value|
   * +--------------------+
   * |                    |
   * |This Etext file i...|
   * |cooperation with ...|
   * |Future and Shakes...|
   * |Etexts that are N...|
   * |                    |
   * |*This Etext has c...|
   * |                    |
   * |<<THIS ELECTRONIC...|
   * |SHAKESPEARE IS CO...|
   * +--------------------+
   * only showing top 10 rows
   *
   * root
   * |-- value: string (nullable = true)
   */
}