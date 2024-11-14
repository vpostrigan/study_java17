package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * XML ingestion to a dataframe.
 *
 * Source of file: NASA patents dataset -
 * https://data.nasa.gov/Raw-Data/NASA-Patents/gquh-watm
 *
 * @author rambabu.posa
 */
object Lab7_41XmlToDataframeScalaApp {

  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("XML to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .load("data/chapter7/nasa-patents.xml")

    // Shows at most 5 rows from the dataframe
    df.show(5)
    df.printSchema

    spark.stop
  }

}