package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * JSON Lines ingestion in a dataframe.
 *
 * For more details about the JSON Lines format, see: http://jsonlines.org/.
 *
 * @author rambabu.posa
 */
object Lab7_31JsonLinesToDataframeScalaApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("JSON Lines to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read
      .format("json")
      .load("data/chapter7/durham-nc-foreclosure-2006-2016.json")

    // Shows at most 5 rows from the dataframe
    df.show(5) // , 13)

    df.printSchema

    spark.stop
  }

  /**
   * +--------------------+--------------------+--------------------+--------------------+--------------------+
   * |           datasetid|              fields|            geometry|    record_timestamp|            recordid|
   * +--------------------+--------------------+--------------------+--------------------+--------------------+
   * |foreclosure-2006-...|{217 E CORPORATIO...|{[-78.8922549, 36...|2017-03-06T12:41:...|629979c85b1cc68c1...|
   * |foreclosure-2006-...|{401 N QUEEN ST, ...|{[-78.895396, 35....|2017-03-06T12:41:...|e3cce8bbc3c9b804c...|
   * |foreclosure-2006-...|{403 N QUEEN ST, ...|{[-78.8950321, 35...|2017-03-06T12:41:...|311559ebfeffe7ebc...|
   * |foreclosure-2006-...|{918 GILBERT ST, ...|{[-78.8873774, 35...|2017-03-06T12:41:...|7ec0761bd385bab8a...|
   * |foreclosure-2006-...|{721 LIBERTY ST, ...|{[-78.888343, 35....|2017-03-06T12:41:...|c81ae2921ffca8125...|
   * +--------------------+--------------------+--------------------+--------------------+--------------------+
   * only showing top 5 rows
   * *
   * root
   * |-- datasetid: string (nullable = true)
   * |-- fields: struct (nullable = true)
   * |    |-- address: string (nullable = true)
   * |    |-- geocode: array (nullable = true)
   * |    |    |-- element: double (containsNull = true)
   * |    |-- parcel_number: string (nullable = true)
   * |    |-- year: string (nullable = true)
   * |-- geometry: struct (nullable = true)
   * |    |-- coordinates: array (nullable = true)
   * |    |    |-- element: double (containsNull = true)
   * |    |-- type: string (nullable = true)
   * |-- record_timestamp: string (nullable = true)
   * |-- recordid: string (nullable = true)
   */
}