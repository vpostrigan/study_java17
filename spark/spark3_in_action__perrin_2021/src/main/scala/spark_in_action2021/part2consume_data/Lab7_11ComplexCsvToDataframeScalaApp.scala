package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 *
 * @author rambabu.posa
 */
object Lab7_11ComplexCsvToDataframeScalaApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Complex CSV to Dataframe")
      .master("local[*]")
      .getOrCreate

    println("Using Apache Spark v" + spark.version)

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("quote", "*")
      .option("dateFormat", "MM/dd/yyyy")
      .option("inferSchema", true)
      .load("data/chapter7/books.csv")

    println("Excerpt of the dataframe content:")

    // Shows at most 7 rows from the dataframe, with columns as wide as 90
    // characters
    df.show(7, 70)
    println("Dataframe's schema:")
    df.printSchema()

    spark.stop
  }

  /**
   * +---------+--------+----------------------------------------------------------------------+-----------+----------------------+
   * |       id|authorId|                                                                 title|releaseDate|                  link|
   * +---------+--------+----------------------------------------------------------------------+-----------+----------------------+
   * |        1|       1|      Fantastic Beasts and Where to Find Them: The Original Screenplay| 11/18/2016|http://amzn.to/2kup94P|
   * |        2|       1|Harry Potter and the Sorcerer's Stone: The Illustrated Edition (Har...| 10/06/2015|http://amzn.to/2l2lSwP|
   * |# comment|    null|                                                                  null|       null|                  null|
   * |        3|       1|         The Tales of Beedle the Bard, Standard Edition (Harry Potter)|  12/4/2008|http://amzn.to/2kYezqr|
   * |        4|       1|Harry Potter and the Chamber of Secrets: The Illustrated Edition (H...| 10/04/2016|http://amzn.to/2kYhL5n|
   * |        5|       2|Informix 12.10 on Mac 10.12 with a dash of Java 8: The Tale of the ...| 04/23/2017|http://amzn.to/2i3mthT|
   * |        6|       2|Development Tools in 2006: any Room for a 4GL-style Language?
   * An i...| 12/28/2016|http://amzn.to/2vBxOe1|
   * +---------+--------+----------------------------------------------------------------------+-----------+----------------------+
   * only showing top 7 rows
   *
   * Dataframe's schema:
   * root
   * |-- id: string (nullable = true)
   * |-- authorId: integer (nullable = true)
   * |-- title: string (nullable = true)
   * |-- releaseDate: string (nullable = true)
   * |-- link: string (nullable = true)
   */
}