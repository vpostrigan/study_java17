package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This lab combines a join with a group by to find out who published more
 * books among great authors.
 *
 * @author rambabu.posa
 */
object Lab15_61AuthorsAndBooksCountBooksScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Authors and Books")
      .master("local[*]")
      .getOrCreate

    var filename: String = "data/chapter15/books/authors.csv"
    val authorsDf: DataFrame = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    authorsDf.show
    authorsDf.printSchema

    filename = "data/chapter15/books/books.csv"
    val booksDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    booksDf.show
    booksDf.printSchema

    var libraryDf = authorsDf
      .join(booksDf, authorsDf.col("id") === booksDf.col("authorId"), "left")
      .withColumn("bookId", booksDf.col("id"))
      .drop(booksDf.col("id"))
      .groupBy(authorsDf.col("id"), authorsDf.col("name"), authorsDf.col("link"))
      .count

    libraryDf = libraryDf.orderBy(libraryDf.col("count").desc)

    libraryDf.show
    libraryDf.printSchema

    spark.stop
  }

}
/*
+---+-------------------+--------------------+--------------------+
| id|               name|                link|           wikipedia|
+---+-------------------+--------------------+--------------------+
|  1|      J. K. Rowling|http://amzn.to/2l...|                null|
|  2|Jean-Georges Perrin|http://amzn.to/2w...|https://en.wikipe...|
|  4|      Denis Diderot|http://amzn.to/2i...|                null|
|  3|         Mark Twain|http://amzn.to/2v...|https://en.wikipe...|
|  5|       Stephen King|http://amzn.to/2v...|                null|
|  6|        Craig Walls|http://amzn.to/2A...|                null|
|  7|        John Sonmez|http://amzn.to/2h...|                null|
|  8|     John Steinbeck|http://amzn.to/2A...|                null|
|  9| Raoul-Gabriel Urma|http://amzn.to/2A...|                null|
| 10|        Mario Fusco|http://amzn.to/2A...|                null|
| 11|       Alan Mycroft|http://amzn.to/2A...|                null|
| 12|William Shakespeare|http://amzn.to/2j...|                null|
| 13|      Blaise Pascal|http://amzn.to/2z...|                null|
| 14|Jean de La Fontaine|http://amzn.to/2A...|                null|
| 15|     René Descartes|http://amzn.to/2j...|                null|
+---+-------------------+--------------------+--------------------+

root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- link: string (nullable = true)
 |-- wikipedia: string (nullable = true)

+---+--------+--------------------+--------------------+--------------------+
| id|authorId|               title|         releaseDate|                link|
+---+--------+--------------------+--------------------+--------------------+
|  1|       1|Fantastic Beasts ...|            11/18/16|http://amzn.to/2k...|
|  2|       1|Harry Potter and ...|             10/6/15|http://amzn.to/2l...|
|  3|       1|The Tales of Beed...|             12/4/08|http://amzn.to/2k...|
|  4|       1|Harry Potter and ...|             10/4/16|http://amzn.to/2k...|
|  5|       2|Informix 12.10 on...|             4/23/17|http://amzn.to/2i...|
|  6|       2|Development Tools...|            12/28/16|http://amzn.to/2v...|
|  7|       3|Adventures of Huc...|             5/26/94|http://amzn.to/2w...|
|  8|       3|A Connecticut Yan...|             6/17/17|http://amzn.to/2x...|
| 10|       4|Jacques le Fataliste|              3/1/00|http://amzn.to/2u...|
| 11|       4|Diderot Encyclope...|                null|http://amzn.to/2i...|
| 12|    null|   A Woman in Berlin|             7/11/06|http://amzn.to/2i...|
| 13|       6|Spring Boot in Ac...|              1/3/16|http://amzn.to/2h...|
| 14|       6|Spring in Action:...|            11/28/14|http://amzn.to/2y...|
| 15|       7|Soft Skills: The ...|            12/29/14|http://amzn.to/2z...|
| 16|       8|     Of Mice and Men|                null|http://amzn.to/2z...|
| 17|       9|Java 8 in Action:...|             Streams| and functional-s...|
| 18|      12|              Hamlet|              6/8/12|http://amzn.to/2y...|
| 19|      13|             Pensées|          12/31/1670|http://amzn.to/2j...|
| 20|      14|     Fables choisies| mises en vers pa...|            9/1/1999|
| 21|      15|Discourse on Meth...|           6/15/1999|http://amzn.to/2h...|
+---+--------+--------------------+--------------------+--------------------+
only showing top 20 rows

root
 |-- id: integer (nullable = true)
 |-- authorId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- releaseDate: string (nullable = true)
 |-- link: string (nullable = true)

+---+-------------------+--------------------+-----+
| id|               name|                link|count|
+---+-------------------+--------------------+-----+
|  2|Jean-Georges Perrin|http://amzn.to/2w...|    4|
|  1|      J. K. Rowling|http://amzn.to/2l...|    4|
| 12|William Shakespeare|http://amzn.to/2j...|    3|
|  6|        Craig Walls|http://amzn.to/2A...|    2|
|  4|      Denis Diderot|http://amzn.to/2i...|    2|
|  3|         Mark Twain|http://amzn.to/2v...|    2|
|  7|        John Sonmez|http://amzn.to/2h...|    1|
| 11|       Alan Mycroft|http://amzn.to/2A...|    1|
|  9| Raoul-Gabriel Urma|http://amzn.to/2A...|    1|
| 14|Jean de La Fontaine|http://amzn.to/2A...|    1|
| 10|        Mario Fusco|http://amzn.to/2A...|    1|
| 15|     René Descartes|http://amzn.to/2j...|    1|
|  8|     John Steinbeck|http://amzn.to/2A...|    1|
|  5|       Stephen King|http://amzn.to/2v...|    1|
| 13|      Blaise Pascal|http://amzn.to/2z...|    1|
+---+-------------------+--------------------+-----+

root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- link: string (nullable = true)
 |-- count: long (nullable = false)
 */