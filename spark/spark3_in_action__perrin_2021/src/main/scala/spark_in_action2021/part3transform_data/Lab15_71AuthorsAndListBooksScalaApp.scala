package spark_in_action2021.part3transform_data

import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * This lab combines a join with a group by to find out the list of books by author.
 *
 * @author rambabu.posa
 */
object Lab15_71AuthorsAndListBooksScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Authors and Books")
      .master("local[*]")
      .getOrCreate

    var filename: String = "data/chapter15/books/authors.csv"
    val authorsDf: Dataset[Row] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    filename = "data/chapter15/books/books.csv"
    val booksDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    val libraryDf = authorsDf
      .join(booksDf, authorsDf.col("id") === booksDf.col("authorId"), "left")
      .withColumn("bookId", booksDf.col("id"))
      .drop(booksDf.col("id"))
      .orderBy(col("name").asc)

    libraryDf.show()
    libraryDf.printSchema()

    println("List of authors and their titles")
    var booksByAuthorDf = libraryDf
      .groupBy(col("name"))
      .agg(collect_list("title"))

    booksByAuthorDf.show(false)
    booksByAuthorDf.printSchema()

    println("List of authors and their titles, with ids")
    booksByAuthorDf = libraryDf
      .select("authorId", "name", "bookId", "title")
      .withColumn("book", struct(col("bookId"), col("title")))
      .drop("bookId", "title")
      .groupBy(col("authorId"), col("name"))
      .agg(collect_list("book").as("book"))

    booksByAuthorDf.show(false)
    booksByAuthorDf.printSchema()

    spark.stop
  }

}
/*
+---+-------------------+--------------------+--------------------+--------+--------------------+--------------------+--------------------+------+
| id|               name|                link|           wikipedia|authorId|               title|         releaseDate|                link|bookId|
+---+-------------------+--------------------+--------------------+--------+--------------------+--------------------+--------------------+------+
| 11|       Alan Mycroft|http://amzn.to/2A...|                null|    null|                null|                null|                null|  null|
| 13|      Blaise Pascal|http://amzn.to/2z...|                null|      13|             Pensées|          12/31/1670|http://amzn.to/2j...|    19|
|  6|        Craig Walls|http://amzn.to/2A...|                null|       6|Spring in Action:...|            11/28/14|http://amzn.to/2y...|    14|
|  6|        Craig Walls|http://amzn.to/2A...|                null|       6|Spring Boot in Ac...|              1/3/16|http://amzn.to/2h...|    13|
|  4|      Denis Diderot|http://amzn.to/2i...|                null|       4|Diderot Encyclope...|                null|http://amzn.to/2i...|    11|
|  4|      Denis Diderot|http://amzn.to/2i...|                null|       4|Jacques le Fataliste|              3/1/00|http://amzn.to/2u...|    10|
|  1|      J. K. Rowling|http://amzn.to/2l...|                null|       1|Harry Potter and ...|             10/4/16|http://amzn.to/2k...|     4|
|  1|      J. K. Rowling|http://amzn.to/2l...|                null|       1|Fantastic Beasts ...|            11/18/16|http://amzn.to/2k...|     1|
|  1|      J. K. Rowling|http://amzn.to/2l...|                null|       1|Harry Potter and ...|             10/6/15|http://amzn.to/2l...|     2|
|  1|      J. K. Rowling|http://amzn.to/2l...|                null|       1|The Tales of Beed...|             12/4/08|http://amzn.to/2k...|     3|
| 14|Jean de La Fontaine|http://amzn.to/2A...|                null|      14|     Fables choisies| mises en vers pa...|            9/1/1999|    20|
|  2|Jean-Georges Perrin|http://amzn.to/2w...|https://en.wikipe...|       2|  Spark in Action 2e|            01/01/20|  http://jgp.net/sia|    24|
|  2|Jean-Georges Perrin|http://amzn.to/2w...|https://en.wikipe...|       2|Development Tools...|            12/28/16|http://amzn.to/2v...|     6|
|  2|Jean-Georges Perrin|http://amzn.to/2w...|https://en.wikipe...|       2|Informix 12.10 on...|             4/23/17|http://amzn.to/2i...|     5|
|  2|Jean-Georges Perrin|http://amzn.to/2w...|https://en.wikipe...|       2|   Une approche du C|            09/01/94|     http://jgp.net/|    25|
|  7|        John Sonmez|http://amzn.to/2h...|                null|       7|Soft Skills: The ...|            12/29/14|http://amzn.to/2z...|    15|
|  8|     John Steinbeck|http://amzn.to/2A...|                null|       8|     Of Mice and Men|                null|http://amzn.to/2z...|    16|
| 10|        Mario Fusco|http://amzn.to/2A...|                null|    null|                null|                null|                null|  null|
|  3|         Mark Twain|http://amzn.to/2v...|https://en.wikipe...|       3|A Connecticut Yan...|             6/17/17|http://amzn.to/2x...|     8|
|  3|         Mark Twain|http://amzn.to/2v...|https://en.wikipe...|       3|Adventures of Huc...|             5/26/94|http://amzn.to/2w...|     7|
+---+-------------------+--------------------+--------------------+--------+--------------------+--------------------+--------------------+------+
only showing top 20 rows

root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- link: string (nullable = true)
 |-- wikipedia: string (nullable = true)
 |-- authorId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- releaseDate: string (nullable = true)
 |-- link: string (nullable = true)
 |-- bookId: integer (nullable = true)

List of authors and their titles
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|name               |collect_list(title)                                                                                                                                                                                                                                                                                              |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Alan Mycroft       |[]                                                                                                                                                                                                                                                                                                               |
|Blaise Pascal      |[Pensées]                                                                                                                                                                                                                                                                                                        |
|Craig Walls        |[Spring in Action: Covers Spring 4, Spring Boot in Action]                                                                                                                                                                                                                                                       |
|Denis Diderot      |[Diderot Encyclopedia: The Complete Illustrations 1762-1777, Jacques le Fataliste]                                                                                                                                                                                                                               |
|J. K. Rowling      |[Harry Potter and the Chamber of Secrets: The Illustrated Edition (Harry Potter, Book 2), The Tales of Beedle the Bard, Standard Edition (Harry Potter), Harry Potter and the Sorcerer's Stone: The Illustrated Edition (Harry Potter, Book 1), Fantastic Beasts and Where to Find Them: The Original Screenplay]|
|Jean de La Fontaine|[Fables choisies]                                                                                                                                                                                                                                                                                                |
|Jean-Georges Perrin|[Une approche du C, Spark in Action 2e, Development Tools in 2006: any Room for a 4GL-style Language?: An independent study by Jean Georges Perrin, IIUG Board Member, Informix 12.10 on Mac 10.12 with a dash of Java 8: The Tale of the Apple, the Coffee, and a Great Database]                               |
|John Sonmez        |[Soft Skills: The software developer's life manual]                                                                                                                                                                                                                                                              |
|John Steinbeck     |[Of Mice and Men]                                                                                                                                                                                                                                                                                                |
|Mario Fusco        |[]                                                                                                                                                                                                                                                                                                               |
|Mark Twain         |[A Connecticut Yankee in King Arthur's Court, Adventures of Huckleberry Finn]                                                                                                                                                                                                                                    |
|Raoul-Gabriel Urma |[Java 8 in Action: Lambdas]                                                                                                                                                                                                                                                                                      |
|René Descartes     |[Discourse on Method and Meditations on First Philosophy]                                                                                                                                                                                                                                                        |
|Stephen King       |[]                                                                                                                                                                                                                                                                                                               |
|William Shakespeare|[Macbeth, Twelfth Night, Hamlet]                                                                                                                                                                                                                                                                                 |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

root
 |-- name: string (nullable = true)
 |-- collect_list(title): array (nullable = false)
 |    |-- element: string (containsNull = false)

List of authors and their titles, with ids
+--------+-------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|authorId|name               |book                                                                                                                                                                                                                                                                                                                                 |
+--------+-------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|null    |Alan Mycroft       |[{null, null}]                                                                                                                                                                                                                                                                                                                       |
|13      |Blaise Pascal      |[{19, Pensées}]                                                                                                                                                                                                                                                                                                                      |
|6       |Craig Walls        |[{14, Spring in Action: Covers Spring 4}, {13, Spring Boot in Action}]                                                                                                                                                                                                                                                               |
|4       |Denis Diderot      |[{11, Diderot Encyclopedia: The Complete Illustrations 1762-1777}, {10, Jacques le Fataliste}]                                                                                                                                                                                                                                       |
|1       |J. K. Rowling      |[{4, Harry Potter and the Chamber of Secrets: The Illustrated Edition (Harry Potter, Book 2)}, {3, The Tales of Beedle the Bard, Standard Edition (Harry Potter)}, {2, Harry Potter and the Sorcerer's Stone: The Illustrated Edition (Harry Potter, Book 1)}, {1, Fantastic Beasts and Where to Find Them: The Original Screenplay}]|
|14      |Jean de La Fontaine|[{20, Fables choisies}]                                                                                                                                                                                                                                                                                                              |
|2       |Jean-Georges Perrin|[{25, Une approche du C}, {24, Spark in Action 2e}, {6, Development Tools in 2006: any Room for a 4GL-style Language?: An independent study by Jean Georges Perrin, IIUG Board Member}, {5, Informix 12.10 on Mac 10.12 with a dash of Java 8: The Tale of the Apple, the Coffee, and a Great Database}]                             |
|7       |John Sonmez        |[{15, Soft Skills: The software developer's life manual}]                                                                                                                                                                                                                                                                            |
|8       |John Steinbeck     |[{16, Of Mice and Men}]                                                                                                                                                                                                                                                                                                              |
|null    |Mario Fusco        |[{null, null}]                                                                                                                                                                                                                                                                                                                       |
|3       |Mark Twain         |[{8, A Connecticut Yankee in King Arthur's Court}, {7, Adventures of Huckleberry Finn}]                                                                                                                                                                                                                                              |
|9       |Raoul-Gabriel Urma |[{17, Java 8 in Action: Lambdas}]                                                                                                                                                                                                                                                                                                    |
|15      |René Descartes     |[{21, Discourse on Method and Meditations on First Philosophy}]                                                                                                                                                                                                                                                                      |
|null    |Stephen King       |[{null, null}]                                                                                                                                                                                                                                                                                                                       |
|12      |William Shakespeare|[{23, Macbeth}, {22, Twelfth Night}, {18, Hamlet}]                                                                                                                                                                                                                                                                                   |
+--------+-------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

root
 |-- authorId: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- book: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- bookId: integer (nullable = true)
 |    |    |-- title: string (nullable = true)

 */