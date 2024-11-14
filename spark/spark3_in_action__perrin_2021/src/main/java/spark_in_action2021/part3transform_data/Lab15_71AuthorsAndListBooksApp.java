package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This lab combines a join with a group by to find out the list of books by author.
 *
 * @author jgp
 */
public class Lab15_71AuthorsAndListBooksApp {

    public static void main(String[] args) {
        Lab15_71AuthorsAndListBooksApp app = new Lab15_71AuthorsAndListBooksApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Authors and Books")
                .master("local")
                .getOrCreate();

        Dataset<Row> authorsDf = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("data/chapter15/books/authors.csv");
        authorsDf.show();
        authorsDf.printSchema();

        Dataset<Row> booksDf = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("data/chapter15/books/books.csv");
        booksDf.show();
        booksDf.printSchema();

        Dataset<Row> libraryDf = authorsDf
                .join(
                        booksDf,
                        authorsDf.col("id").equalTo(booksDf.col("authorId")),
                        "left")
                .withColumn("bookId", booksDf.col("id"))
                .drop(booksDf.col("id"))
                .orderBy(col("name").asc());
        libraryDf.show();
        libraryDf.printSchema();

        System.out.println("List of authors and their titles");
        Dataset<Row> booksByAuthorDf = libraryDf
                .groupBy(col("name"))
                .agg(collect_list("title"));
        booksByAuthorDf.show(false);
        booksByAuthorDf.printSchema();

        System.out.println("List of authors and their titles, with ids");
        booksByAuthorDf = libraryDf
                .select("authorId", "name", "bookId", "title")
                .withColumn("book", struct(col("bookId"), col("title")))
                .drop("bookId", "title")
                .groupBy(col("authorId"), col("name"))
                .agg(collect_list("book").as("book"));
        booksByAuthorDf.show(false);
        booksByAuthorDf.printSchema();

        Dataset<Row> flatInvoicesDf = Lab13_21FlattenJsonApp.flattenNestedStructure(spark, booksByAuthorDf);
        flatInvoicesDf.show(false);
        flatInvoicesDf.printSchema();
    }

}
/*
2022-11-05 13:44:09.793 - INFO --- [           main] til.Version.logVersion(Version.java:133): Elasticsearch Hadoop v7.17.3 [e52f4f523a]
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

/*
+--------+-------------------+-----------+-----------------------------------------------------------------------------------------------------------------------------+
|authorId|name               |book_bookId|book_title                                                                                                                   |
+--------+-------------------+-----------+-----------------------------------------------------------------------------------------------------------------------------+
|null    |Alan Mycroft       |null       |null                                                                                                                         |
|13      |Blaise Pascal      |19         |Pensées                                                                                                                      |
|6       |Craig Walls        |14         |Spring in Action: Covers Spring 4                                                                                            |
|6       |Craig Walls        |13         |Spring Boot in Action                                                                                                        |
|4       |Denis Diderot      |11         |Diderot Encyclopedia: The Complete Illustrations 1762-1777                                                                   |
|4       |Denis Diderot      |10         |Jacques le Fataliste                                                                                                         |
|1       |J. K. Rowling      |4          |Harry Potter and the Chamber of Secrets: The Illustrated Edition (Harry Potter, Book 2)                                      |
|1       |J. K. Rowling      |3          |The Tales of Beedle the Bard, Standard Edition (Harry Potter)                                                                |
|1       |J. K. Rowling      |2          |Harry Potter and the Sorcerer's Stone: The Illustrated Edition (Harry Potter, Book 1)                                        |
|1       |J. K. Rowling      |1          |Fantastic Beasts and Where to Find Them: The Original Screenplay                                                             |
|14      |Jean de La Fontaine|20         |Fables choisies                                                                                                              |
|2       |Jean-Georges Perrin|25         |Une approche du C                                                                                                            |
|2       |Jean-Georges Perrin|24         |Spark in Action 2e                                                                                                           |
|2       |Jean-Georges Perrin|6          |Development Tools in 2006: any Room for a 4GL-style Language?: An independent study by Jean Georges Perrin, IIUG Board Member|
|2       |Jean-Georges Perrin|5          |Informix 12.10 on Mac 10.12 with a dash of Java 8: The Tale of the Apple, the Coffee, and a Great Database                   |
|7       |John Sonmez        |15         |Soft Skills: The software developer's life manual                                                                            |
|8       |John Steinbeck     |16         |Of Mice and Men                                                                                                              |
|null    |Mario Fusco        |null       |null                                                                                                                         |
|3       |Mark Twain         |8          |A Connecticut Yankee in King Arthur's Court                                                                                  |
|3       |Mark Twain         |7          |Adventures of Huckleberry Finn                                                                                               |
+--------+-------------------+-----------+-----------------------------------------------------------------------------------------------------------------------------+
only showing top 20 rows

root
 |-- authorId: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- book_bookId: integer (nullable = true)
 |-- book_title: string (nullable = true)
 */