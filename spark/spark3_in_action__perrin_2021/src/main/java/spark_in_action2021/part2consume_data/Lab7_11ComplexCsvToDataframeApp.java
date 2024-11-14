package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * <p>
 * https://github.com/jgperrin/net.jgp.books.spark.ch07/
 */
public class Lab7_11ComplexCsvToDataframeApp {

    public static void main(String[] args) {
        Lab7_11ComplexCsvToDataframeApp app = new Lab7_11ComplexCsvToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", "true")
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "M/d/yyyy")
                .option("inferSchema", true)
                .option("comment", "#")
                .load("data/chapter7/books.csv");

        System.out.println("Excerpt of the dataframe content:");
        df.show(7, 10);
//+---+--------+----------+-----------+----------+
//| id|authorId|     title|releaseDate|      link|
//+---+--------+----------+-----------+----------+
//|  1|       1|Fantast...| 11/18/2016|http://...|
//|  2|       1|Harry P...| 10/06/2015|http://...|
//|  3|       1|The Tal...| 12/04/2008|http://...|
//|  4|       1|Harry P...| 10/04/2016|http://...|
//|  5|       2|Informi...| 04/23/2017|http://...|
//|  6|       2|Develop...| 12/28/2016|http://...|
//|  7|       3|Adventu...| 05/26/1994|http://...|
//+---+--------+----------+-----------+----------+
//only showing top 7 rows

        System.out.println("Dataframe's schema:");
        df.printSchema();
//        root
//                |-- id: integer (nullable = true)
//                |-- authorId: integer (nullable = true)
//                |-- title: string (nullable = true)
//                |-- releaseDate: string (nullable = true)
//                |-- link: string (nullable = true)
    }

}
