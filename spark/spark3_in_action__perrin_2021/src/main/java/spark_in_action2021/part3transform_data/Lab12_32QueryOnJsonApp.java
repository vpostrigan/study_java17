package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Using JSONpath-like in SQL queries.
 *
 * @author jgp
 */
public class Lab12_32QueryOnJsonApp {

    public static void main(String[] args) {
        Lab12_32QueryOnJsonApp app = new Lab12_32QueryOnJsonApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Query on a JSON doc")
                .master("local")
                .getOrCreate();

        // Reads a JSON, stores it in a dataframe
        Dataset<Row> df = spark.read().format("json")
                .option("multiline", true)
                .load("data/chapter12/json/store.json");
        df.show(5, false);
/*
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|expensive|store                                                                                                                                                                                                                                                                     |
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|10       |{{red, 19.95}, [{Nigel Rees, reference, null, 8.95, Sayings of the Century}, {Evelyn Waugh, fiction, null, 12.99, Sword of Honour}, {Herman Melville, fiction, 0-553-21311-3, 8.99, Moby Dick}, {J. R. R. Tolkien, fiction, 0-395-19395-8, 22.99, The Lord of the Rings}]}|
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 */

        // Explode the array
        df = df.withColumn("items", functions.explode(df.col("store.book")));
        // Creates a view so I can use SQL
        df.createOrReplaceTempView("books");
        df.show(5, false);
/*
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
|expensive|store                                                                                                                                                                                                                                                                     |items                                                                   |
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
|10       |{{red, 19.95}, [{Nigel Rees, reference, null, 8.95, Sayings of the Century}, {Evelyn Waugh, fiction, null, 12.99, Sword of Honour}, {Herman Melville, fiction, 0-553-21311-3, 8.99, Moby Dick}, {J. R. R. Tolkien, fiction, 0-395-19395-8, 22.99, The Lord of the Rings}]}|{Nigel Rees, reference, null, 8.95, Sayings of the Century}             |
|10       |{{red, 19.95}, [{Nigel Rees, reference, null, 8.95, Sayings of the Century}, {Evelyn Waugh, fiction, null, 12.99, Sword of Honour}, {Herman Melville, fiction, 0-553-21311-3, 8.99, Moby Dick}, {J. R. R. Tolkien, fiction, 0-395-19395-8, 22.99, The Lord of the Rings}]}|{Evelyn Waugh, fiction, null, 12.99, Sword of Honour}                   |
|10       |{{red, 19.95}, [{Nigel Rees, reference, null, 8.95, Sayings of the Century}, {Evelyn Waugh, fiction, null, 12.99, Sword of Honour}, {Herman Melville, fiction, 0-553-21311-3, 8.99, Moby Dick}, {J. R. R. Tolkien, fiction, 0-395-19395-8, 22.99, The Lord of the Rings}]}|{Herman Melville, fiction, 0-553-21311-3, 8.99, Moby Dick}              |
|10       |{{red, 19.95}, [{Nigel Rees, reference, null, 8.95, Sayings of the Century}, {Evelyn Waugh, fiction, null, 12.99, Sword of Honour}, {Herman Melville, fiction, 0-553-21311-3, 8.99, Moby Dick}, {J. R. R. Tolkien, fiction, 0-395-19395-8, 22.99, The Lord of the Rings}]}|{J. R. R. Tolkien, fiction, 0-395-19395-8, 22.99, The Lord of the Rings}|
+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
 */
        Dataset<Row> authorsOfReferenceBookDf =
                spark.sql("SELECT items.author FROM books WHERE items.category = 'reference'");
        authorsOfReferenceBookDf.show(5);
/*
+----------+
|    author|
+----------+
|Nigel Rees|
+----------+
 */
    }

}
