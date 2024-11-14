package spark_in_action2021.part1theory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * https://github.com/jgperrin/net.jgp.books.spark.ch01
 */
public class Lab1_CsvToDataframeApp {

    public static void main(String[] args) {
        Lab1_CsvToDataframeApp app = new Lab1_CsvToDataframeApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        try (SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local")
                .getOrCreate();) {
            // Reads a CSV file with header, called books.csv, stores it in a dataframe
            Dataset<Row> df = spark.read().format("csv")
                    .option("header", "true")
                    .load("data/books.csv");

            // Shows at most 5 rows from the dataframe
            df.show(5);
        }
    }

}
