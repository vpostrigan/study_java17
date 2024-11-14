package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lab7_12ComplexCsvToDataframePushDownFilterApp {

    public static void main(String[] args) {
        Lab7_12ComplexCsvToDataframePushDownFilterApp app = new Lab7_12ComplexCsvToDataframePushDownFilterApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "M/d/yyyy")
                .option("inferSchema", true)
                .option("comment", "#")
                .load("data/chapter7/books.csv")
                .filter("authorId = 1");

        System.out.println("Excerpt of the dataframe content:");

        // Shows at most 7 rows from the dataframe, with columns as wide as 90 characters
        df.show(7, 70);
        System.out.println("Dataframe's schema:");
        df.printSchema();
    }

}
