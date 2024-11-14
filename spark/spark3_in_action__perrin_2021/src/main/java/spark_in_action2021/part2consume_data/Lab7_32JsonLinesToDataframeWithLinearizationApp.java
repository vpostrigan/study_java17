package spark_in_action2021.part2consume_data;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * JSON Lines ingestion in a dataframe.
 * <p>
 * Note: This example is an extra, which is not described in chapter 7 of
 * the book: it illustrates how to access nested structure in the JSON
 * document directly. Linearization will be covered in chapter 12.
 *
 * @author jgp
 */
public class Lab7_32JsonLinesToDataframeWithLinearizationApp {

    public static void main(String[] args) {
        Lab7_32JsonLinesToDataframeWithLinearizationApp app =
                new Lab7_32JsonLinesToDataframeWithLinearizationApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header, called books.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("json")
                .load("data/chapter7/durham-nc-foreclosure-2006-2016.json");

        df = df.withColumn("year", col("fields.year"));
        df = df.withColumn("coordinates", col("geometry.coordinates"));

        // Shows at most 5 rows from the dataframe
        df.show(5);
        df.printSchema();
    }

    /**
     +--------------------+--------------------+--------------------+--------------------+--------------------+----+--------------------+
     |           datasetid|              fields|            geometry|    record_timestamp|            recordid|year|         coordinates|
     +--------------------+--------------------+--------------------+--------------------+--------------------+----+--------------------+
     |foreclosure-2006-...|{217 E CORPORATIO...|{[-78.8922549, 36...|2017-03-06T12:41:...|629979c85b1cc68c1...|2006|[-78.8922549, 36....|
     |foreclosure-2006-...|{401 N QUEEN ST, ...|{[-78.895396, 35....|2017-03-06T12:41:...|e3cce8bbc3c9b804c...|2006|[-78.895396, 35.9...|
     |foreclosure-2006-...|{403 N QUEEN ST, ...|{[-78.8950321, 35...|2017-03-06T12:41:...|311559ebfeffe7ebc...|2006|[-78.8950321, 35....|
     |foreclosure-2006-...|{918 GILBERT ST, ...|{[-78.8873774, 35...|2017-03-06T12:41:...|7ec0761bd385bab8a...|2006|[-78.8873774, 35....|
     |foreclosure-2006-...|{721 LIBERTY ST, ...|{[-78.888343, 35....|2017-03-06T12:41:...|c81ae2921ffca8125...|2006|[-78.888343, 35.9...|
     +--------------------+--------------------+--------------------+--------------------+--------------------+----+--------------------+
     only showing top 5 rows

     root
     |-- datasetid: string (nullable = true)
     |-- fields: struct (nullable = true)
     |    |-- address: string (nullable = true)
     |    |-- geocode: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- parcel_number: string (nullable = true)
     |    |-- year: string (nullable = true)
     |-- geometry: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- record_timestamp: string (nullable = true)
     |-- recordid: string (nullable = true)
     |-- year: string (nullable = true)
     |-- coordinates: array (nullable = true)
     |    |-- element: double (containsNull = true)
     */
}
