package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author jgp
 */
public class Lab13_11JsonShipmentDisplayApp {

    public static void main(String[] args) {
        Lab13_11JsonShipmentDisplayApp app = new Lab13_11JsonShipmentDisplayApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Display of shipment")
                .master("local")
                .getOrCreate();

        // Reads a JSON, stores it in a dataframe
        Dataset<Row> df = spark.read().format("json")
                .option("multiline", true)
                .load("data/chapter12/json/shipment.json");

        // Shows at most 5 rows from the dataframe (there's only one anyway)
        df.show(5, 16);
        df.printSchema();
    }
/*
+----------------+----------------+----------+----------+----------------+
|           books|        customer|      date|shipmentId|        supplier|
+----------------+----------------+----------+----------+----------------+
|[{2, Spark wi...|{Chapel Hill,...|2019-10-05|    458922|{Shelter Isla...|
+----------------+----------------+----------+----------+----------------+

root
 |-- books: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- qty: long (nullable = true)
 |    |    |-- title: string (nullable = true)
 |-- customer: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- state: string (nullable = true)
 |-- date: string (nullable = true)
 |-- shipmentId: long (nullable = true)
 |-- supplier: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- state: string (nullable = true)

 */
}
