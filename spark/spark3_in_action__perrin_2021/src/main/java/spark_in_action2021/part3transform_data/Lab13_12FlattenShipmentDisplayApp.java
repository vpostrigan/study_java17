package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author jgp
 */
public class Lab13_12FlattenShipmentDisplayApp {

    public static void main(String[] args) {
        Lab13_12FlattenShipmentDisplayApp app = new Lab13_12FlattenShipmentDisplayApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Flatenning JSON doc describing shipments")
                .master("local")
                .getOrCreate();

        // Reads a JSON, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/chapter12/json/shipment.json");

        // explode создает новую запись для каждого элемента заданного массива
        df = df
                .withColumn("supplier_name", df.col("supplier.name"))
                .withColumn("supplier_city", df.col("supplier.city"))
                .withColumn("supplier_state", df.col("supplier.state"))
                .withColumn("supplier_country", df.col("supplier.country"))
                .drop("supplier")
                .withColumn("customer_name", df.col("customer.name"))
                .withColumn("customer_city", df.col("customer.city"))
                .withColumn("customer_state", df.col("customer.state"))
                .withColumn("customer_country", df.col("customer.country"))
                .drop("customer")
                .withColumn("items", explode(df.col("books")));
        df = df
                .withColumn("qty", df.col("items.qty"))
                .withColumn("title", df.col("items.title"))
                .drop("items")
                .drop("books");

        // Shows at most 5 rows from the dataframe (there's only one anyway)
        df.show(5, false);
        df.printSchema();

        df.createOrReplaceTempView("shipment_detail");
        Dataset<Row> bookCountDf =
                spark.sql("SELECT COUNT(*) AS bookCount FROM shipment_detail");
        bookCountDf.show(false);
    }
/*
+----------+----------+--------------------+--------------+--------------+----------------+-------------------+-------------+--------------+----------------+---+----------------------------+
|date      |shipmentId|supplier_name       |supplier_city |supplier_state|supplier_country|customer_name      |customer_city|customer_state|customer_country|qty|title                       |
+----------+----------+--------------------+--------------+--------------+----------------+-------------------+-------------+--------------+----------------+---+----------------------------+
|2019-10-05|458922    |Manning Publications|Shelter Island|New York      |USA             |Jean Georges Perrin|Chapel Hill  |North Carolina|USA             |2  |Spark with Java             |
|2019-10-05|458922    |Manning Publications|Shelter Island|New York      |USA             |Jean Georges Perrin|Chapel Hill  |North Carolina|USA             |25 |Spark in Action, 2nd Edition|
|2019-10-05|458922    |Manning Publications|Shelter Island|New York      |USA             |Jean Georges Perrin|Chapel Hill  |North Carolina|USA             |1  |Spark in Action, 1st Edition|
+----------+----------+--------------------+--------------+--------------+----------------+-------------------+-------------+--------------+----------------+---+----------------------------+

root
 |-- date: string (nullable = true)
 |-- shipmentId: long (nullable = true)
 |-- supplier_name: string (nullable = true)
 |-- supplier_city: string (nullable = true)
 |-- supplier_state: string (nullable = true)
 |-- supplier_country: string (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- customer_city: string (nullable = true)
 |-- customer_state: string (nullable = true)
 |-- customer_country: string (nullable = true)
 |-- qty: long (nullable = true)
 |-- title: string (nullable = true)

+---------+
|bookCount|
+---------+
|3        |
+---------+

 */
}
