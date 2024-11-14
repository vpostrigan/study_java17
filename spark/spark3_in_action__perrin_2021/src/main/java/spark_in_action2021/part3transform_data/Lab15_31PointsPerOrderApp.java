package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orders analytics.
 *
 * @author jgp
 */
public class Lab15_31PointsPerOrderApp {
    private static Logger log = LoggerFactory.getLogger(Lab15_31PointsPerOrderApp.class);

    public static void main(String[] args) {
        Lab15_31PointsPerOrderApp app = new Lab15_31PointsPerOrderApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Orders loyalty point")
                .master("local[*]")
                .getOrCreate();

        spark.udf().register("pointAttribution", new Lab15_31PointAttributionUdaf());

        // Reads a CSV file with header, called orders.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/chapter15/orders/orders.csv");
        df.show(20);

        // Calculating the points for each customer, not each order
        Dataset<Row> pointDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(
                        sum("quantity"),
                        callUDF("pointAttribution", col("quantity")).as("point"));
        pointDf.show(20);

        // Alternate way: calculate order by order
        int max = Lab15_31PointAttributionUdaf.MAX_POINT_PER_ORDER;
        Dataset<Row> eachOrderDf = df
                .withColumn(
                        "point",
                        when(col("quantity").$greater(max), max)
                                .otherwise(col("quantity")))
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(
                        sum("quantity"),
                        sum("point").as("point"));
        eachOrderDf.show(20);
    }

}
/*
+------------+--------+-----+--------+-------+----------+
|   firstName|lastName|state|quantity|revenue| timestamp|
+------------+--------+-----+--------+-------+----------+
|Jean-Georges|  Perrin|   NC|       1|    300|1551903533|
|Jean-Georges|  Perrin|   NC|       2|    120|1551903567|
|Jean-Georges|  Perrin|   CA|       4|     75|1551903599|
|      Holden|   Karau|   CA|       6|     37|1551904299|
|       Ginni| Rometty|   NY|       7|     91|1551916792|
|      Holden|   Karau|   CA|       4|    153|1552876129|
+------------+--------+-----+--------+-------+----------+

+------------+--------+-----+-------------+-----+
|   firstName|lastName|state|sum(quantity)|point|
+------------+--------+-----+-------------+-----+
|       Ginni| Rometty|   NY|            7|    3|
|Jean-Georges|  Perrin|   CA|            4|    3|
|      Holden|   Karau|   CA|           10|    6|
|Jean-Georges|  Perrin|   NC|            3|    3|
+------------+--------+-----+-------------+-----+

+------------+--------+-----+-------------+-----+
|   firstName|lastName|state|sum(quantity)|point|
+------------+--------+-----+-------------+-----+
|       Ginni| Rometty|   NY|            7|    3|
|Jean-Georges|  Perrin|   CA|            4|    3|
|      Holden|   Karau|   CA|           10|    6|
|Jean-Georges|  Perrin|   NC|            3|    3|
+------------+--------+-----+-------------+-----+
 */