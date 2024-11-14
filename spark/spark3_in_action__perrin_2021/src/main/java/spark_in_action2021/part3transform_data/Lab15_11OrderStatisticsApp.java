package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

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
public class Lab15_11OrderStatisticsApp {
    private static Logger log = LoggerFactory.getLogger(Lab15_11OrderStatisticsApp.class);

    public static void main(String[] args) {
        Lab15_11OrderStatisticsApp app = new Lab15_11OrderStatisticsApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Orders analytics")
                .master("local[*]")
                .getOrCreate();

        // Reads a CSV file with header, called orders.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/chapter15/orders/orders.csv");
        df.show(5);

        // Calculating the orders info using the dataframe API
        Dataset<Row> apiDf = df
                .groupBy(col("firstName"), col("lastName"), col("state")) // столбцы для группировки
                .agg(// начало процесса агрегации
                        sum("quantity"), // sum для столбца quantity
                        sum("revenue"),
                        avg("revenue"));
        apiDf.show(20);

        // Calculating the orders info using SparkSQL
        df.createOrReplaceTempView("orders");
        String sqlStatement = "SELECT " +
                "    firstName, " +
                "    lastName, " +
                "    state, " +
                "    SUM(quantity), " +
                "    SUM(revenue), " +
                "    AVG(revenue) " +
                "  FROM orders " +
                "  GROUP BY firstName, lastName, state";
        Dataset<Row> sqlDf = spark.sql(sqlStatement);
        sqlDf.show(20);
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
+------------+--------+-----+--------+-------+----------+
only showing top 5 rows

+------------+--------+-----+-------------+------------+------------+
|   firstName|lastName|state|sum(quantity)|sum(revenue)|avg(revenue)|
+------------+--------+-----+-------------+------------+------------+
|       Ginni| Rometty|   NY|            7|          91|        91.0|
|Jean-Georges|  Perrin|   CA|            4|          75|        75.0|
|      Holden|   Karau|   CA|           10|         190|        95.0|
|Jean-Georges|  Perrin|   NC|            3|         420|       210.0|
+------------+--------+-----+-------------+------------+------------+

+------------+--------+-----+-------------+------------+------------+
|   firstName|lastName|state|sum(quantity)|sum(revenue)|avg(revenue)|
+------------+--------+-----+-------------+------------+------------+
|       Ginni| Rometty|   NY|            7|          91|        91.0|
|Jean-Georges|  Perrin|   CA|            4|          75|        75.0|
|      Holden|   Karau|   CA|           10|         190|        95.0|
|Jean-Georges|  Perrin|   NC|            3|         420|       210.0|
+------------+--------+-----+-------------+------------+------------+
 */