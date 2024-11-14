package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lab17_22MeetingsPerDepartmentApp {

    public static void main(String[] args) {
        Lab17_22MeetingsPerDepartmentApp app = new Lab17_22MeetingsPerDepartmentApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Counting the number of meetings per department")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("delta")
                .load(Lab17_21FeedDeltaLakeApp.OUTFILE);

        df = df.groupBy(col("authorDept"))
                .count()
                .orderBy(col("count").desc_nulls_first());

        df.show(25);
        df.printSchema();
    }

}
/*
|authorDept|count|
+----------+-----+
|        75|  489|
|        59|  323|
|        69|  242|
|        33|  218|
|        78|  211|
|        92|  208|
|        31|  208|
|        13|  204|
|        38|  201|
|        44|  183|
|        91|  182|
|        76|  164|
|        77|  160|
|      null|  155|
|        35|  153|
|         6|  147|
|        62|  144|
|        45|  141|
|        67|  140|
|        95|  136|
|        26|  134|
|        94|  132|
|        57|  128|
|        34|  123|
|        27|  123|
+----------+-----+
only showing top 25 rows

root
 |-- authorDept: integer (nullable = true)
 |-- count: long (nullable = false)
 */