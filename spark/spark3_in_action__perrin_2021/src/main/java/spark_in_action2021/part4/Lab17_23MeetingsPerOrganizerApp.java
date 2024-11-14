package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lab17_23MeetingsPerOrganizerApp {

    public static void main(String[] args) {
        Lab17_23MeetingsPerOrganizerApp app = new Lab17_23MeetingsPerOrganizerApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("delta")
                .load(Lab17_21FeedDeltaLakeApp.OUTFILE);

        df = df.groupBy(col("authorType"))
                .count()
                .orderBy(col("authorType").asc_nulls_last());

        df.show(25, 0, false);
        df.printSchema();
    }
}
/*
+-------------------------------+-----+
|authorType                     |count|
+-------------------------------+-----+
|Citoyen / Citoyenne            |2383 |
|Organisation à but lucratif    |101  |
|Organisation à but non lucratif|1425 |
|Élu / élue et Institution      |4104 |
|null                           |1488 |
+-------------------------------+-----+

root
 |-- authorType: string (nullable = true)
 |-- count: long (nullable = false)
 */