package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class Lab15_41MaxValueAggregationApp {

    public static void main(String[] args) {
        Lab15_41MaxValueAggregationApp app = new Lab15_41MaxValueAggregationApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Aggregates max values")
                .master("local[*]")
                .getOrCreate();

        // Reads a CSV file with header, called courses.csv, stores it in a dataframe
        Dataset<Row> rawDf = spark.read().format("csv")
                .option("header", true)
                .option("sep", "|")
                .load("data/chapter15/misc/courses.csv");
        rawDf.show(20);

        // Performs the aggregation, grouping on columns id, batch_id, and
        // session_name
        Dataset<Row> maxValuesDf = rawDf
                .select("*")
                .groupBy(col("id"), col("batch_id"), col("session_name"))
                .agg(max("time"));
        maxValuesDf.show(5);
    }

}
/*
+---+-----+--------+------------+-------------+-----+
| id|batch|batch_id|session_name|         time|value|
+---+-----+--------+------------+-------------+-----+
|001|  abc|     098|    course-I|1551409926133|  2.3|
|001|  abc|     098|    course-I|1551404747843|  7.3|
|001|  abc|     098|    course-I|1551409934220|  6.3|
|002|  def|     097|   course-II|1551409926453|  2.3|
|002|  def|     097|   course-II|1551404747843|  7.3|
|002|  def|     097|   course-II|1551409934220|  6.3|
+---+-----+--------+------------+-------------+-----+

+---+--------+------------+-------------+
| id|batch_id|session_name|    max(time)|
+---+--------+------------+-------------+
|002|     097|   course-II|1551409934220|
|001|     098|    course-I|1551409934220|
+---+--------+------------+-------------+
 */