package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.minute;
import static org.apache.spark.sql.functions.second;

/**
 * Custom UDF to check if in range.
 *
 * @author jgp
 */
public class Lab14_31InCustomRangeApp {

    public static void main(String[] args) {
        Lab14_31InCustomRangeApp app = new Lab14_31InCustomRangeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Custom UDF to check if in range")
                .master("local[*]")
                .getOrCreate();
        spark
                .udf()
                .register("inRange", new Lab14_32InRangeUdf(), DataTypes.BooleanType);

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("time", DataTypes.StringType, false),
                DataTypes.createStructField("range", DataTypes.StringType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("id1", "2019-03-11 05:00:00", "00h00-07h30;23h30-23h59"));
        rows.add(RowFactory.create("id2", "2019-03-11 09:00:00", "00h00-07h30;23h30-23h59"));
        rows.add(RowFactory.create("id3", "2019-03-11 10:30:00", "00h00-07h30;23h30-23h59"));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show(false);

        df = df
                .withColumn(
                        "date",
                        date_format(col("time"), "yyyy-MM-dd HH:mm:ss.SSSS"))
                .withColumn("h", hour(col("date")))
                .withColumn("m", minute(col("date")))
                .withColumn("s", second(col("date")))
                .withColumn("event", expr("h*3600 + m*60 +s"))
                .drop("date")
                .drop("h")
                .drop("m")
                .drop("s");
        df.show(false);

        df = df.withColumn("between",
                callUDF("inRange", col("range"), col("event")));
        df.show(false);
    }

}
/*
+---+-------------------+-----------------------+
|id |time               |range                  |
+---+-------------------+-----------------------+
|id1|2019-03-11 05:00:00|00h00-07h30;23h30-23h59|
|id2|2019-03-11 09:00:00|00h00-07h30;23h30-23h59|
|id3|2019-03-11 10:30:00|00h00-07h30;23h30-23h59|
+---+-------------------+-----------------------+

+---+-------------------+-----------------------+-----+
|id |time               |range                  |event|
+---+-------------------+-----------------------+-----+
|id1|2019-03-11 05:00:00|00h00-07h30;23h30-23h59|18000|
|id2|2019-03-11 09:00:00|00h00-07h30;23h30-23h59|32400|
|id3|2019-03-11 10:30:00|00h00-07h30;23h30-23h59|37800|
+---+-------------------+-----------------------+-----+

2022-10-30 15:37:10.019 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:14): -> call(00h00-07h30;23h30-23h59, 18000)
2022-10-30 15:37:10.022 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:17): Processing range #0: 00h00-07h30
2022-10-30 15:37:10.022 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:25): Checking between 0 and 27000
2022-10-30 15:37:10.023 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:14): -> call(00h00-07h30;23h30-23h59, 32400)
2022-10-30 15:37:10.023 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:17): Processing range #0: 00h00-07h30
2022-10-30 15:37:10.023 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:25): Checking between 0 and 27000
2022-10-30 15:37:10.024 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:17): Processing range #1: 23h30-23h59
2022-10-30 15:37:10.024 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:25): Checking between 84600 and 86340
2022-10-30 15:37:10.024 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:14): -> call(00h00-07h30;23h30-23h59, 37800)
2022-10-30 15:37:10.024 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:17): Processing range #0: 00h00-07h30
2022-10-30 15:37:10.025 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:25): Checking between 0 and 27000
2022-10-30 15:37:10.025 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:17): Processing range #1: 23h30-23h59
2022-10-30 15:37:10.025 -DEBUG --- [           main] angeUdf.call(Lab14_32InRangeUdf.java:25): Checking between 84600 and 86340
+---+-------------------+-----------------------+-----+-------+
|id |time               |range                  |event|between|
+---+-------------------+-----------------------+-----+-------+
|id1|2019-03-11 05:00:00|00h00-07h30;23h30-23h59|18000|true   |
|id2|2019-03-11 09:00:00|00h00-07h30;23h30-23h59|32400|false  |
|id3|2019-03-11 10:30:00|00h00-07h30;23h30-23h59|37800|false  |
+---+-------------------+-----------------------+-----+-------+
 */