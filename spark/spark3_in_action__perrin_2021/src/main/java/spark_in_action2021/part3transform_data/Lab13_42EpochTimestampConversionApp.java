package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.unix_timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark_in_action2021.Logs;

/**
 * Use of from_unixtime() and unix_timestamp().
 *
 * @author jgp
 */
public class Lab13_42EpochTimestampConversionApp {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab13_42EpochTimestampConversionApp app = new Lab13_42EpochTimestampConversionApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        SparkSession spark = SparkSession.builder()
                .appName("expr()")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("event", DataTypes.IntegerType, false),
                DataTypes.createStructField("original_ts", DataTypes.StringType, false)});

        // Building a df with a sequence of chronological timestamps
        List<Row> rows = new ArrayList<>();
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 50; i++) {
            rows.add(RowFactory.create(i, String.valueOf(now)));
            now += new Random().nextInt(3) + 1;
        }
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        allLogs.showAndSaveToCsv("[1] data csv", df, true);

        // Turning the timestamps to Timestamp datatype
        df = df.withColumn("date",
                from_unixtime(col("original_ts")).cast(DataTypes.TimestampType));
        allLogs.showAndSaveToCsv("[2] turning timestamps", df, true);

        // Turning back the timestamps to epoch
        df = df.withColumn("epoch",
                unix_timestamp(col("date")));
        allLogs.showAndSaveToCsv("[3] turning back", df, true);

        // Collecting the result and printing ou
        List<Row> timeRows = df.collectAsList();
        for (Row r : timeRows) {
            System.out.printf("[%d] : %s (%s)\n",
                    r.getInt(0),
                    r.getAs("epoch"),
                    r.getAs("date"));
        }
    }
/*
16:31:54.090: [1] data csv
+-----+-----------+
|event|original_ts|
+-----+-----------+
|0    |1666531911 |
|1    |1666531914 |
|2    |1666531915 |
|3    |1666531916 |
|4    |1666531918 |
+-----+-----------+
only showing top 5 rows

root
 |-- event: integer (nullable = false)
 |-- original_ts: string (nullable = false)

16:31:56.402: [1] data csv has 50. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_42EpochTimestampConversionApp/1_csv
// //
16:31:56.781: [2] turning timestamps
+-----+-----------+-------------------+
|event|original_ts|date               |
+-----+-----------+-------------------+
|0    |1666531911 |2022-10-23 16:31:51|
|1    |1666531914 |2022-10-23 16:31:54|
|2    |1666531915 |2022-10-23 16:31:55|
|3    |1666531916 |2022-10-23 16:31:56|
|4    |1666531918 |2022-10-23 16:31:58|
+-----+-----------+-------------------+
only showing top 5 rows

root
 |-- event: integer (nullable = false)
 |-- original_ts: string (nullable = false)
 |-- date: timestamp (nullable = true)

16:31:57.148: [2] turning timestamps has 50. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_42EpochTimestampConversionApp/2_csv
// //
16:31:57.267: [3] turning back
+-----+-----------+-------------------+----------+
|event|original_ts|date               |epoch     |
+-----+-----------+-------------------+----------+
|0    |1666531911 |2022-10-23 16:31:51|1666531911|
|1    |1666531914 |2022-10-23 16:31:54|1666531914|
|2    |1666531915 |2022-10-23 16:31:55|1666531915|
|3    |1666531916 |2022-10-23 16:31:56|1666531916|
|4    |1666531918 |2022-10-23 16:31:58|1666531918|
+-----+-----------+-------------------+----------+
only showing top 5 rows

root
 |-- event: integer (nullable = false)
 |-- original_ts: string (nullable = false)
 |-- date: timestamp (nullable = true)
 |-- epoch: long (nullable = true)

16:31:57.521: [3] turning back has 50. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_42EpochTimestampConversionApp/3_csv
// //
[0] : 1666531911 (2022-10-23 16:31:51.0)
[1] : 1666531914 (2022-10-23 16:31:54.0)
[2] : 1666531915 (2022-10-23 16:31:55.0)
[3] : 1666531916 (2022-10-23 16:31:56.0)
[4] : 1666531918 (2022-10-23 16:31:58.0)
[5] : 1666531919 (2022-10-23 16:31:59.0)
[6] : 1666531922 (2022-10-23 16:32:02.0)
[7] : 1666531923 (2022-10-23 16:32:03.0)
[8] : 1666531925 (2022-10-23 16:32:05.0)
[9] : 1666531928 (2022-10-23 16:32:08.0)
[10] : 1666531929 (2022-10-23 16:32:09.0)
[11] : 1666531931 (2022-10-23 16:32:11.0)
[12] : 1666531934 (2022-10-23 16:32:14.0)
[13] : 1666531936 (2022-10-23 16:32:16.0)
[14] : 1666531937 (2022-10-23 16:32:17.0)
[15] : 1666531940 (2022-10-23 16:32:20.0)
[16] : 1666531943 (2022-10-23 16:32:23.0)
[17] : 1666531945 (2022-10-23 16:32:25.0)
[18] : 1666531946 (2022-10-23 16:32:26.0)
[19] : 1666531949 (2022-10-23 16:32:29.0)
[20] : 1666531952 (2022-10-23 16:32:32.0)
[21] : 1666531955 (2022-10-23 16:32:35.0)
[22] : 1666531956 (2022-10-23 16:32:36.0)
[23] : 1666531957 (2022-10-23 16:32:37.0)
[24] : 1666531958 (2022-10-23 16:32:38.0)
[25] : 1666531959 (2022-10-23 16:32:39.0)
[26] : 1666531960 (2022-10-23 16:32:40.0)
[27] : 1666531962 (2022-10-23 16:32:42.0)
[28] : 1666531963 (2022-10-23 16:32:43.0)
[29] : 1666531966 (2022-10-23 16:32:46.0)
[30] : 1666531968 (2022-10-23 16:32:48.0)
[31] : 1666531970 (2022-10-23 16:32:50.0)
[32] : 1666531971 (2022-10-23 16:32:51.0)
[33] : 1666531974 (2022-10-23 16:32:54.0)
[34] : 1666531977 (2022-10-23 16:32:57.0)
[35] : 1666531980 (2022-10-23 16:33:00.0)
[36] : 1666531981 (2022-10-23 16:33:01.0)
[37] : 1666531983 (2022-10-23 16:33:03.0)
[38] : 1666531986 (2022-10-23 16:33:06.0)
[39] : 1666531987 (2022-10-23 16:33:07.0)
[40] : 1666531989 (2022-10-23 16:33:09.0)
[41] : 1666531990 (2022-10-23 16:33:10.0)
[42] : 1666531993 (2022-10-23 16:33:13.0)
[43] : 1666531995 (2022-10-23 16:33:15.0)
[44] : 1666531997 (2022-10-23 16:33:17.0)
[45] : 1666531998 (2022-10-23 16:33:18.0)
[46] : 1666532000 (2022-10-23 16:33:20.0)
[47] : 1666532001 (2022-10-23 16:33:21.0)
[48] : 1666532002 (2022-10-23 16:33:22.0)
[49] : 1666532005 (2022-10-23 16:33:25.0)
 */
}
