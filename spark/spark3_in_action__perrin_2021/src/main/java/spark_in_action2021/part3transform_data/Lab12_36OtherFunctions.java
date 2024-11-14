package spark_in_action2021.part3transform_data;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;

public class Lab12_36OtherFunctions {

    public static void main(String[] args) {
        Lab12_36OtherFunctions app = new Lab12_36OtherFunctions();
        app.startEpochConversionApp();
        app.startEpochTimestampConversionApp();
        app.startExprApp();
        app.startKeepingOrderApp();
        app.startSelfJoinAndSelectApp();
        app.startSelfJoinApp();
    }

    /**
     * Use of from_unixtime().
     */
    private void startEpochConversionApp() {
        SparkSession spark = SparkSession.builder()
                .appName("expr()")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("event", DataTypes.IntegerType, false),
                DataTypes.createStructField("ts", DataTypes.StringType, false)});

        // Building a df with a sequence of chronological timestamps
        List<Row> rows = new ArrayList<>();
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 1000; i++) {
            rows.add(RowFactory.create(i, String.valueOf(now)));
            now += new Random().nextInt(3) + 1;
        }
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show();
/*
+-----+----------+
|event|        ts|
+-----+----------+
|    0|1662220286|
|    1|1662220287|
|    2|1662220289|
|    3|1662220290|
 */

        // Turning the timestamps to dates
        df = df.withColumn("date", from_unixtime(col("ts")));
        df.show();
/*
+-----+----------+-------------------+
|event|        ts|               date|
+-----+----------+-------------------+
|    0|1662220286|2022-09-03 18:51:26|
|    1|1662220287|2022-09-03 18:51:27|
|    2|1662220289|2022-09-03 18:51:29|
|    3|1662220290|2022-09-03 18:51:30|
|    4|1662220293|2022-09-03 18:51:33|
 */

        // Collecting the result and printing ou
        List<Row> timeRows = df.collectAsList();
        for (Row r : timeRows) {
            System.out.printf("[%d] : %s (%s)\n", r.getInt(0), r.getString(1), r.getString(2));
        }
/*
[0] : 1662220286 (2022-09-03 18:51:26)
[1] : 1662220287 (2022-09-03 18:51:27)
[2] : 1662220289 (2022-09-03 18:51:29)
[3] : 1662220290 (2022-09-03 18:51:30)
[4] : 1662220293 (2022-09-03 18:51:33)
 */
    }

    /**
     * Use of from_unixtime() and unix_timestamp().
     */
    private void startEpochTimestampConversionApp() {
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
        for (int i = 0; i < 1000; i++) {
            rows.add(RowFactory.create(i, String.valueOf(now)));
            now += new Random().nextInt(3) + 1;
        }
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show();
        df.printSchema();
/*
+-----+-----------+
|event|original_ts|
+-----+-----------+
|    0| 1662220290|
|    1| 1662220292|
|    2| 1662220294|
|    3| 1662220295|
|    4| 1662220296|
|    5| 1662220297|

root
 |-- event: integer (nullable = false)
 |-- original_ts: string (nullable = false)
 */

        // Turning the timestamps to Timestamp datatype
        df = df.withColumn("date",
                from_unixtime(col("original_ts")).cast(DataTypes.TimestampType));
        df.show();
        df.printSchema();
/*
+-----+-----------+-------------------+
|event|original_ts|               date|
+-----+-----------+-------------------+
|    0| 1662220290|2022-09-03 18:51:30|
|    1| 1662220292|2022-09-03 18:51:32|
|    2| 1662220294|2022-09-03 18:51:34|
|    3| 1662220295|2022-09-03 18:51:35|
|    4| 1662220296|2022-09-03 18:51:36|

root
 |-- event: integer (nullable = false)
 |-- original_ts: string (nullable = false)
 |-- date: timestamp (nullable = true)
 */
        // Turning back the timestamps to epoch
        df = df.withColumn("epoch",
                unix_timestamp(col("date")));
        df.show();
        df.printSchema();
/*
+-----+-----------+-------------------+----------+
|event|original_ts|               date|     epoch|
+-----+-----------+-------------------+----------+
|    0| 1662220290|2022-09-03 18:51:30|1662220290|
|    1| 1662220292|2022-09-03 18:51:32|1662220292|
|    2| 1662220294|2022-09-03 18:51:34|1662220294|
|    3| 1662220295|2022-09-03 18:51:35|1662220295|
|    4| 1662220296|2022-09-03 18:51:36|1662220296|

root
 |-- event: integer (nullable = false)
 |-- original_ts: string (nullable = false)
 |-- date: timestamp (nullable = true)
 |-- epoch: long (nullable = true)
 */

        // Collecting the result and printing ou
        List<Row> timeRows = df.collectAsList();
        for (Row r : timeRows) {
            System.out.printf("[%d] : %s (%s)\n", r.getInt(0), r.getAs("epoch"), r.getAs("date"));
        }
/*
[0] : 1662220290 (2022-09-03 18:51:30.0)
[1] : 1662220292 (2022-09-03 18:51:32.0)
[2] : 1662220294 (2022-09-03 18:51:34.0)
[3] : 1662220295 (2022-09-03 18:51:35.0)
[4] : 1662220296 (2022-09-03 18:51:36.0)
 */
    }

    /**
     * Use of expr().
     */
    private void startExprApp() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("expr()")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("start", DataTypes.IntegerType, false),
                DataTypes.createStructField("end", DataTypes.IntegerType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("bla", 10, 30));
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show();
/*
+-----+-----+---+
|title|start|end|
+-----+-----+---+
|  bla|   10| 30|
+-----+-----+---+
 */
        df = df.withColumn("time_spent", expr("end - start"))
                .drop("start")
                .drop("end");
        df.show();
/*
+-----+----------+
|title|time_spent|
+-----+----------+
|  bla|        20|
+-----+----------+
 */
    }

    /**
     * Keeping the order of rows during transformations.
     */
    private void startKeepingOrderApp() {
        SparkSession spark = SparkSession.builder()
                .appName("Splitting a dataframe to collect it")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("col1", DataTypes.IntegerType, false),
                DataTypes.createStructField("col2", DataTypes.StringType, false),
                DataTypes.createStructField("sum", DataTypes.DoubleType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, "a", 3555204326.27));
        rows.add(RowFactory.create(4, "b", 22273491.72));
        rows.add(RowFactory.create(5, "c", 219175.0));
        rows.add(RowFactory.create(3, "a", 219175.0));
        rows.add(RowFactory.create(2, "c", 75341433.37));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show();
/*
+----+----+---------------+
|col1|col2|            sum|
+----+----+---------------+
|   1|   a|3.55520432627E9|
|   4|   b|  2.227349172E7|
|   5|   c|       219175.0|
|   3|   a|       219175.0|
|   2|   c|  7.534143337E7|
+----+----+---------------+
 */
        df = df.withColumn("__idx", monotonically_increasing_id());
        df.show();
/*
+----+----+---------------+-----+
|col1|col2|            sum|__idx|
+----+----+---------------+-----+
|   1|   a|3.55520432627E9|    0|
|   4|   b|  2.227349172E7|    1|
|   5|   c|       219175.0|    2|
|   3|   a|       219175.0|    3|
|   2|   c|  7.534143337E7|    4|
+----+----+---------------+-----+
 */
        df = df.dropDuplicates("col2")
                .orderBy("__idx")
                .drop("__idx");
        df.show();
/*
+----+----+---------------+
|col1|col2|            sum|
+----+----+---------------+
|   1|   a|3.55520432627E9|
|   4|   b|  2.227349172E7|
|   5|   c|       219175.0|
+----+----+---------------+
 */
    }

    /**
     * Self join.
     */
    private void startSelfJoinAndSelectApp() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Self join")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src", DataTypes.StringType, false),
                DataTypes.createStructField("predicate", DataTypes.StringType, false),
                DataTypes.createStructField("dst", DataTypes.StringType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("a", "r1", ":b1"));
        rows.add(RowFactory.create("a", "r2", "k"));
        rows.add(RowFactory.create("b1", "r3", ":b4"));
        rows.add(RowFactory.create("b1", "r10", "d"));
        rows.add(RowFactory.create(":b4", "r4", "f"));
        rows.add(RowFactory.create(":b4", "r5", ":b5"));
        rows.add(RowFactory.create(":b5", "r9", "t"));
        rows.add(RowFactory.create(":b5", "r10", "e"));

        Dataset<Row> inputDf = spark.createDataFrame(rows, schema);
        inputDf.show(false);
/*
+---+---------+---+
|src|predicate|dst|
+---+---------+---+
|a  |r1       |:b1|
|a  |r2       |k  |
|b1 |r3       |:b4|
|b1 |r10      |d  |
|:b4|r4       |f  |
|:b4|r5       |:b5|
|:b5|r9       |t  |
|:b5|r10      |e  |
+---+---------+---+
 */
        Dataset<Row> left = inputDf.withColumnRenamed("dst", "dst2");
        left.show();
/*
+---+---------+----+
|src|predicate|dst2|
+---+---------+----+
|  a|       r1| :b1|
|  a|       r2|   k|
| b1|       r3| :b4|
| b1|      r10|   d|
|:b4|       r4|   f|
|:b4|       r5| :b5|
|:b5|       r9|   t|
|:b5|      r10|   e|
+---+---------+----+
 */
        Dataset<Row> right = inputDf.withColumnRenamed("src", "dst2");
        right.show();
/*
+----+---------+---+
|dst2|predicate|dst|
+----+---------+---+
|   a|       r1|:b1|
|   a|       r2|  k|
|  b1|       r3|:b4|
|  b1|      r10|  d|
| :b4|       r4|  f|
| :b4|       r5|:b5|
| :b5|       r9|  t|
| :b5|      r10|  e|
+----+---------+---+
 */
        Dataset<Row> r = left.join(right,
                left.col("dst2").equalTo(right.col("dst2")));
        r.show();
/*
+---+---------+----+----+---------+---+
|src|predicate|dst2|dst2|predicate|dst|
+---+---------+----+----+---------+---+
| b1|       r3| :b4| :b4|       r5|:b5|
| b1|       r3| :b4| :b4|       r4|  f|
|:b4|       r5| :b5| :b5|      r10|  e|
|:b4|       r5| :b5| :b5|       r9|  t|
+---+---------+----+----+---------+---+
 */
        Dataset<Row> resultOption1Df = r.select(left.col("src"), r.col("dst"));
        resultOption1Df.show();
/*
+---+---+
|src|dst|
+---+---+
| b1|:b5|
| b1|  f|
|:b4|  e|
|:b4|  t|
+---+---+
 */
        Dataset<Row> resultOption2Df = r.select(col("src"), col("dst"));
        resultOption2Df.show();
/*
+---+---+
|src|dst|
+---+---+
| b1|:b5|
| b1|  f|
|:b4|  e|
|:b4|  t|
+---+---+
 */
        Dataset<Row> resultOption3Df = r.select("src", "dst");
        resultOption3Df.show();
/*
+---+---+
|src|dst|
+---+---+
| b1|:b5|
| b1|  f|
|:b4|  e|
|:b4|  t|
+---+---+
 */
    }

    /**
     * Self join.
     */
    private void startSelfJoinApp() {
        SparkSession spark = SparkSession.builder()
                .appName("Self join")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("tid", DataTypes.IntegerType, false),
                DataTypes.createStructField("acct", DataTypes.IntegerType, false),
                DataTypes.createStructField("bssn", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 123, 111, "Peter"));
        rows.add(RowFactory.create(2, 123, 222, "Paul"));
        rows.add(RowFactory.create(3, 456, 333, "John"));
        rows.add(RowFactory.create(4, 567, 444, "Casey"));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show(false);
/*
+---+----+----+-----+
|tid|acct|bssn|name |
+---+----+----+-----+
|1  |123 |111 |Peter|
|2  |123 |222 |Paul |
|3  |456 |333 |John |
|4  |567 |444 |Casey|
+---+----+----+-----+
 */
        Dataset<Row> rightDf = df
                .withColumnRenamed("acct", "acct2")
                .withColumnRenamed("bssn", "bssn2")
                .withColumnRenamed("name", "name2")
                .drop("tid");

        Dataset<Row> joinedDf = df.join(rightDf,
                df.col("acct").equalTo(rightDf.col("acct2")),
                "leftsemi")
                .drop(rightDf.col("acct2"))
                .drop(rightDf.col("name2"))
                .drop(rightDf.col("bssn2"));
        joinedDf.show(false);
/*
+---+----+----+-----+
|tid|acct|bssn|name |
+---+----+----+-----+
|1  |123 |111 |Peter|
|2  |123 |222 |Paul |
|3  |456 |333 |John |
|4  |567 |444 |Casey|
+---+----+----+-----+
 */
        Dataset<Row> listDf = joinedDf
                .groupBy(joinedDf.col("acct"))
                .agg(collect_list("bssn"), collect_list("name"));
        listDf.show(false);
/*
+----+------------------+------------------+
|acct|collect_list(bssn)|collect_list(name)|
+----+------------------+------------------+
|456 |[333]             |[John]            |
|567 |[444]             |[Casey]           |
|123 |[111, 222]        |[Peter, Paul]     |
+----+------------------+------------------+
 */
        Dataset<Row> setDf = joinedDf
                .groupBy(joinedDf.col("acct"))
                .agg(collect_set("bssn"), collect_set("name"));
        setDf.show(false);
/*
+----+-----------------+-----------------+
|acct|collect_set(bssn)|collect_set(name)|
+----+-----------------+-----------------+
|456 |[333]            |[John]           |
|567 |[444]            |[Casey]          |
|123 |[222, 111]       |[Paul, Peter]    |
+----+-----------------+-----------------+
 */
    }

}
