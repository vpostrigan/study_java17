package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Self join.
 *
 * @author jgp
 */
public class Lab13_46SelfJoinApp {

    public static void main(String[] args) {
        Lab13_46SelfJoinApp app = new Lab13_46SelfJoinApp();
        app.start();
    }

    private void start() {
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

        Dataset<Row> joinedDf = df
                .join(
                        rightDf,
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
