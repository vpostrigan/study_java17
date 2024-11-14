package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.col;

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
public class Lab13_45SelfJoinAndSelectApp {

    public static void main(String[] args) {
        Lab13_45SelfJoinAndSelectApp app = new Lab13_45SelfJoinAndSelectApp();
        app.start();
    }

    private void start() {
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

}
