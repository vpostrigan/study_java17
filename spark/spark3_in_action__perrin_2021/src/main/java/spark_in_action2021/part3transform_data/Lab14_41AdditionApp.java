package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.callUDF;
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
 * Additions via UDF.
 *
 * @author jgp
 */
public class Lab14_41AdditionApp {

    public static void main(String[] args) {
        Lab14_41AdditionApp app = new Lab14_41AdditionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Addition")
                .master("local[*]")
                .getOrCreate();
        spark
                .udf()
                .register("add_int", new Lab14_41IntegerAdditionUdf(), DataTypes.IntegerType);
        spark
                .udf()
                .register("add_string", new Lab14_41StringAdditionUdf(), DataTypes.StringType);

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("fname", DataTypes.StringType, false),
                DataTypes.createStructField("lname", DataTypes.StringType, false),
                DataTypes.createStructField("score1", DataTypes.IntegerType, false),
                DataTypes.createStructField("score2", DataTypes.IntegerType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("Jean-Georges", "Perrin", 123, 456));
        rows.add(RowFactory.create("Jacek", "Laskowski", 147, 758));
        rows.add(RowFactory.create("Holden", "Karau", 258, 369));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show(false);

        df = df
                .withColumn(
                        "concat",
                        callUDF("add_string", col("fname"), col("lname")));
        df.show(false);

        df = df
                .withColumn(
                        "score",
                        callUDF("add_int", col("score1"), col("score2")));
        df.show(false);
    }

}
/*
+------------+---------+------+------+
|fname       |lname    |score1|score2|
+------------+---------+------+------+
|Jean-Georges|Perrin   |123   |456   |
|Jacek       |Laskowski|147   |758   |
|Holden      |Karau    |258   |369   |
+------------+---------+------+------+

+------------+---------+------+------+------------------+
|fname       |lname    |score1|score2|concat            |
+------------+---------+------+------+------------------+
|Jean-Georges|Perrin   |123   |456   |Jean-GeorgesPerrin|
|Jacek       |Laskowski|147   |758   |JacekLaskowski    |
|Holden      |Karau    |258   |369   |HoldenKarau       |
+------------+---------+------+------+------------------+

+------------+---------+------+------+------------------+-----+
|fname       |lname    |score1|score2|concat            |score|
+------------+---------+------+------+------------------+-----+
|Jean-Georges|Perrin   |123   |456   |Jean-GeorgesPerrin|579  |
|Jacek       |Laskowski|147   |758   |JacekLaskowski    |905  |
|Holden      |Karau    |258   |369   |HoldenKarau       |627  |
+------------+---------+------+------+------------------+-----+
 */