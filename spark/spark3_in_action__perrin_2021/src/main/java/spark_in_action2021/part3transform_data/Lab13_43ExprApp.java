package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.expr;

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
 * Use of expr().
 *
 * @author jgp
 */
public class Lab13_43ExprApp {

    public static void main(String[] args) {
        Lab13_43ExprApp app = new Lab13_43ExprApp();
        app.start();
    }

    private void start() {
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

        df = df.withColumn("time_spent", expr("end - start"))
                .drop("start")
                .drop("end");
        df.show();
    }
/*
+-----+-----+---+
|title|start|end|
+-----+-----+---+
|  bla|   10| 30|
+-----+-----+---+

+-----+----------+
|title|time_spent|
+-----+----------+
|  bla|        20|
+-----+----------+
 */
}
