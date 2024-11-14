package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

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
 * Keeping the order of rows during transformations.
 *
 * @author jgp
 */
public class Lab13_44KeepingOrderApp {

    public static void main(String[] args) {
        Lab13_44KeepingOrderApp app = new Lab13_44KeepingOrderApp();
        app.start();
    }

    private void start() {
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

        df = df.withColumn("__idx", monotonically_increasing_id());
        df.show();

        df = df.dropDuplicates("col2")
                .orderBy("__idx")
                .drop("__idx");
        df.show();
    }
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

+----+----+---------------+-----+
|col1|col2|            sum|__idx|
+----+----+---------------+-----+
|   1|   a|3.55520432627E9|    0|
|   4|   b|  2.227349172E7|    1|
|   5|   c|       219175.0|    2|
|   3|   a|       219175.0|    3|
|   2|   c|  7.534143337E7|    4|
+----+----+---------------+-----+

+----+----+---------------+
|col1|col2|            sum|
+----+----+---------------+
|   1|   a|3.55520432627E9|
|   4|   b|  2.227349172E7|
|   5|   c|       219175.0|
+----+----+---------------+
 */
}
