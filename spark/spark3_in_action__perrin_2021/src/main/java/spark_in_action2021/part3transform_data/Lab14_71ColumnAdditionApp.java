package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.array;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
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
public class Lab14_71ColumnAdditionApp {

    private static final int COL_COUNT = 8;

    public static void main(String[] args) {
        Lab14_71ColumnAdditionApp app = new Lab14_71ColumnAdditionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Column addition")
                .master("local[*]")
                .getOrCreate();
        spark
                .udf()
                .register("add", new Lab14_72ColumnAdditionUdf(), DataTypes.IntegerType);

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("c0", DataTypes.IntegerType, false),
                DataTypes.createStructField("c1", DataTypes.IntegerType, false),
                DataTypes.createStructField("c2", DataTypes.IntegerType, false),
                DataTypes.createStructField("c3", DataTypes.IntegerType, false),
                DataTypes.createStructField("c4", DataTypes.IntegerType, false),
                DataTypes.createStructField("c5", DataTypes.IntegerType, false),
                DataTypes.createStructField("c6", DataTypes.IntegerType, false),
                DataTypes.createStructField("c7", DataTypes.IntegerType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 2, 4, 8, 16, 32, 64, 128));
        rows.add(RowFactory.create(0, 0, 0, 0, 0, 0, 0, 0));
        rows.add(RowFactory.create(1, 1, 1, 1, 1, 1, 1, 1));

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        Column[] cols = new Column[COL_COUNT];
        for (int i = 0; i < COL_COUNT; i++) {
            cols[i] = df.col("c" + i);
        }
        Column col = array(cols);

        df = df
                .withColumn(
                        "sum",
                        callUDF("add", col));
        df.show(false);
    }

}
/*
+---+---+---+---+---+---+---+---+---+
|c0 |c1 |c2 |c3 |c4 |c5 |c6 |c7 |sum|
+---+---+---+---+---+---+---+---+---+
|1  |2  |4  |8  |16 |32 |64 |128|255|
|0  |0  |0  |0  |0  |0  |0  |0  |0  |
|1  |1  |1  |1  |1  |1  |1  |1  |8  |
+---+---+---+---+---+---+---+---+---+
 */