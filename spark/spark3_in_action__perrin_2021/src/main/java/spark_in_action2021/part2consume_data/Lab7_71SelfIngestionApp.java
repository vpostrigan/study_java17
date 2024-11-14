package spark_in_action2021.part2consume_data;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Simple ingestion followed by map and reduce operations.
 *
 * @author jgp
 */
public class Lab7_71SelfIngestionApp {

    public static void main(String[] args) {
        Lab7_71SelfIngestionApp app = new Lab7_71SelfIngestionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Self ingestion")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df;
        { // createDataframe
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("i", DataTypes.IntegerType, false)});

            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9,
                    1, 2, 3, 4, 5, 6, 7, 8, 9);
            List<Row> rows = new ArrayList<>();
            for (int i : data) {
                rows.add(RowFactory.create(i));
            }

            df = spark.createDataFrame(rows, schema);
        }
        df.show(false);

        // //

        // map and reduce with getAs()
        // The following code does not work (yet) with Spark 3.0.0 (preview 1)
        int totalLines = df
                .map((MapFunction<Row, Integer>) row -> row.getAs("i"), Encoders.INT())
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        System.out.println(totalLines);
        assert 90 == totalLines;

        // map and reduce with getInt()
        // The following code does not work (yet) with Spark 3.0.0 (preview 1)
        totalLines = df
                .map((MapFunction<Row, Integer>) row -> row.getInt(0), Encoders.INT())
                .reduce((ReduceFunction<Integer>) (a, b) -> a + b);
        System.out.println(totalLines);
        assert 90 == totalLines;

        // SQL-like
        long totalLinesL = df.selectExpr("sum(*)").first().getLong(0);
        System.out.println(totalLinesL);
        assert 90 == totalLines;
    }

}