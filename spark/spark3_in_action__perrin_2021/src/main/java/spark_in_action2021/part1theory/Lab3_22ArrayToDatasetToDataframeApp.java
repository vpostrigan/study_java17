package spark_in_action2021.part1theory;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Converts an array to a Dataframe via a Dataset
 *
 * @author jgp
 */
public class Lab3_22ArrayToDatasetToDataframeApp {

    public static void main(String[] args) {
        Lab3_22ArrayToDatasetToDataframeApp app = new Lab3_22ArrayToDatasetToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to dataframe")
                .master("local")
                .getOrCreate();

        List<String> data = Arrays.asList("Jean", "Liz", "Pierre", "Lauric");
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();
        /**
         * +------+
         * | value|
         * +------+
         * |  Jean|
         * |   Liz|
         * |Pierre|
         * |Lauric|
         * +------+
         *
         * root
         *  |-- value: string (nullable = true)
         */

        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();
        /**
         * +------+
         * | value|
         * +------+
         * |  Jean|
         * |   Liz|
         * |Pierre|
         * |Lauric|
         * +------+
         *
         * root
         *  |-- value: string (nullable = true)
         */
    }

}