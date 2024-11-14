package spark_in_action2021.part1theory;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * Converts an array to a Dataset of strings
 *
 * @author jgp
 */
public class Lab3_21ArrayToDatasetApp {

    public static void main(String[] args) {
        Lab3_21ArrayToDatasetApp app = new Lab3_21ArrayToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String>")
                .master("local")
                .getOrCreate();

        List<String> data = Arrays.asList("Jean", "Liz", "Pierre", "Lauric");
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();
    }

}