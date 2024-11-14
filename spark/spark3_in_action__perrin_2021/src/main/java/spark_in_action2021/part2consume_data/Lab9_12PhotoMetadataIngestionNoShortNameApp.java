package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.exif.utils.K;

/**
 * Ingest metadata from a directory containing photos, make them available as EXIF.
 *
 * @author jgp
 */
public class Lab9_12PhotoMetadataIngestionNoShortNameApp {

    public static void main(String[] args) {
        Lab9_12PhotoMetadataIngestionNoShortNameApp app =
                new Lab9_12PhotoMetadataIngestionNoShortNameApp();
        app.start();
    }

    private boolean start() {
        SparkSession spark = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local")
                .getOrCreate();

        // Import directory
        String importDirectory = "data/chapter9";

        // read the data
        Dataset<Row> df = spark.read()
                .format("spark_in_action2021.exif.ExifDirectoryDataSourceShortnameAdvertiser")
                .option(K.RECURSIVE, K.FALSE)
                .option(K.LIMIT, "100000")
                .option(K.EXTENSIONS, "jpg,jpeg")
                .load(importDirectory);

        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);

        return true;
    }

}