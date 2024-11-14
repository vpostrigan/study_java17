package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Avro ingestion in a dataframe.
 * <p>
 * Source of file: Apache Avro project -
 * https://github.com/apache/orc/tree/master/examples
 *
 * @author jgp
 */
public class Lab7_61AvroToDataframeApp {

    public static void main(String[] args) {
        Lab7_61AvroToDataframeApp app = new Lab7_61AvroToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Avro to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads an Avro file, stores it in a dataframe
        Dataset<Row> df = spark.read().format("avro")
                .load("data/chapter7/weather.avro");

        // Shows at most 10 rows from the dataframe
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
/**
 +------------+-------------+----+
 |     station|         time|temp|
 +------------+-------------+----+
 |011990-99999|-619524000000|   0|
 |011990-99999|-619506000000|  22|
 |011990-99999|-619484400000| -11|
 |012650-99999|-655531200000| 111|
 |012650-99999|-655509600000|  78|
 +------------+-------------+----+

 root
 |-- station: string (nullable = true)
 |-- time: long (nullable = true)
 |-- temp: integer (nullable = true)
 */
}