package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Text ingestion in a dataframe.
 * <p>
 * Source of file: Rome & Juliet (Shakespeare) -
 * http://www.gutenberg.org/cache/epub/1777/pg1777.txt
 *
 * @author jgp
 */
public class Lab7_51TextToDataframeApp {

    public static void main(String[] args) {
        Lab7_51TextToDataframeApp app = new Lab7_51TextToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Text to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("text")
                .load("data/chapter7/romeo-juliet-pg1777.txt");

        // Shows at most 10 rows from the dataframe
        df.show(10);
        df.printSchema();
    }
/**
 +--------------------+
 |               value|
 +--------------------+
 |                    |
 |This Etext file i...|
 |cooperation with ...|
 |Future and Shakes...|
 |Etexts that are N...|
 |                    |
 |*This Etext has c...|
 |                    |
 |<<THIS ELECTRONIC...|
 |SHAKESPEARE IS CO...|
 +--------------------+
 only showing top 10 rows

 root
 |-- value: string (nullable = true)

 */
}
