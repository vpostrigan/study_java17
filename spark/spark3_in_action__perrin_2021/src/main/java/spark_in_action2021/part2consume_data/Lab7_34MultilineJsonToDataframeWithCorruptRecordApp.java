package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Failing multiline ingestion JSON ingestion in a dataframe.
 * <p>
 * This example illustrates what happens when you forget the multiline
 * option and try to ingest a multiline JSON file.
 * <p>
 * Output is:
 *
 * <pre>
 * +--------------------+
 * |     _corrupt_record|
 * +--------------------+
 * |                 [ {|
 * |       "tag" : "A1",|
 * |  "geopoliticalar...|
 * +--------------------+
 * only showing top 3 rows
 *
 * root
 *  |-- _corrupt_record: string (nullable = true)
 * </pre>
 * <p>
 * The data comes from The Bureau of Consular Affairs of the US Department
 * of State. You can access their open data portal at
 * https://cadatacatalog.state.gov/.
 *
 * @author jgp
 */
public class Lab7_34MultilineJsonToDataframeWithCorruptRecordApp {

    public static void main(String[] args) {
        Lab7_34MultilineJsonToDataframeWithCorruptRecordApp app =
                new Lab7_34MultilineJsonToDataframeWithCorruptRecordApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe, without multiline option")
                .master("local")
                .getOrCreate();

        // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe,
        // without specifying the multiline option
        Dataset<Row> df = spark.read().format("json")
                .load("data/chapter7/countrytravelinfo.json");

        // Shows at most 3 rows from the dataframe
        df.show(3);
        df.printSchema();
    }

}