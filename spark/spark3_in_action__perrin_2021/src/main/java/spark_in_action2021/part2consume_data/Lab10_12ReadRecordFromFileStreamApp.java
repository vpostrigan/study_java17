package spark_in_action2021.part2consume_data;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark_in_action2021.streaming.lib.RecordWriterUtils;

public class Lab10_12ReadRecordFromFileStreamApp {
    private static Logger log = LoggerFactory.getLogger(Lab10_12ReadRecordFromFileStreamApp.class);

    public static void main(String[] args) {
        Lab10_12ReadRecordFromFileStreamApp app = new Lab10_12ReadRecordFromFileStreamApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start()");

        SparkSession spark = SparkSession.builder()
                .appName("Read records from a file stream")
                .master("local")
                .getOrCreate();

        // Specify the record that will be ingested.
        // Note that the schema much match the record coming from the generator
        // (or source)
        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");

        Dataset<Row> df = spark.readStream().format("csv")
                .schema(recordSchema)
                .load(RecordWriterUtils.inputDirectory);

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();
        log.debug("Query ready");

        try {
            query.awaitTermination(60000); // the query will stop in a minute
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end {}.", e.getMessage(), e);
        }

        // Executed only after a nice kill
        log.debug("Query status: {}", query.status());
        log.debug("<- start()");
    }

}
