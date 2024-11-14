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

/**
 * Saves the record in the stream in a parquet file.
 *
 * @author jgp
 */
public class Lab10_43StreamRecordOutputKafkaApp {
    private static Logger log = LoggerFactory.getLogger(Lab10_43StreamRecordOutputKafkaApp.class);

    public static void main(String[] args) {
        Lab10_43StreamRecordOutputKafkaApp app = new Lab10_43StreamRecordOutputKafkaApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start()");

        SparkSession spark = SparkSession.builder()
                .appName("Read lines over a file stream")
                .master("local")
                .getOrCreate();

        // The record structure must match the structure of your generated record
        // (or your real record if you are not using generated records)
        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");

        // Reading the record is always the same
        Dataset<Row> df = spark.readStream().format("csv")
                .schema(recordSchema)
                .csv(RecordWriterUtils.inputDirectory);

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("kafka") // Format is Apache Kafka
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("topic", "updates")
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end {}.", e.getMessage(), e);
        }

        log.debug("<- start()");
    }

}
