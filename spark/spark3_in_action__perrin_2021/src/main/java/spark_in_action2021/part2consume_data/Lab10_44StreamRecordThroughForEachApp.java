package spark_in_action2021.part2consume_data;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
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
 * Analyzes the records on the stream and send each record to a debugger class.
 *
 * @author jgp
 */
public class Lab10_44StreamRecordThroughForEachApp {
    private static Logger log = LoggerFactory.getLogger(Lab10_44StreamRecordThroughForEachApp.class);

    public static void main(String[] args) {
        Lab10_44StreamRecordThroughForEachApp app = new Lab10_44StreamRecordThroughForEachApp();
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

        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");

        Dataset<Row> df = spark.readStream().format("csv")
                .schema(recordSchema)
                .csv(RecordWriterUtils.inputDirectory);

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Update())
                .foreach(new RecordLogDebugger())
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end {}.", e.getMessage(), e);
        }

        log.debug("<- start()");
    }


    /**
     * Very basic logger.
     *
     * @author jgp
     */
    private static class RecordLogDebugger extends ForeachWriter<Row> {
        private static final long serialVersionUID = 4137020658417523102L;
        private static Logger log = LoggerFactory.getLogger(RecordLogDebugger.class);
        private static int count = 0;

        /**
         * Closes the writer
         */
        @Override
        public void close(Throwable arg0) {
        }

        /**
         * Opens the writer
         */
        @Override
        public boolean open(long arg0, long arg1) {
            return true;
        }

        /**
         * Processes a row
         */
        @Override
        public void process(Row arg0) {
            count++;
            log.debug("Record #{} has {} column(s)", count, arg0.length());
            log.debug("First value: {}", arg0.get(0));
        }

    }

}
