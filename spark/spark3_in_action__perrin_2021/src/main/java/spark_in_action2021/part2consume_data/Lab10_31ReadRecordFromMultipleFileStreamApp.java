package spark_in_action2021.part2consume_data;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark_in_action2021.streaming.lib.RecordWriterUtils;

/**
 * 1) Start RecordsInFilesGeneratorApp
 * 2) In parallel start RecordsInFilesGeneratorApp --output-directory C:\TEMP\streaming2\in
 * 3) In parallel start Lab10_31ReadRecordFromMultipleFileStreamApp
 * <p>
 * or
 * <p>
 * mvn install exec:java@generate-records-in-files-alt-dir
 * mvn install exec:java@generate-records-in-files
 */
public class Lab10_31ReadRecordFromMultipleFileStreamApp {
    private static transient Logger log = LoggerFactory.getLogger(Lab10_31ReadRecordFromMultipleFileStreamApp.class);

    public static void main(String[] args) {
        Lab10_31ReadRecordFromMultipleFileStreamApp app = new Lab10_31ReadRecordFromMultipleFileStreamApp();
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

        // Two directories
        String landingDirectoryStream1 = RecordWriterUtils.inputDirectory;
        String landingDirectoryStream2 = RecordWriterUtils.inputDirectory2;

        // Two streams
        Dataset<Row> dfStream1 = spark.readStream().format("csv")
                .schema(recordSchema)
                .load(landingDirectoryStream1);

        Dataset<Row> dfStream2 = spark.readStream().format("csv")
                .schema(recordSchema)
                .load(landingDirectoryStream2);

        // Each stream will be processed by the same writer
        StreamingQuery queryStream1 = dfStream1
                .writeStream()
                .outputMode(OutputMode.Append())
                .foreach(new AgeChecker(1))
                .start();

        StreamingQuery queryStream2 = dfStream2
                .writeStream()
                .outputMode(OutputMode.Append())
                .foreach(new AgeChecker(2))
                .start();

        // Loop through the records for 1 minute
        long startProcessing = System.currentTimeMillis();

        for (int i = 1; queryStream1.isActive() && queryStream2.isActive(); i++) {
            log.debug("iteration #{}", i);
            if (startProcessing + 60000 < System.currentTimeMillis()) {
                queryStream1.stop();
                queryStream2.stop();
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // Simply ignored
            }
        }
        log.debug("<- start()");
    }

    private static class AgeChecker extends ForeachWriter<Row> {
        private static final long serialVersionUID = 8383715100587612498L;
        private static Logger log = LoggerFactory.getLogger(AgeChecker.class);

        private int streamId = 0;

        public AgeChecker(int streamId) {
            this.streamId = streamId;
        }

        @Override
        public void close(Throwable arg0) {
        }

        @Override
        public boolean open(long partitionId, long epochId) {
            return true;
        }

        @Override
        public void process(Row r) {
            if (r.length() != 5) {
                return;
            }
            String type = "UNKNOWN";
            int age = r.getInt(3);
            if (age < 13) {
                type = "kid";
            } else if (age > 12 && age < 20) {
                type = "teen";
            } else if (age > 64) {
                type = "senior";
            }
            log.debug("On stream #{}: {} is a " + type + ", they are {} yrs old.",
                    streamId,
                    r.getString(0),
                    age);
        }
    }

}
