package spark_in_action2021.part2consume_data;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark_in_action2021.streaming.lib.RecordWriterUtils;

/**
 * Reads a stream from a stream (files)
 * <p>
 * (exec:exec creates new VM and allows arguments for JVM)
 * <p>
 * 1) rename _log4j.properties on log4j.properties
 * <p>
 * 2) generate files
 * $ SET JAVA_HOME=C:\Progra~1\Java\jdk-11_amazon_corretto
 * $ SET JRE_HOME=C:\Progra~1\Java\jdk-11_amazon_corretto\jre
 * $ mvn clean install exec:java@generate-records-in-files
 * <p>
 * 3) Second console
 * $ SET JAVA_HOME=C:\Progra~1\Java\jdk-11_amazon_corretto
 * $ SET JRE_HOME=C:\Progra~1\Java\jdk-11_amazon_corretto\jre
 * $ mvn clean install exec:exec@lab200
 *
 * @author jgp
 */
public class Lab10_11ReadLinesFromFileStreamApp {
    private static final Logger log = LoggerFactory.getLogger(Lab10_11ReadLinesFromFileStreamApp.class);

    public static void main(String[] args) {
        Lab10_11ReadLinesFromFileStreamApp app = new Lab10_11ReadLinesFromFileStreamApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start()");

        SparkSession spark = SparkSession.builder()
                .appName("Read lines from a file stream")
                .master("local")
                .getOrCreate();
        log.debug("Spark session initiated");

        Dataset<Row> df = spark.readStream().format("text")
                .load(RecordWriterUtils.inputDirectory);
        log.debug("Dataframe read from stream");

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .option("truncate", false)
                .option("numRows", 3)
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
