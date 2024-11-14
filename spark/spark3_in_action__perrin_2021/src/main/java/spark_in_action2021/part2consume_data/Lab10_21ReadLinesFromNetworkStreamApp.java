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

/**
 * 1) cd C:\Program Files (x86)\Nmap
 * $ ncat.exe -lk 1025
 * (-l работает в режиме прослушивания; -k устанавливать несколько соединений)
 *
 * Start Lab10_21ReadLinesFromNetworkStreamApp
 *
 * enter values in terminal '$ ncat.exe -lk 1025'
 */
public class Lab10_21ReadLinesFromNetworkStreamApp {
    private static Logger log = LoggerFactory.getLogger(Lab10_21ReadLinesFromNetworkStreamApp.class);

    public static void main(String[] args) {
        Lab10_21ReadLinesFromNetworkStreamApp app = new Lab10_21ReadLinesFromNetworkStreamApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start()");

        SparkSession spark = SparkSession.builder()
                .appName("Read lines over a network stream")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.readStream().format("socket")
                .option("host", "localhost")
                .option("port", 1025)
                .load();

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            log.error(
                    "Exception while waiting for query to end {}.",
                    e.getMessage(),
                    e);
        }

        // Executed only after a nice kill
        log.debug("Query status: {}", query.status());
        log.debug("<- start()");
    }

}
