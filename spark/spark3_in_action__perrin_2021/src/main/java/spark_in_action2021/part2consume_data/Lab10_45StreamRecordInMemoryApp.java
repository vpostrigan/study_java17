package spark_in_action2021.part2consume_data;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark_in_action2021.streaming.lib.RecordWriterUtils;

/**
 * Analyzes the records on the stream and send each record to a debugger class.
 *
 * @author jgp
 */
public class Lab10_45StreamRecordInMemoryApp {
    private static Logger log = LoggerFactory.getLogger(Lab10_45StreamRecordInMemoryApp.class);

    public static void main(String[] args) {
        Lab10_45StreamRecordInMemoryApp app = new Lab10_45StreamRecordInMemoryApp();
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
                .outputMode(OutputMode.Append())
                .format("memory") // формат вывода - память
                .option("queryName", "people") // виртуальная таблица называется people
                .start();

        // Wait and process the incoming stream for the next minute
        Dataset<Row> queryInMemoryDf; // будет использоваться фрейм данных с получаемыми из потока данными
        int i = 0;
        long start = System.currentTimeMillis();
        while (query.isActive()) {
            queryInMemoryDf = spark.sql("SELECT * FROM people"); // Создание фрейма данных с содержимым запроса SQL
            i++;
            log.debug("Pass #{}, dataframe contains {} records", i, queryInMemoryDf.count());
            queryInMemoryDf.show();
            if (start + 60000 < System.currentTimeMillis()) {
                query.stop(); // когда время запроса превышает минуту, выполнение останавливается
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // Simply ignored
            }
        }

        log.debug("<- start()");
    }

}

