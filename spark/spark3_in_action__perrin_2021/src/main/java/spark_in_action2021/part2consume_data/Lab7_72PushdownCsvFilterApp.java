package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.Logs;

/**
 * CSV ingestion in a dataframe.
 *
 * VN options -Xmx4g
 *
 * @author jgp
 */
public class Lab7_72PushdownCsvFilterApp {

    enum Mode {
        NO_FILTER, FILTER
    }

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab7_72PushdownCsvFilterApp app = new Lab7_72PushdownCsvFilterApp();
        app.buildBigCsvFile(allLogs);
        app.start(Mode.NO_FILTER, allLogs);
        app.start(Mode.FILTER, allLogs);

        System.out.println("[OUT]:\n" + allLogs.getSb());
    }

    private void buildBigCsvFile(Logs allLogs) {
        allLogs.outPrintln("Build big CSV file");
        SparkSession spark = SparkSession.builder()
                .appName("Pushdown CSV filter")
                .config("spark.sql.csv.filterPushdown.enabled", false)
                .master("local[*]")
                .getOrCreate();

        allLogs.outPrintln("Read initial CSV");
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/chapter7/VideoGameSales/vgsales.csv");

        allLogs.outPrintln("Increasing number of record 10x");
        for (int i = 0; i < 10; i++) {
            allLogs.outPrintln("Increasing number of record, step #" + i);
            df = df.union(df);
        }

        allLogs.outPrintln("Saving big CSV file");
        String file = allLogs.getFolder() + "csv";
        df = df.coalesce(1);
        df.write().format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save(file);
        spark.stop();
        allLogs.outPrintln("Big CSV file ready. " + file);
    }

    private void start(Mode filter, Logs allLogs) {
        SparkSession spark = null;
        Dataset<Row> df = null;

        long t0 = System.currentTimeMillis();
        switch (filter) {
            case NO_FILTER:
                spark = SparkSession.builder()
                        .appName("Pushdown CSV filter")
                        .config("spark.sql.csv.filterPushdown.enabled", false)
                        .master("local[*]")
                        .getOrCreate();
                allLogs.outPrintln("Using Apache Spark v" + spark.version());
                // ingestionWithoutFilter
                df = spark.read().format("csv")
                        .option("header", true)
                        .option("inferSchema", true)
                        .load(allLogs.getFolder() + "csv/*.csv");
                break;

            case FILTER:
                spark = SparkSession.builder()
                        .appName("Pushdown CSV filter")
                        .config("spark.sql.csv.filterPushdown.enabled", true)
                        .master("local[*]")
                        .getOrCreate();
                allLogs.outPrintln("Using Apache Spark v" + spark.version());
                // ingestionWithFilter
                df = spark.read().format("csv")
                        .option("header", true)
                        .option("inferSchema", true)
                        .load(allLogs.getFolder() + "csv/*.csv")
                        .filter("Platform = 'Wii'");
                break;
        }

        df.explain("formatted");

        String file = allLogs.getFolder() + filter;
        df.write().format("parquet")
                .mode(SaveMode.Overwrite)
                .save(file);
        long t1 = System.currentTimeMillis();
        spark.stop();
        allLogs.outPrintln("Operation " + filter + " tool " + (t1 - t0) + " ms. File: " + file);
    }
/**
 Operation FILTER tool 33924 ms. File: C:\Users\admin\AppData\Local\Temp\Lab7_72PushdownCsvFilterApp/FILTER
 [OUT]:
 Build big CSV file
 Read initial CSV
 Increasing number of record 10x
 Increasing number of record, step #0
 Increasing number of record, step #1
 Increasing number of record, step #2
 Increasing number of record, step #3
 Increasing number of record, step #4
 Increasing number of record, step #5
 Increasing number of record, step #6
 Increasing number of record, step #7
 Increasing number of record, step #8
 Increasing number of record, step #9
 Saving big CSV file
 Big CSV file ready. C:\Users\admin\AppData\Local\Temp\Lab7_72PushdownCsvFilterApp/csv
 Using Apache Spark v3.1.3
 Operation NO_FILTER tool 50674 ms. File: C:\Users\admin\AppData\Local\Temp\Lab7_72PushdownCsvFilterApp/NO_FILTER
 Using Apache Spark v3.1.3
 Operation FILTER tool 33924 ms. File: C:\Users\admin\AppData\Local\Temp\Lab7_72PushdownCsvFilterApp/FILTER

 Process finished with exit code 0
 */
}