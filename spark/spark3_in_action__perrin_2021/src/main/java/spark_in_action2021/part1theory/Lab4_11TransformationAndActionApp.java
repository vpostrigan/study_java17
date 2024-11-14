package spark_in_action2021.part1theory;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.Logs;

/**
 * https://github.com/jgperrin/net.jgp.books.spark.ch04
 *
 * shows that Spark is lazy
 * and can optimize operations and make them faster than smaller number of operations
 * like 'full_operations' takes smaller time than 'operations'
 */
public class Lab4_11TransformationAndActionApp {

    public static void main(String[] args) throws InterruptedException {
        Logs allLogs = new Logs();

        Lab4_11TransformationAndActionApp app = new Lab4_11TransformationAndActionApp();
        app.start("no_operations", allLogs);
        app.start("operations", allLogs);
        app.start("full_operations", allLogs);

        System.out.println("[OUT]:\n" + allLogs.getSb());
    }

    private void start(String mode, Logs allLogs) {
        long t0 = System.currentTimeMillis();

        // Step 1 - Creates a session on a local master
        try (SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")
                .master("local")
                .getOrCreate();) {
            long t1 = System.currentTimeMillis();
            allLogs.outPrintln("1. Creating a session ........... " + (t1 - t0) + " ms " + " mode: " + mode);

            // Step 2 - Reads a CSV file with header, stores it in a dataframe
            Dataset<Row> df = spark.read().format("csv")
                    .option("header", "true")
                    .load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
            Dataset<Row> initalDf = df;
            long t2 = System.currentTimeMillis();
            allLogs.outPrintln("2. Loading initial dataset ...... " + (t2 - t1) + " ms");

            // Step 3 - Build a bigger dataset
            for (int i = 0; i < 10; i++) {
                df = df.union(initalDf);
            }
            long t3 = System.currentTimeMillis();
            allLogs.outPrintln("3. Building full dataset ........ " + (t3 - t2) + " ms");

            // Step 4 - Cleanup. preparation
            df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
            df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
            long t4 = System.currentTimeMillis();
            allLogs.outPrintln("4. Rename   ..................... " + (t4 - t3) + " ms");

            // Step 5 - Transformation
            if (mode.compareToIgnoreCase("no_operations") != 0) {
                df = df
                        .withColumn("avg", expr("(lcl+ucl)/2"))
                        .withColumn("lcl2", df.col("lcl"))
                        .withColumn("ucl2", df.col("ucl"));
                if (mode.compareToIgnoreCase("full_operations") == 0) {
                    df = df
                            .drop(df.col("avg"))
                            .drop(df.col("lcl2"))
                            .drop(df.col("ucl2"));
                }
            }
            long t5 = System.currentTimeMillis();
            allLogs.outPrintln("5. Transformations  ............. " + (t5 - t4) + " ms");

            // Step 6 - Action
            df.collect();
            long t6 = System.currentTimeMillis();
            allLogs.outPrintln("6. Final action ................. " + (t6 - t5) + " ms");

            allLogs.outPrintln("# of records .................... " + df.count());
        }
    }

}