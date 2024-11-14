package spark_in_action2021.part1theory;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Compute Pi on a cluster.
 *
 * 1) Start cluster
 * $ cd /opt/apache-spark/sbin
 * $ ./start-master.sh
 * $ ./start-slave.sh spark://un:7077  // un - one
 * $ ./start-slave.sh spark://deux:7077  // deux - two
 *
 * windows
 * $ D:\program_files\spark-3.1.3-bin-hadoop3.2\bin>spark-class org.apache.spark.deploy.master.Master
 * $ D:\program_files\spark-3.1.3-bin-hadoop3.2\bin>spark-class org.apache.spark.deploy.worker.Worker spark://10.254.15.99:7077
 *
 * 2) Run App. It will connect to cluster and run task
 */
public class Lab5_21PiComputeClusterApp implements Serializable {
    private static long counter = 0;

    public static void main(String[] args) {
        Lab5_21PiComputeClusterApp app = new Lab5_21PiComputeClusterApp();
        app.start(10);
    }

    private void start(int slices) {
        int numberOfThrows = 100_000 * slices;

        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");

        // [1]
        long t0 = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder()
                .appName("JavaSparkPi on a cluster")
                .master("spark://10.254.15.99:7077") // spark://un:7077
                .config("spark.executor.memory", "4g")
                // Uncomment the next block if you want to run your application from the IDE
                // - note that you will have to deploy the jar first to *every* worker.
                // Spark can share a jar from which it is launched
                // - either via spark-submit or via a direct connection, but if you
                // run this application from the IDE, it will not know what to do.
                /*
                 * .config("spark.jars",
                 * C:\Users\admin\.m2\repository\com\spark3\spark3\0.0.1-SNAPSHOT\spark3-0.0.1-SNAPSHOT.jar
                 * "/home/jgp/.m2/repository/net/jgp/books/sparkWithJava-chapter05/1.0.0-SNAPSHOT/sparkWithJava-chapter05-1.0.0-SNAPSHOT.jar")
                 */
                .getOrCreate();
        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");

        // [2]
        List<Integer> l = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            l.add(i);
        }
        Dataset<Row> incrementalDf = spark
                .createDataset(l, Encoders.INT())
                .toDF();
        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

        // [3] отображение
        Dataset<Integer> dartsDs = incrementalDf.map(
                (MapFunction<Row, Integer>) r -> {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    counter++;
                    if (counter % 100000 == 0) {
                        System.out.println("" + counter + " darts thrown so far");
                    }
                    return (x * x + y * y <= 1) ? 1 : 0;
                }
                , Encoders.INT());
        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        // [4] свертка
        Integer dartsInCircle = dartsDs.reduce((ReduceFunction<Integer>) (x, y) -> {
            return x + y;
        });
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");

        // [5]
        System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);

        spark.stop();
    }

}
