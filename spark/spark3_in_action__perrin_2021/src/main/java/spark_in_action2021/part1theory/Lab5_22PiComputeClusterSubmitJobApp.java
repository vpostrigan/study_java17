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
 * 1) start cluster
 * <p>
 * 2) submit jar and task
 * D:\program_files\spark-3.1.3-bin-hadoop3.2\bin>SET JAVA_HOME=C:\Progra~1\Java\jdk-11_amazon_corretto
 * D:\program_files\spark-3.1.3-bin-hadoop3.2\bin>SET SPARK_HOME=D:\program_files\spark-3.1.3-bin-hadoop3.2
 * D:\program_files\spark-3.1.3-bin-hadoop3.2\bin>spark-submit --class spark_in_action2021.Lab5_22PiComputeClusterSubmitJobApp --master "spark://10.254.15.99:7077" D:\workspace_study\study\spark3\target\spark3-0.0.1-SNAPSHOT-uber.jar
 */
public class Lab5_22PiComputeClusterSubmitJobApp implements Serializable {
    private static long counter = 0;

    public static void main(String[] args) {
        Lab5_22PiComputeClusterSubmitJobApp app = new Lab5_22PiComputeClusterSubmitJobApp();
        app.start(10);
    }

    private void start(int slices) {
        int numberOfThrows = 100_000 * slices;

        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");

        // [1]
        long t0 = System.currentTimeMillis();
        SparkSession spark = SparkSession
                .builder() // no master!
                .appName("JavaSparkPi on a cluster (via spark-submit)")
                .config("spark.executor.memory", "4g")
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
