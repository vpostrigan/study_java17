package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ORC ingestion in a dataframe.
 * <p>
 * Source of file: Apache ORC project -
 * https://github.com/apache/orc/tree/master/examples
 *
 * @author jgp
 */
public class Lab7_62OrcToDataframeApp {

    public static void main(String[] args) {
        Lab7_62OrcToDataframeApp app = new Lab7_62OrcToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("ORC to Dataframe")
                .config("spark.sql.orc.impl", "native")
                .master("local")
                .getOrCreate();

        // Reads an ORC file, stores it in a dataframe
        Dataset<Row> df = spark.read().format("orc")
                .load("data/chapter7/demo-11-zlib.orc");

        // Shows at most 10 rows from the dataframe
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
/**
 +-----+-----+-----+-------+-----+-----+-----+-----+-----+
 |_col0|_col1|_col2|  _col3|_col4|_col5|_col6|_col7|_col8|
 +-----+-----+-----+-------+-----+-----+-----+-----+-----+
 |    1|    M|    M|Primary|  500| Good|    0|    0|    0|
 |    2|    F|    M|Primary|  500| Good|    0|    0|    0|
 |    3|    M|    S|Primary|  500| Good|    0|    0|    0|
 |    4|    F|    S|Primary|  500| Good|    0|    0|    0|
 |    5|    M|    D|Primary|  500| Good|    0|    0|    0|
 |    6|    F|    D|Primary|  500| Good|    0|    0|    0|
 |    7|    M|    W|Primary|  500| Good|    0|    0|    0|
 |    8|    F|    W|Primary|  500| Good|    0|    0|    0|
 |    9|    M|    U|Primary|  500| Good|    0|    0|    0|
 |   10|    F|    U|Primary|  500| Good|    0|    0|    0|
 +-----+-----+-----+-------+-----+-----+-----+-----+-----+
 only showing top 10 rows

 root
 |-- _col0: integer (nullable = true)
 |-- _col1: string (nullable = true)
 |-- _col2: string (nullable = true)
 |-- _col3: string (nullable = true)
 |-- _col4: integer (nullable = true)
 |-- _col5: string (nullable = true)
 |-- _col6: integer (nullable = true)
 |-- _col7: integer (nullable = true)
 |-- _col8: integer (nullable = true)
 */
}