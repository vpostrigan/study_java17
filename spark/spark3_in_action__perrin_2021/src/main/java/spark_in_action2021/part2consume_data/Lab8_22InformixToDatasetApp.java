package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

/**
 * MySQL injection to Spark, using the Sakila sample database.
 * <p>
 *     https://github.com/informix/informix-dockerhub-readme/blob/master/12.10.FC12/informix-developer-database.md
 * docker run -it --name ifx --privileged -p 9088:9088 -p 9089:9089 -p 27017:27017 -p 27018:27018 -p 27883:27883 -e LICENSE=accept ibmcom/informix-developer-database:latest
 *
 * @author jgp
 */
public class Lab8_22InformixToDatasetApp {

    public static void main(String[] args) {
        Lab8_22InformixToDatasetApp app = new Lab8_22InformixToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Informix to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        // Specific Informix dialect
        JdbcDialect dialect = new Lab8_21InformixJdbcDialect();
        JdbcDialects.registerDialect(dialect);

        Dataset<Row> df = spark.read().format("jdbc")
                .option("url", "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y")
                .option("dbtable", "customer")
                .option("user", "informix")
                .option("password", "in4mix")
                .load();

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
    }

}
