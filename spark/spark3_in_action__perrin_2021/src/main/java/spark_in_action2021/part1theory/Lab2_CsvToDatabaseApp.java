package spark_in_action2021.part1theory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 * https://github.com/jgperrin/net.jgp.books.spark.ch02
 * <p>
 * if table ch02 doesn't exist, it will be created
 * <p>
 * To manually create the PostgreSQL database, you can use the following SQL commands:
 * <p>
 * CREATE ROLE jgp WITH
 * LOGIN
 * NOSUPERUSER
 * NOCREATEDB
 * NOCREATEROLE
 * INHERIT
 * NOREPLICATION
 * CONNECTION LIMIT -1
 * PASSWORD 'Spark<3Java';
 * <p>
 * CREATE DATABASE spark_labs
 * WITH
 * OWNER = jgp
 * ENCODING = 'UTF8'
 * CONNECTION LIMIT = -1;
 */
public class Lab2_CsvToDatabaseApp {

    public static void main(String[] args) {
        Lab2_CsvToDatabaseApp app = new Lab2_CsvToDatabaseApp();
        //app.start("/database_postgresql.properties");
        //app.start("/database_mysql.properties");
        app.start2("/database_mysql.properties");
    }

    private void start(String database) {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        System.out.println("\nStep 1: Ingestion");
        // ---------

        // Reads a CSV file with header, called authors.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        System.out.println("\nStep 2: Transform");
        // ---------

        // Creates a new column called "name" as the concatenation of lname, a
        // virtual column containing ", " and the fname column
        df = df.withColumn(
                "name",
                concat(df.col("lname"), lit(", "), df.col("fname")));

        System.out.println("\nStep 3: Save");
        // ---------

        Properties props = new Properties();
        try (InputStream in = Lab2_CsvToDatabaseApp.class.getResourceAsStream(database)) {
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // The connection URL, assuming your PostgreSQL instance runs locally on the
        // default port, and the database we use is "postgres"
        String dbConnectionUrl = props.getProperty("url");

        // Properties to connect to the database, the JDBC driver is part of our pom.xml
        Properties prop = new Properties();
        prop.setProperty("driver", props.getProperty("driver"));
        prop.setProperty("user", props.getProperty("username"));
        prop.setProperty("password", props.getProperty("password"));

        // Write in a table called ch02
        df.write()
                // will fix Exception in thread "main" org.apache.spark.sql.AnalysisException: Table or view 'ch02' already exists. SaveMode: ErrorIfExists.
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "ch02", prop);

        spark.stop();
        System.out.println("Process complete");
    }

    // //

    /**
     * CSV to a relational database. This is very similar to lab #100 (start() above), the
     * save() syntax is slightly different.
     */
    private void start2(String database) {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB (lab #900)")
                .master("local[*]")
                .getOrCreate();

        System.out.println("\nStep 1: Ingestion");
        // ---------

        // Reads a CSV file with header, called authors.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/authors.csv");

        System.out.println("\nStep 2: Transform");
        // ---------

        // Creates a new column called "name" as the concatenation of lname, a
        // virtual column containing ", " and the fname column
        df = df.withColumn(
                "name",
                concat(df.col("lname"), lit(", "), df.col("fname")));

        System.out.println("\nStep 3: Save");
        // ---------

        Properties props = new Properties();
        try (InputStream in = Lab2_CsvToDatabaseApp.class.getResourceAsStream(database)) {
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Write in a table called ch02lab900
        df.write().format("jdbc")
                .mode(SaveMode.Overwrite)
                .option("dbtable", "ch02lab900")
                .option("url", props.getProperty("url"))
                .option("driver", props.getProperty("driver"))
                .option("user", props.getProperty("username"))
                .option("password", props.getProperty("password"))
                .save();

        spark.stop();
        System.out.println("Process complete");
    }

}
