package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NYC schools analytics.
 *
 * @author jgp
 */
public class Lab15_51NewYorkSchoolsToPostgreSqlApp {
    private static Logger log = LoggerFactory.getLogger(Lab15_51NewYorkSchoolsToPostgreSqlApp.class);

    private SparkSession spark = null;

    public static void main(String[] args) {
        Lab15_51NewYorkSchoolsToPostgreSqlApp app = new Lab15_51NewYorkSchoolsToPostgreSqlApp();
        app.start();
    }

    private void start() {
        long t0 = System.currentTimeMillis();

        spark = SparkSession.builder()
                .appName("NYC schools to PostgreSQL")
                .master("local[*]")
                .getOrCreate();

        long t1 = System.currentTimeMillis();

        Dataset<Row> df =
                loadDataUsing2018Format(
                        "data/chapter15/nyc_school_attendance/2018*.csv.gz");

        df = df.unionByName(
                loadDataUsing2015Format(
                        "data/chapter15/nyc_school_attendance/2015*.csv.gz"));

        df = df.unionByName(
                loadDataUsing2006Format(
                        "data/chapter15/nyc_school_attendance/200*.csv.gz",
                        "data/chapter15/nyc_school_attendance/2012*.csv.gz"));

        long t2 = System.currentTimeMillis();

        String dbConnectionUrl = "jdbc:postgresql://localhost:5432/postgres";

        // Properties to connect to the database, the JDBC driver is part of our pom.xml
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "somePassword");

        // Write in a table called ch02
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "ch13_nyc_schools", prop);
        long t3 = System.currentTimeMillis();

        log.info("Dataset contains {} rows, processed in {} ms.", df.count(), (t3 - t0));
        log.info("Spark init ... {} ms.", (t1 - t0));
        log.info("Ingestion .... {} ms.", (t2 - t1));
        log.info("Output ....... {} ms.", (t3 - t2));
        df.sample(.5).show(5);
        df.printSchema();
    }

    /**
     * Loads a data file matching the 2018 format, then prepares it.
     *
     * @param fileNames
     * @return
     */
    private Dataset<Row> loadDataUsing2018Format(String... fileNames) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("schoolId", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
                DataTypes.createStructField("present", DataTypes.IntegerType, false),
                DataTypes.createStructField("absent", DataTypes.IntegerType, false),
                DataTypes.createStructField("released", DataTypes.IntegerType, false)});

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("dateFormat", "yyyyMMdd")
                .schema(schema)
                .load(fileNames);

        return df.withColumn("schoolYear", lit(2018));
    }

    /**
     * Load a data file matching the 2006 format.
     */
    private Dataset<Row> loadDataUsing2006Format(String... fileNames) {
        return loadData(fileNames, "yyyyMMdd");
    }

    /**
     * Load a data file matching the 2015 format.
     */
    private Dataset<Row> loadDataUsing2015Format(String... fileNames) {
        return loadData(fileNames, "MM/dd/yyyy");
    }

    /**
     * Common loader for most datasets, accepts a date format as part of the parameters.
     */
    private Dataset<Row> loadData(String[] fileNames, String dateFormat) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("schoolId", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
                DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
                DataTypes.createStructField("present", DataTypes.IntegerType, false),
                DataTypes.createStructField("absent", DataTypes.IntegerType, false),
                DataTypes.createStructField("released", DataTypes.IntegerType, false)});

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("dateFormat", dateFormat)
                .schema(schema)
                .load(fileNames);

        return df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4));
    }

}
/*
2022-11-05 12:50:01.287 - INFO --- [           main] til.Version.logVersion(Version.java:133): Elasticsearch Hadoop v7.17.3 [e52f4f523a]
2022-11-05 12:51:14.128 - INFO --- [           main] 51NewYorkSchoolsToPostgreSqlApp.java:73): Dataset contains 3398803 rows, processed in 74934 ms.
2022-11-05 12:51:14.129 - INFO --- [           main] 51NewYorkSchoolsToPostgreSqlApp.java:74): Spark init ... 3553 ms.
2022-11-05 12:51:14.129 - INFO --- [           main] 51NewYorkSchoolsToPostgreSqlApp.java:75): Ingestion .... 2958 ms.
2022-11-05 12:51:14.129 - INFO --- [           main] 51NewYorkSchoolsToPostgreSqlApp.java:76): Output ....... 68423 ms.
+--------+----------+--------+-------+------+--------+----------+
|schoolId|      date|enrolled|present|absent|released|schoolYear|
+--------+----------+--------+-------+------+--------+----------+
|  01M015|2018-09-06|     171|     17|   154|       0|      2018|
|  01M015|2018-09-13|     173|      9|   164|       0|      2018|
|  01M015|2018-09-14|     173|     11|   162|       0|      2018|
|  01M015|2018-09-18|     174|      7|   167|       0|      2018|
|  01M015|2018-09-21|     174|      8|   166|       0|      2018|
+--------+----------+--------+-------+------+--------+----------+
only showing top 5 rows

root
 |-- schoolId: string (nullable = true)
 |-- date: date (nullable = true)
 |-- enrolled: integer (nullable = true)
 |-- present: integer (nullable = true)
 |-- absent: integer (nullable = true)
 |-- released: integer (nullable = true)
 |-- schoolYear: string (nullable = true)
 */