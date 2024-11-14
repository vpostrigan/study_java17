package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * MySQL injection to Spark, using the Sakila sample database.
 * <p>
 * https://github.com/jgperrin/net.jgp.books.spark.ch08
 *
 * 'Environment variables: DB_PASSWORD=password'
 *
 * @author jgp
 */
public class Lab8_11MySQLToDatasetApp {

    public static void main(String[] args) {
        Lab8_11MySQLToDatasetApp app = new Lab8_11MySQLToDatasetApp();
        app.start();
        app.startWithLongUrlApp();
        app.startWithOptionsApp();
        app.start_ingestion_env_variable();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("useSSL", "false");

        Dataset<Row> df = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/sakila?serverTimezone=UTC",
                        "actor", props);
        df = df.orderBy(df.col("last_name"));

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");

/**
 +--------+----------+---------+-------------------+
 |actor_id|first_name|last_name|        last_update|
 +--------+----------+---------+-------------------+
 |      92|   KIRSTEN|   AKROYD|2006-02-15 06:34:33|
 |      58| CHRISTIAN|   AKROYD|2006-02-15 06:34:33|
 |     182|    DEBBIE|   AKROYD|2006-02-15 06:34:33|
 |     118|      CUBA|    ALLEN|2006-02-15 06:34:33|
 |     145|       KIM|    ALLEN|2006-02-15 06:34:33|
 +--------+----------+---------+-------------------+
 only showing top 5 rows

 root
 |-- actor_id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

 The dataframe contains 200 record(s).
 */
    }

    private void startWithLongUrlApp() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        // Using a JDBC URL
        String jdbcUrl = "jdbc:mysql://localhost:3306/sakila"
                + "?user=root"
                + "&password=password"
                + "&allowPublicKeyRetrieval=true"
                + "&useSSL=false"
                + "&serverTimezone=UTC";

        // And read in one shot
        Dataset<Row> df = spark.read()
                .jdbc(jdbcUrl, "actor", new Properties());
        df = df.orderBy(df.col("last_name"));

        df.show();
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
/**
 +--------+-----------+---------+-------------------+
 |actor_id| first_name|last_name|        last_update|
 +--------+-----------+---------+-------------------+
 |     182|     DEBBIE|   AKROYD|2006-02-15 06:34:33|
 |      92|    KIRSTEN|   AKROYD|2006-02-15 06:34:33|
 |      58|  CHRISTIAN|   AKROYD|2006-02-15 06:34:33|
 |     118|       CUBA|    ALLEN|2006-02-15 06:34:33|
 |     194|      MERYL|    ALLEN|2006-02-15 06:34:33|
 |     145|        KIM|    ALLEN|2006-02-15 06:34:33|
 |      76|   ANGELINA|  ASTAIRE|2006-02-15 06:34:33|
 |     112|    RUSSELL|   BACALL|2006-02-15 06:34:33|
 |      67|    JESSICA|   BAILEY|2006-02-15 06:34:33|
 |     190|     AUDREY|   BAILEY|2006-02-15 06:34:33|
 |     115|   HARRISON|     BALE|2006-02-15 06:34:33|
 |     187|      RENEE|     BALL|2006-02-15 06:34:33|
 |      47|      JULIA|BARRYMORE|2006-02-15 06:34:33|
 |     158|     VIVIEN| BASINGER|2006-02-15 06:34:33|
 |     174|    MICHAEL|   BENING|2006-02-15 06:34:33|
 |     124|   SCARLETT|   BENING|2006-02-15 06:34:33|
 |      14|     VIVIEN|   BERGEN|2006-02-15 06:34:33|
 |     121|       LIZA|  BERGMAN|2006-02-15 06:34:33|
 |      91|CHRISTOPHER|    BERRY|2006-02-15 06:34:33|
 |      60|      HENRY|    BERRY|2006-02-15 06:34:33|
 +--------+-----------+---------+-------------------+
 only showing top 20 rows

 root
 |-- actor_id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

 The dataframe contains 200 record(s).
 */
    }

    private void startWithOptionsApp() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        // In a "one-liner" with method chaining and options
        Dataset<Row> df = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/sakila")
                .option("dbtable", "actor")
                .option("user", "root")
                .option("password", "password")
                .option("allowPublicKeyRetrieval", "true")
                .option("useSSL", "false")
                .option("serverTimezone", "UTC")
                .load();
        df = df.orderBy(df.col("last_name"));

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
/**
 +--------+----------+---------+-------------------+
 |actor_id|first_name|last_name|        last_update|
 +--------+----------+---------+-------------------+
 |      92|   KIRSTEN|   AKROYD|2006-02-15 06:34:33|
 |      58| CHRISTIAN|   AKROYD|2006-02-15 06:34:33|
 |     182|    DEBBIE|   AKROYD|2006-02-15 06:34:33|
 |     118|      CUBA|    ALLEN|2006-02-15 06:34:33|
 |     145|       KIM|    ALLEN|2006-02-15 06:34:33|
 +--------+----------+---------+-------------------+
 only showing top 5 rows

 root
 |-- actor_id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

 The dataframe contains 200 record(s).
 */
    }

    /**
     * MySQL injection to Spark, using the Sakila sample database, taking the
     * password from an environment variable instead of a hard-coded value.
     *
     * @author jgp
     */
    private void start_ingestion_env_variable() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");

        byte[] password = System.getenv("DB_PASSWORD").getBytes();
        props.put("password", new String(password));
        password = null;
        props.put("allowPublicKeyRetrieval", "true");
        props.put("useSSL", "false");

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila?serverTimezone=UTC",
                "actor", props);
        df = df.orderBy(df.col("last_name"));

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
/**
 +--------+----------+---------+-------------------+
 |actor_id|first_name|last_name|        last_update|
 +--------+----------+---------+-------------------+
 |      92|   KIRSTEN|   AKROYD|2006-02-15 06:34:33|
 |      58| CHRISTIAN|   AKROYD|2006-02-15 06:34:33|
 |     182|    DEBBIE|   AKROYD|2006-02-15 06:34:33|
 |     118|      CUBA|    ALLEN|2006-02-15 06:34:33|
 |     145|       KIM|    ALLEN|2006-02-15 06:34:33|
 +--------+----------+---------+-------------------+
 only showing top 5 rows

 root
 |-- actor_id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- last_update: timestamp (nullable = true)


 The dataframe contains 200 record(s).
 */
    }

}
