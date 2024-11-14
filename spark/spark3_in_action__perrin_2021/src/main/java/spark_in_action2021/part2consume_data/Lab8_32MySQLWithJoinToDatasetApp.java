package spark_in_action2021.part2consume_data;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author jgp
 */
public class Lab8_32MySQLWithJoinToDatasetApp {

    public static void main(String[] args) {
        Lab8_32MySQLWithJoinToDatasetApp app = new Lab8_32MySQLWithJoinToDatasetApp();
        app.start();
        app.startSelectAll();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with join to Dataframe using JDBC")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("useSSL", "false");
        props.put("serverTimezone", "UTC");

        // Builds the SQL query doing the join operation
        String sqlQuery =
                "select actor.first_name, actor.last_name, film.title, film.description "
                        + "from actor, film_actor, film "
                        + "where actor.actor_id = film_actor.actor_id "
                        + "and film_actor.film_id = film.film_id";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "(" + sqlQuery + ") actor_film_alias",
                props);

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");

/**
 +----------+---------+--------------------+--------------------+
 |first_name|last_name|               title|         description|
 +----------+---------+--------------------+--------------------+
 |  PENELOPE|  GUINESS|    ACADEMY DINOSAUR|A Epic Drama of a...|
 |  PENELOPE|  GUINESS|ANACONDA CONFESSIONS|A Lacklusture Dis...|
 |  PENELOPE|  GUINESS|         ANGELS LIFE|A Thoughtful Disp...|
 |  PENELOPE|  GUINESS|BULWORTH COMMANDM...|A Amazing Display...|
 |  PENELOPE|  GUINESS|       CHEAPER CLYDE|A Emotional Chara...|
 +----------+---------+--------------------+--------------------+
 only showing top 5 rows

 root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)

 The dataframe contains 5462 record(s).
 */

    }

    private void startSelectAll() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with join to Dataframe using JDBC")
                .master("local")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("useSSL", "false");
        props.put("serverTimezone", "UTC");

        // Builds the SQL query doing the join operation
        String sqlQuery =
                "select * "
                        + "from actor, film_actor, film "
                        + "where actor.actor_id = film_actor.actor_id "
                        + "and film_actor.film_id = film.film_id";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "(" + sqlQuery + ") actor_film_alias",
                props);

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");

/**
 Exception in thread "main" java.sql.SQLSyntaxErrorException: Duplicate column name 'actor_id'
 */
    }

}
