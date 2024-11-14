package spark_in_action2021.part2consume_data;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Lab8_41MySQLToDatasetPartitionApp {

    public static void main(String[] args) {
        Lab8_41MySQLToDatasetPartitionApp app = new Lab8_41MySQLToDatasetPartitionApp();
        app.start();
        app.startWithoutPartitionApp();
    }

    /**
     * Partitioning the film table in 10 for a MySQL injection to Spark, using
     * the Sakila sample database.
     *
     * @author jgp
     */
    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using JDBC with partitioning")
                .master("local")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("useSSL", "false");
        props.put("serverTimezone", "UTC");

        // Used for partitioning
        props.put("partitionColumn", "film_id");
        props.put("lowerBound", "1");
        props.put("upperBound", "1000");
        props.put("numPartitions", "10");

// 22/04/28 11:54:19 INFO JDBCRelation: Number of partitions: 10, WHERE clauses of these partitions: `film_id` < 101 or `film_id` is null, `film_id` >= 101 AND `film_id` < 201, `film_id` >= 201 AND `film_id` < 301, `film_id` >= 301 AND `film_id` < 401, `film_id` >= 401 AND `film_id` < 501, `film_id` >= 501 AND `film_id` < 601, `film_id` >= 601 AND `film_id` < 701, `film_id` >= 701 AND `film_id` < 801, `film_id` >= 801 AND `film_id` < 901, `film_id` >= 901

        view(spark, props);

/**
 +-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
 |film_id|           title|         description|release_year|language_id|original_language_id|rental_duration|rental_rate|length|replacement_cost|rating|    special_features|        last_update|
 +-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
 |      1|ACADEMY DINOSAUR|A Epic Drama of a...|  2006-01-01|          1|                null|              6|       0.99|    86|           20.99|    PG|Deleted Scenes,Be...|2006-02-15 07:03:42|
 |      2|  ACE GOLDFINGER|A Astounding Epis...|  2006-01-01|          1|                null|              3|       4.99|    48|           12.99|     G|Trailers,Deleted ...|2006-02-15 07:03:42|
 |      3|ADAPTATION HOLES|A Astounding Refl...|  2006-01-01|          1|                null|              7|       2.99|    50|           18.99| NC-17|Trailers,Deleted ...|2006-02-15 07:03:42|
 |      4|AFFAIR PREJUDICE|A Fanciful Docume...|  2006-01-01|          1|                null|              5|       2.99|   117|           26.99|     G|Commentaries,Behi...|2006-02-15 07:03:42|
 |      5|     AFRICAN EGG|A Fast-Paced Docu...|  2006-01-01|          1|                null|              6|       2.99|   130|           22.99|     G|      Deleted Scenes|2006-02-15 07:03:42|
 +-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
 only showing top 5 rows

 root
 |-- film_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)
 |-- release_year: date (nullable = true)
 |-- language_id: integer (nullable = true)
 |-- original_language_id: integer (nullable = true)
 |-- rental_duration: integer (nullable = true)
 |-- rental_rate: decimal(4,2) (nullable = true)
 |-- length: integer (nullable = true)
 |-- replacement_cost: decimal(5,2) (nullable = true)
 |-- rating: string (nullable = true)
 |-- special_features: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

 The dataframe contains 1000 record(s).
 The dataframe is split over 10 partition(s).
 */
    }

    private void startWithoutPartitionApp() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using JDBC without partitioning")
                .master("local")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("useSSL", "false");
        props.put("serverTimezone", "UTC");

        view(spark, props);
/**
 +-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
 |film_id|           title|         description|release_year|language_id|original_language_id|rental_duration|rental_rate|length|replacement_cost|rating|    special_features|        last_update|
 +-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
 |      1|ACADEMY DINOSAUR|A Epic Drama of a...|  2006-01-01|          1|                null|              6|       0.99|    86|           20.99|    PG|Deleted Scenes,Be...|2006-02-15 07:03:42|
 |      2|  ACE GOLDFINGER|A Astounding Epis...|  2006-01-01|          1|                null|              3|       4.99|    48|           12.99|     G|Trailers,Deleted ...|2006-02-15 07:03:42|
 |      3|ADAPTATION HOLES|A Astounding Refl...|  2006-01-01|          1|                null|              7|       2.99|    50|           18.99| NC-17|Trailers,Deleted ...|2006-02-15 07:03:42|
 |      4|AFFAIR PREJUDICE|A Fanciful Docume...|  2006-01-01|          1|                null|              5|       2.99|   117|           26.99|     G|Commentaries,Behi...|2006-02-15 07:03:42|
 |      5|     AFRICAN EGG|A Fast-Paced Docu...|  2006-01-01|          1|                null|              6|       2.99|   130|           22.99|     G|      Deleted Scenes|2006-02-15 07:03:42|
 +-------+----------------+--------------------+------------+-----------+--------------------+---------------+-----------+------+----------------+------+--------------------+-------------------+
 only showing top 5 rows

 root
 |-- film_id: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)
 |-- release_year: date (nullable = true)
 |-- language_id: integer (nullable = true)
 |-- original_language_id: integer (nullable = true)
 |-- rental_duration: integer (nullable = true)
 |-- rental_rate: decimal(4,2) (nullable = true)
 |-- length: integer (nullable = true)
 |-- replacement_cost: decimal(5,2) (nullable = true)
 |-- rating: string (nullable = true)
 |-- special_features: string (nullable = true)
 |-- last_update: timestamp (nullable = true)

 The dataframe contains 1000 record(s).
 The dataframe is split over 1 partition(s).
 */
    }

    private void view(SparkSession spark, Properties props) {

        // Let's look for all movies matching the query
        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "film",
                props);

        // Displays the dataframe and some of its metadata
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
        System.out.println("The dataframe is split over " + df.rdd()
                .getPartitions().length + " partition(s).");
    }

}
