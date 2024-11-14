package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.net.ssl.SSLContext;

/**
 * CSV ingestion in a dataframe.
 *
 * @author jgp
 */
public class Lab16_41BasicBookStats {

    public static void main(String[] args) {
        Lab16_41BasicBookStats app = new Lab16_41BasicBookStats();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Basic book stats")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header, called books.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/chapter16/goodreads/books.csv");

        df = df
                .withColumn("main_author", split(col("authors"), "-").getItem(0));

        // Shows at most 5 rows from the dataframe
        df.show(5);
    }

}
/*
+------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+------------+
|bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|# num_pages|ratings_count|text_reviews_count| main_author|
+------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+------------+
|     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|        652|      1944099|             26249|J.K. Rowling|
|     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|        870|      1996446|             27613|J.K. Rowling|
|     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|        320|      5629932|             70390|J.K. Rowling|
|     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|        352|         6267|               272|J.K. Rowling|
|     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|        435|      2149872|             33964|J.K. Rowling|
+------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+------------+
only showing top 5 rows
 */