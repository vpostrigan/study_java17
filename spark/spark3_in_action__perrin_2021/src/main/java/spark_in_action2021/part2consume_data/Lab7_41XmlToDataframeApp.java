package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * XML ingestion to a dataframe.
 * <p>
 * Source of file: NASA patents dataset -
 * https://data.nasa.gov/Raw-Data/NASA-Patents/gquh-watm
 *
 * @author jgp
 */
public class Lab7_41XmlToDataframeApp {

    public static void main(String[] args) {
        Lab7_41XmlToDataframeApp app = new Lab7_41XmlToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("XML to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads a XML file with header, called nasa-patents.xml,
        // stores it in a dataframe
        Dataset<Row> df = spark.read().format("xml")
                .option("rowTag", "row")
                .load("data/chapter7/nasa-patents.xml");

        // Shows at most 5 rows from the dataframe
        df.show(5);
        df.printSchema();
    }
/**
 +--------------------+----+----------+--------------------+--------------+--------------+--------------------+----------------------+-------------+-----------+--------------------+
 |           __address|__id|__position|              __uuid|application_sn|   case_number|              center|patent_expiration_date|patent_number|     status|               title|
 +--------------------+----+----------+--------------------+--------------+--------------+--------------------+----------------------+-------------+-----------+--------------------+
 |https://data.nasa...| 407|       407|2311F785-C00F-422...|    13/033,085|     KSC-12871|NASA Kennedy Spac...|                  null|            0|Application|Polyimide Wire In...|
 |https://data.nasa...|   1|         1|BAC69188-84A6-4D2...|    08/543,093|   ARC-14048-1|NASA Ames Researc...|   2015-10-03 03:00:00|      5694939|     Issued|Autogenic-Feedbac...|
 |https://data.nasa...|   2|         2|23D6A5BD-26E2-42D...|    09/017,519|   ARC-14231-1|NASA Ames Researc...|   2017-02-04 02:00:00|      6109270|     Issued|Multimodality Ins...|
 |https://data.nasa...|   3|         3|F8052701-E520-43A...|    10/874,003|ARC-14231-2DIV|NASA Ames Researc...|   2024-06-16 03:00:00|      6976013|     Issued|Metrics For Body ...|
 |https://data.nasa...|   4|         4|20A4C4A9-EEB6-45D...|    09/652,299|   ARC-14231-3|NASA Ames Researc...|   2017-02-04 02:00:00|      6718196|     Issued|Multimodality Ins...|
 +--------------------+----+----------+--------------------+--------------+--------------+--------------------+----------------------+-------------+-----------+--------------------+
 only showing top 5 rows

 root
 |-- __address: string (nullable = true)
 |-- __id: long (nullable = true)
 |-- __position: long (nullable = true)
 |-- __uuid: string (nullable = true)
 |-- application_sn: string (nullable = true)
 |-- case_number: string (nullable = true)
 |-- center: string (nullable = true)
 |-- patent_expiration_date: timestamp (nullable = true)
 |-- patent_number: string (nullable = true)
 |-- status: string (nullable = true)
 |-- title: string (nullable = true)
 */
}