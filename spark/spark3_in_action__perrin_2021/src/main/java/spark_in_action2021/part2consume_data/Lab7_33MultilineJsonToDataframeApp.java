package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Multiline ingestion JSON ingestion in a dataframe.
 *
 * @author jgp
 */
public class Lab7_33MultilineJsonToDataframeApp {

    public static void main(String[] args) {
        Lab7_33MultilineJsonToDataframeApp app =
                new Lab7_33MultilineJsonToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
        Dataset<Row> df = spark.read().format("json")
                .option("multiline", true)
                .load("data/chapter7/countrytravelinfo.json");

        // Shows at most 3 rows from the dataframe
        df.show(3);
        df.printSchema();
    }

    /**
     +-----------------------+-----------------------+--------------------+--------------------+--------+--------------------+------------------------------------+--------------------+---+--------------------------+---------------------+
     |destination_description|entry_exit_requirements|    geopoliticalarea|              health|iso_code|    last_update_date|local_laws_and_special_circumstances| safety_and_security|tag|travel_embassyAndConsulate|travel_transportation|
     +-----------------------+-----------------------+--------------------+--------------------+--------+--------------------+------------------------------------+--------------------+---+--------------------------+---------------------+
     |   <p>The three isla...|   <p>All U.S. citiz...|Bonaire, Sint Eus...|<p>Medical care o...|        |Last Updated: Sep...|                <p>&nbsp;</p><p><...|<p>There are no k...| A1|          <div class="c...| <p><b>Road Condit...|
     |   <p>French Guiana ...|   <p>Visit the&nbsp...|       French Guiana|<p>Medical care w...|      GF|Last Updated: Oct...|                <p><b>Criminal Pe...|<p>French Guiana ...| A2|          <div class="c...| <p><b>Road Condit...|
     |   <p>See the Depart...|   <p><b>Passports a...|       St Barthelemy|<p><b>We do not p...|      BL|Last Updated: Jun...|                <p><b>Criminal Pe...|<p><b>Crime</b>: ...| A3|          <div class="c...| <p><b>Road Condit...|
     +-----------------------+-----------------------+--------------------+--------------------+--------+--------------------+------------------------------------+--------------------+---+--------------------------+---------------------+
     only showing top 3 rows

     root
     |-- destination_description: string (nullable = true)
     |-- entry_exit_requirements: string (nullable = true)
     |-- geopoliticalarea: string (nullable = true)
     |-- health: string (nullable = true)
     |-- iso_code: string (nullable = true)
     |-- last_update_date: string (nullable = true)
     |-- local_laws_and_special_circumstances: string (nullable = true)
     |-- safety_and_security: string (nullable = true)
     |-- tag: string (nullable = true)
     |-- travel_embassyAndConsulate: string (nullable = true)
     |-- travel_transportation: string (nullable = true)
     */
}
