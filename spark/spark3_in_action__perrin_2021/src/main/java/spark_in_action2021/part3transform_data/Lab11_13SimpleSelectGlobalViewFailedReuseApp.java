package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reusing a global view between two apps is not possible
 * <p>
 * To test:
 * Run Lab11_12SimpleSelectGlobalViewApp first
 * You have 60s to run Lab11_13SimpleSelectGlobalViewFailedReuseApp,
 * but you will see that the view is not shared between the application...
 *
 * @author jgp
 */
public class Lab11_13SimpleSelectGlobalViewFailedReuseApp {

    public static void main(String[] args) {
        Lab11_13SimpleSelectGlobalViewFailedReuseApp app =
                new Lab11_13SimpleSelectGlobalViewFailedReuseApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        // This will fail as it is not the same application
        Dataset<Row> smallCountries = spark.sql(
                "SELECT * FROM global_temp.geodata " +
                        "WHERE yr1980 > 1 " +
                        "ORDER BY 2 " +
                        "LIMIT 5");

        // Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
        smallCountries.show(10, false);
    }

}
