package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple SQL select on ingested data, using a global view
 *
 * @author jgp
 */
public class Lab11_12SimpleSelectGlobalViewApp {

    public static void main(String[] args) {
        Lab11_12SimpleSelectGlobalViewApp app = new Lab11_12SimpleSelectGlobalViewApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
        });

        // Reads a CSV file with header, called books.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");
        df.createOrReplaceGlobalTempView("geodata"); // it will be dropped after all sessions end
        df.printSchema();

        Dataset<Row> smallCountriesDf = spark.sql(
                "SELECT * FROM global_temp.geodata " +
                        "WHERE yr1980 < 1 " +
                        "ORDER BY 2 " +
                        "LIMIT 5");
        // Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
        smallCountriesDf.show(10, false);

        // Create a new session and query the same data
        SparkSession spark2 = spark.newSession();
        Dataset<Row> slightlyBiggerCountriesDf = spark2.sql(
                "SELECT * FROM global_temp.geodata " +
                        "WHERE yr1980 >= 1 " +
                        "ORDER BY 2 " +
                        "LIMIT 5");
        slightlyBiggerCountriesDf.show(10, false);

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
/**
 root
 |-- geo: string (nullable = true)
 |-- yr1980: double (nullable = true)

 +---------------------------------+-------+
 |geo                              |yr1980 |
 +---------------------------------+-------+
 |Falkland Islands (Islas Malvinas)|0.002  |
 |Niue                             |0.002  |
 |Saint Pierre and Miquelon        |0.00599|
 |Saint Helena                     |0.00647|
 |Turks and Caicos Islands         |0.00747|
 +---------------------------------+-------+

 +--------------------+-------+
 |geo                 |yr1980 |
 +--------------------+-------+
 |United Arab Emirates|1.00029|
 |Trinidad and Tobago |1.09051|
 |Oman                |1.18548|
 |Lesotho             |1.35857|
 |Kuwait              |1.36977|
 +--------------------+-------+
 */
}
