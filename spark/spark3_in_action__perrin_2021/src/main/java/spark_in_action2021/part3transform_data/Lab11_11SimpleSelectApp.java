package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple SQL select on ingested data
 *
 * @author jgp
 */
public class Lab11_11SimpleSelectApp {

    public static void main(String[] args) {
        Lab11_11SimpleSelectApp app = new Lab11_11SimpleSelectApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        // schema has only two columns so rest will be drop
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
        });

        // Reads a CSV file with header, called books.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");
        df.createOrReplaceTempView("geodata"); // it will be dropped after session end
        df.printSchema();

        Dataset<Row> smallCountries = spark.sql(
                "SELECT * FROM geodata " +
                        "WHERE yr1980 < 1 " +
                        "ORDER BY 2 " +
                        "LIMIT 5");
        // Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
        smallCountries.show(10);
    }
/**
 root
 |-- geo: string (nullable = true)
 |-- yr1980: double (nullable = true)

 +--------------------+-------+
 |                 geo| yr1980|
 +--------------------+-------+
 |Falkland Islands ...|  0.002|
 |                Niue|  0.002|
 |Saint Pierre and ...|0.00599|
 |        Saint Helena|0.00647|
 |Turks and Caicos ...|0.00747|
 +--------------------+-------+
 */
}
