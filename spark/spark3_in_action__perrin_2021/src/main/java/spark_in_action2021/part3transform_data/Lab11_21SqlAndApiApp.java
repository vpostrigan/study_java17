package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple SQL select on ingested data after preparing the data with the dataframe API.
 *
 * @author jgp
 */
public class Lab11_21SqlAndApiApp {

    public static void main(String[] args) {
        Lab11_21SqlAndApiApp app = new Lab11_21SqlAndApiApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SQL")
                .master("local")
                .getOrCreate();

        // Create the schema for the whole dataset
        List<StructField> list = new ArrayList<>();
        list.add(DataTypes.createStructField("geo", DataTypes.StringType, true));
        for (int i = 1980; i <= 2010; i++) {
            list.add(DataTypes.createStructField("yr" + i, DataTypes.DoubleType, false));
        }
        StructType schema = DataTypes.createStructType(list.toArray((new StructField[0])));

        // Reads a CSV file with header (as specified in the schema), called
        // populationbycountry19802010millions.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");

        // Remove the columns we do not want
        for (int i = 1981; i < 2010; i++) {
            df = df.drop(df.col("yr" + i));
        }

        // Creates a new column with the evolution of the population between 1980 and 2010
        df = df.withColumn("evolution", functions.expr("round((yr2010 - yr1980) * 1000000)"));
        df.createOrReplaceTempView("geodata");
        df.show(10, false);

        Dataset<Row> negativeEvolutionDf = spark.sql(
                "SELECT * FROM geodata "
                        + "WHERE geo IS NOT NULL AND evolution <= 0 "
                        + "ORDER BY evolution "
                        + "LIMIT 25");
        // Shows at most 15 rows from the dataframe
        negativeEvolutionDf.show(10, false);

        Dataset<Row> moreThanAMillionDf = spark.sql(
                "SELECT * FROM geodata "
                        + "WHERE geo IS NOT NULL AND evolution > 1000000 "
                        + "ORDER BY evolution DESC "
                        + "LIMIT 25");
        moreThanAMillionDf.show(10, false);
    }
/**
 +-------------------------+---------+---------+-----------+
 |geo                      |yr1980   |yr2010   |evolution  |
 +-------------------------+---------+---------+-----------+
 |North America            |320.27638|456.59331|1.3631693E8|
 |Bermuda                  |0.05473  |0.06827  |13540.0    |
 |Canada                   |24.5933  |33.75974 |9166440.0  |
 |Greenland                |0.05021  |0.05764  |7430.0     |
 |Mexico                   |68.34748 |112.46886|4.412138E7 |
 |Saint Pierre and Miquelon|0.00599  |0.00594  |-50.0      |
 |United States            |227.22468|310.23286|8.300818E7 |
 |Central & South America  |293.05856|480.01228|1.8695372E8|
 |Antarctica               |null     |null     |null       |
 |Antigua and Barbuda      |0.06855  |0.08675  |18200.0    |
 +-------------------------+---------+---------+-----------+
 only showing top 10 rows

 +-------------------------+--------+--------+----------+
 |geo                      |yr1980  |yr2010  |evolution |
 +-------------------------+--------+--------+----------+
 |Bulgaria                 |8.84353 |7.14879 |-1694740.0|
 |Hungary                  |10.71112|9.99234 |-718780.0 |
 |Romania                  |22.13004|21.95928|-170760.0 |
 |Guyana                   |0.75935 |0.74849 |-10860.0  |
 |Montserrat               |0.01177 |0.00512 |-6650.0   |
 |Cook Islands             |0.01801 |0.01149 |-6520.0   |
 |Netherlands Antilles     |0.23244 |0.22869 |-3750.0   |
 |Dominica                 |0.07389 |0.07281 |-1080.0   |
 |Saint Pierre and Miquelon|0.00599 |0.00594 |-50.0     |
 +-------------------------+--------+--------+----------+

 +-----------------------+----------+----------+------------+
 |geo                    |yr1980    |yr2010    |evolution   |
 +-----------------------+----------+----------+------------+
 |World                  |4451.32679|6853.01941|2.40169262E9|
 |Asia & Oceania         |2469.81743|3799.67028|1.32985285E9|
 |Africa                 |478.96479 |1015.47842|5.3651363E8 |
 |India                  |684.8877  |1173.10802|4.8822032E8 |
 |China                  |984.73646 |1330.14129|3.4540483E8 |
 |Central & South America|293.05856 |480.01228 |1.8695372E8 |
 |North America          |320.27638 |456.59331 |1.3631693E8 |
 |Middle East            |93.78699  |212.33692 |1.1854993E8 |
 |Pakistan               |85.21912  |184.40479 |9.918567E7  |
 |Indonesia              |151.0244  |242.96834 |9.194394E7  |
 +-----------------------+----------+----------+------------+
 only showing top 10 rows
 */
}
