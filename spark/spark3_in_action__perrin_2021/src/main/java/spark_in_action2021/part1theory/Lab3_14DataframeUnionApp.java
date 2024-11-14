package spark_in_action2021.part1theory;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Union of two dataframes.
 *
 * @author jgp
 */
public class Lab3_14DataframeUnionApp {
    private SparkSession spark;

    public static void main(String[] args) {
        Lab3_14DataframeUnionApp app = new Lab3_14DataframeUnionApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        this.spark = SparkSession.builder()
                .appName("Union of two dataframes")
                .master("local")
                .getOrCreate();

        Dataset<Row> wakeRestaurantsDf = buildWakeRestaurantsDataframe();
        Dataset<Row> durhamRestaurantsDf = buildDurhamRestaurantsDataframe();
        combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf);
    }

    /**
     * Builds the dataframe containing the Wake county restaurants
     *
     * @return A dataframe
     */
    private Dataset<Row> buildWakeRestaurantsDataframe() {
        Dataset<Row> df = this.spark.read().format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County_NC.csv");
        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumn("dateStart", split(df.col("RESTAURANTOPENDATE"), "T").getItem(0))
                .withColumn("dateEnd", lit(null))
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop(df.col("OBJECTID"))
                .drop(df.col("GEOCODESTATUS"))
                .drop(df.col("PERMITID"));
        df = df.withColumn("id",
                concat(df.col("state"),
                        lit("_"),
                        df.col("county"),
                        lit("_"),
                        df.col("datasetId")))
                .drop("RESTAURANTOPENDATE")
        ;

        // I left the following line if you want to play with repartitioning
        // df = df.repartition(4);

        return df;
    }

    /**
     * Builds the dataframe containing the Durham county restaurants
     *
     * @return A dataframe
     */
    private Dataset<Row> buildDurhamRestaurantsDataframe() {
        Dataset<Row> df = this.spark.read().format("json")
                .load("data/Restaurants_in_Durham_County_NC.json");
        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", split(df.col("fields.opening_date"), "T").getItem(0))
                .withColumn("dateEnd", df.col("fields.closing_date")) // TODO 21-AUG-2010 => 2010-08-21
                .withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop(df.col("fields"))
                .drop(df.col("geometry"))
                .drop(df.col("record_timestamp"))
                .drop(df.col("recordid"));
        df = df.withColumn("id",
                concat(df.col("state"),
                        lit("_"),
                        df.col("county"),
                        lit("_"),
                        df.col("datasetId")));

        // I left the following line if you want to play with repartitioning
        // df = df.repartition(4);

        return df;
    }

    /**
     * Performs the union between the two dataframes.
     *
     * @param df1 Dataframe to union on
     * @param df2 Dataframe to union from
     */
    private void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> df = df1.unionByName(df2);
        df.show(5000);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count: " + partitionCount);
    }

}
