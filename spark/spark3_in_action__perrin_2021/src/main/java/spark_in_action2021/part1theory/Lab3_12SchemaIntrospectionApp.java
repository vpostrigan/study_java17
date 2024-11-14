package spark_in_action2021.part1theory;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Introspection of a schema.
 *
 * @author jgp
 */
public class Lab3_12SchemaIntrospectionApp {

    public static void main(String[] args) {
        Lab3_12SchemaIntrospectionApp app = new Lab3_12SchemaIntrospectionApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Schema introspection for restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header, called Restaurants_in_Wake_County_NC.csv,
        // stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County_NC.csv");

        // Let's transform our dataframe
        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY");
        df = df.withColumn("id",
                concat(df.col("state"),
                        lit("_"),
                        df.col("county"),
                        lit("_"),
                        df.col("datasetId")));

        // NEW
        ////////////////////////////////////////////////////////////////////

        StructType schema = df.schema();

        System.out.println("*** Schema as a tree:");
        schema.printTreeString();
        String schemaAsString = schema.mkString();
        System.out.println("*** Schema as string: " + schemaAsString);
        String schemaAsJson = schema.prettyJson();
        System.out.println("*** Schema as JSON: " + schemaAsJson);

        /**
         * *** Schema as a tree:
         * root
         *  |-- OBJECTID: string (nullable = true)
         *  |-- datasetId: string (nullable = true)
         *  |-- name: string (nullable = true)
         *  |-- address1: string (nullable = true)
         *  |-- address2: string (nullable = true)
         *  |-- city: string (nullable = true)
         *  |-- state: string (nullable = true)
         *  |-- zip: string (nullable = true)
         *  |-- tel: string (nullable = true)
         *  |-- dateStart: string (nullable = true)
         *  |-- type: string (nullable = true)
         *  |-- PERMITID: string (nullable = true)
         *  |-- geoX: string (nullable = true)
         *  |-- geoY: string (nullable = true)
         *  |-- GEOCODESTATUS: string (nullable = true)
         *  |-- county: string (nullable = false)
         *  |-- id: string (nullable = true)
         *
         * *** Schema as string: StructField(OBJECTID,StringType,true)StructField(datasetId,StringType,true)StructField(name,StringType,true)StructField(address1,StringType,true)StructField(address2,StringType,true)StructField(city,StringType,true)StructField(state,StringType,true)StructField(zip,StringType,true)StructField(tel,StringType,true)StructField(dateStart,StringType,true)StructField(type,StringType,true)StructField(PERMITID,StringType,true)StructField(geoX,StringType,true)StructField(geoY,StringType,true)StructField(GEOCODESTATUS,StringType,true)StructField(county,StringType,false)StructField(id,StringType,true)
         *
         * *** Schema as JSON: {
         *   "type" : "struct",
         *   "fields" : [ {
         *     "name" : "OBJECTID",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "datasetId",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "name",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "address1",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "address2",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "city",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "state",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "zip",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "tel",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "dateStart",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "type",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "PERMITID",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "geoX",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "geoY",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "GEOCODESTATUS",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   }, {
         *     "name" : "county",
         *     "type" : "string",
         *     "nullable" : false,
         *     "metadata" : { }
         *   }, {
         *     "name" : "id",
         *     "type" : "string",
         *     "nullable" : true,
         *     "metadata" : { }
         *   } ]
         * }
         */
    }

}