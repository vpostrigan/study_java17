package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.Logs;

/**
 * E X P E R I M E N T A L
 * <p>
 * Performs a join between 3 datasets to build a list of higher education
 * institutions per county.
 *
 * @author jgp
 */
public class Lab12_22HigherEdInstitutionPerCountyUsingAliasApp {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab12_22HigherEdInstitutionPerCountyUsingAliasApp app =
                new Lab12_22HigherEdInstitutionPerCountyUsingAliasApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Join")
                .master("local")
                .getOrCreate();

        // [1] Ingestion of the census data
        Dataset<Row> censusDf = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/chapter12/census/PEP_2017_PEPANNRES.csv");
        censusDf = censusDf
                .drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");
        allLogs.showAndSaveToCsv("[1] Census data", censusDf, 3, false, true);

        // [2] Higher education institution (and yes, there is an Arkansas College
        // of Barbering and Hair Design)
        Dataset<Row> higherEdDf = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/chapter12/dapip/InstitutionCampus.csv");
        higherEdDf = higherEdDf
                .filter("LocationType = 'Institution'")
                .withColumn("addressElements", split(higherEdDf.col("Address"), " "));
        higherEdDf = higherEdDf
                .withColumn("addressElementCount", size(higherEdDf.col("addressElements")));
        higherEdDf = higherEdDf
                .withColumn("zip9",
                        element_at(
                                higherEdDf.col("addressElements"),
                                higherEdDf.col("addressElementCount")));
        higherEdDf = higherEdDf
                .withColumn("splitZipCode", split(higherEdDf.col("zip9"), "-"));
        higherEdDf = higherEdDf
                .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId")
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("LocationType")
                .drop("Address")
                .drop("GeneralPhone")
                .drop("AdminName")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .drop("Fax")
                .drop("UpdateDate")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode")
                .alias("highered");
        allLogs.showAndSaveToCsv("[2] Higher education institutions (DAPIP)", higherEdDf, 3, false, true);

        // [3] Zip to county
        Dataset<Row> countyZipDf = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/chapter12/hud/COUNTY_ZIP_092018.csv");
        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio")
                .alias("hud");
        allLogs.showAndSaveToCsv("[3] Counties / ZIP Codes (HUD)", countyZipDf, 3, false, true);

        // Institutions per county id
        Dataset<Row> institPerCountyDf = higherEdDf.join(countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");
        allLogs.show("Institutions per county id", institPerCountyDf, 3, false, false);

        allLogs.show("Higher education institutions inner-joined with HUD",
                institPerCountyDf
                        .filter(higherEdDf.col("zip").equalTo(27517)), 5, false, true);

        // Institutions per county name
        institPerCountyDf = institPerCountyDf.join(censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");
        allLogs.show("Institutions per county name", institPerCountyDf, 3, false, false);

        allLogs.show("Higher education institutions and county id with census",
                institPerCountyDf
                        .filter(higherEdDf.col("zip").equalTo(27517)), 5, false, true);

        // Final clean up
        institPerCountyDf = institPerCountyDf
                // TODO alias doesn't work like in example, they don't drop columns
                .drop("highered.zip")
                .drop("hud.county")
                .drop("countyId")
                .distinct();
        allLogs.showAndSaveToCsv("Final list", institPerCountyDf, 3, false, false);

        // //

        Dataset<Row> aggDf = institPerCountyDf
                .groupBy("county", "pop2017")
                .count();
        aggDf = aggDf.orderBy(aggDf.col("count").desc());
        allLogs.showAndSaveToCsv("The aggDf list", aggDf, 5, false, false);

        Dataset<Row> popDf = aggDf
                .filter("pop2017>30000")
                .withColumn("institutionPer10k", expr("count*10000/pop2017"));
        popDf = popDf.orderBy(popDf.col("institutionPer10k").desc());
        allLogs.showAndSaveToCsv("The popDf list", popDf, 5, false, false);
    }

}
