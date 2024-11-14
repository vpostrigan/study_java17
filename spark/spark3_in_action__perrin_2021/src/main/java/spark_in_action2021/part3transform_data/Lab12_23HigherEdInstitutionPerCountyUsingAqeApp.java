package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.Logs;

/**
 * Performs a join between 3 datasets to build a list of higher education
 * institutions per county.
 * <p>
 * This example illustrates how to use adaptive query execution (AQE), a
 * feature added in Spark v3.
 * <p>
 * Adaptive Query Execution (AQE) is an optimization technique in Spark SQL
 * that makes use of the runtime statistics to choose the most efficient query execution plan,
 * which is enabled by default since Apache Spark 3.2.0
 *
 * @author jgp
 */
public class Lab12_23HigherEdInstitutionPerCountyUsingAqeApp {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab12_23HigherEdInstitutionPerCountyUsingAqeApp app =
                new Lab12_23HigherEdInstitutionPerCountyUsingAqeApp();
        boolean useAqe = false;
        app.start(allLogs, useAqe);
        app.start(allLogs, useAqe);
        app.start(allLogs, useAqe);
        app.start(allLogs, useAqe);
        app.start(allLogs, useAqe);
    }

    private void start(Logs allLogs, boolean useAqe) {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Join using AQE")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", useAqe)
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
                .drop("splitZipCode");
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
                .drop("tot_ratio");
        allLogs.showAndSaveToCsv("[3] Counties / ZIP Codes (HUD)", countyZipDf, 3, false, true);

        long t0 = System.currentTimeMillis();

        // Institutions per county id
        Dataset<Row> institPerCountyDf = higherEdDf.join(countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");

        // Institutions per county name
        institPerCountyDf = institPerCountyDf.join(censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");

        // Action
        institPerCountyDf = institPerCountyDf.cache();
        Row[] rows = (Row[]) institPerCountyDf.collect();

        long t1 = System.currentTimeMillis();
        System.out.println("AQE is " + useAqe +
                ", join operations took: " + (t1 - t0) + " ms.");

        spark.stop();
    }
/**
 2022-08-21 22:04:53.747 - INFO --- [           main] til.Version.logVersion(Version.java:133): Elasticsearch Hadoop v7.17.3 [e52f4f523a]
 22:04:58.230: [1] Census data
 +--------+-----------------------+-------+
 |countyId|county                 |pop2017|
 +--------+-----------------------+-------+
 |1001    |Autauga County, Alabama|55504  |
 |1003    |Baldwin County, Alabama|212628 |
 |1005    |Barbour County, Alabama|25270  |
 +--------+-----------------------+-------+
 only showing top 3 rows

 root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

 22:04:59.063: [1] Census data has 3220. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/1_csv
 // //
 22:05:00.265: [2] Higher education institutions (DAPIP)
 +-----------------------------------+-----+
 |location                           |zip  |
 +-----------------------------------+-----+
 |Community College of the Air Force |36112|
 |Alabama A & M University           |35762|
 |University of Alabama at Birmingham|35233|
 +-----------------------------------+-----+
 only showing top 3 rows

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)

 22:05:01.072: [2] Higher education institutions (DAPIP) has 9439. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/2_csv
 // //
 22:05:01.874: [3] Counties / ZIP Codes (HUD)
 +------+-----+
 |county|zip  |
 +------+-----+
 |1001  |36701|
 |1001  |36051|
 |1001  |36006|
 +------+-----+
 only showing top 3 rows

 root
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

 22:05:02.408: [3] Counties / ZIP Codes (HUD) has 53993. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/3_csv
 // //
 AQE is false, join operations took: 1270 ms.
 22:05:04.232: [1] Census data
 +--------+-----------------------+-------+
 |countyId|county                 |pop2017|
 +--------+-----------------------+-------+
 |1001    |Autauga County, Alabama|55504  |
 |1003    |Baldwin County, Alabama|212628 |
 |1005    |Barbour County, Alabama|25270  |
 +--------+-----------------------+-------+
 only showing top 3 rows

 root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

 22:05:04.458: [1] Census data has 3220. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/4_csv
 // //
 22:05:05.010: [2] Higher education institutions (DAPIP)
 +-----------------------------------+-----+
 |location                           |zip  |
 +-----------------------------------+-----+
 |Community College of the Air Force |36112|
 |Alabama A & M University           |35762|
 |University of Alabama at Birmingham|35233|
 +-----------------------------------+-----+
 only showing top 3 rows

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)

 22:05:05.440: [2] Higher education institutions (DAPIP) has 9439. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/5_csv
 // //
 22:05:05.957: [3] Counties / ZIP Codes (HUD)
 +------+-----+
 |county|zip  |
 +------+-----+
 |1001  |36701|
 |1001  |36051|
 |1001  |36006|
 +------+-----+
 only showing top 3 rows

 root
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

 22:05:06.248: [3] Counties / ZIP Codes (HUD) has 53993. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/6_csv
 // //
 AQE is false, join operations took: 598 ms.
 22:05:07.280: [1] Census data
 +--------+-----------------------+-------+
 |countyId|county                 |pop2017|
 +--------+-----------------------+-------+
 |1001    |Autauga County, Alabama|55504  |
 |1003    |Baldwin County, Alabama|212628 |
 |1005    |Barbour County, Alabama|25270  |
 +--------+-----------------------+-------+
 only showing top 3 rows

 root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

 22:05:07.505: [1] Census data has 3220. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/7_csv
 // //
 22:05:07.909: [2] Higher education institutions (DAPIP)
 +-----------------------------------+-----+
 |location                           |zip  |
 +-----------------------------------+-----+
 |Community College of the Air Force |36112|
 |Alabama A & M University           |35762|
 |University of Alabama at Birmingham|35233|
 +-----------------------------------+-----+
 only showing top 3 rows

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)

 22:05:08.204: [2] Higher education institutions (DAPIP) has 9439. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/8_csv
 // //
 22:05:08.622: [3] Counties / ZIP Codes (HUD)
 +------+-----+
 |county|zip  |
 +------+-----+
 |1001  |36701|
 |1001  |36051|
 |1001  |36006|
 +------+-----+
 only showing top 3 rows

 root
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

 22:05:08.878: [3] Counties / ZIP Codes (HUD) has 53993. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/9_csv
 // //
 AQE is false, join operations took: 503 ms.
 22:05:09.806: [1] Census data
 +--------+-----------------------+-------+
 |countyId|county                 |pop2017|
 +--------+-----------------------+-------+
 |1001    |Autauga County, Alabama|55504  |
 |1003    |Baldwin County, Alabama|212628 |
 |1005    |Barbour County, Alabama|25270  |
 +--------+-----------------------+-------+
 only showing top 3 rows

 root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

 22:05:09.992: [1] Census data has 3220. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/10_csv
 // //
 22:05:10.344: [2] Higher education institutions (DAPIP)
 +-----------------------------------+-----+
 |location                           |zip  |
 +-----------------------------------+-----+
 |Community College of the Air Force |36112|
 |Alabama A & M University           |35762|
 |University of Alabama at Birmingham|35233|
 +-----------------------------------+-----+
 only showing top 3 rows

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)

 22:05:10.618: [2] Higher education institutions (DAPIP) has 9439. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/11_csv
 // //
 22:05:11.002: [3] Counties / ZIP Codes (HUD)
 +------+-----+
 |county|zip  |
 +------+-----+
 |1001  |36701|
 |1001  |36051|
 |1001  |36006|
 +------+-----+
 only showing top 3 rows

 root
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

 22:05:11.255: [3] Counties / ZIP Codes (HUD) has 53993. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/12_csv
 // //
 AQE is false, join operations took: 432 ms.
 22:05:12.077: [1] Census data
 +--------+-----------------------+-------+
 |countyId|county                 |pop2017|
 +--------+-----------------------+-------+
 |1001    |Autauga County, Alabama|55504  |
 |1003    |Baldwin County, Alabama|212628 |
 |1005    |Barbour County, Alabama|25270  |
 +--------+-----------------------+-------+
 only showing top 3 rows

 root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

 22:05:12.250: [1] Census data has 3220. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/13_csv
 // //
 22:05:12.544: [2] Higher education institutions (DAPIP)
 +-----------------------------------+-----+
 |location                           |zip  |
 +-----------------------------------+-----+
 |Community College of the Air Force |36112|
 |Alabama A & M University           |35762|
 |University of Alabama at Birmingham|35233|
 +-----------------------------------+-----+
 only showing top 3 rows

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)

 22:05:12.858: [2] Higher education institutions (DAPIP) has 9439. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/14_csv
 // //
 22:05:13.244: [3] Counties / ZIP Codes (HUD)
 +------+-----+
 |county|zip  |
 +------+-----+
 |1001  |36701|
 |1001  |36051|
 |1001  |36006|
 +------+-----+
 only showing top 3 rows

 root
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

 22:05:13.488: [3] Counties / ZIP Codes (HUD) has 53993. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_23HigherEdInstitutionPerCountyUsingAqeApp/15_csv
 // //
 AQE is false, join operations took: 379 ms.

 */
}
