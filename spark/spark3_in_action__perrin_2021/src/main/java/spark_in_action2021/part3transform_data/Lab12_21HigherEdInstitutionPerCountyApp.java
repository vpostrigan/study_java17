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
 * Performs a join between 3 datasets to build a list of higher education
 * institutions per county.
 *
 * @author jgp
 */
public class Lab12_21HigherEdInstitutionPerCountyApp {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab12_21HigherEdInstitutionPerCountyApp app =
                new Lab12_21HigherEdInstitutionPerCountyApp();
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

        // Institutions per county id
        Dataset<Row> institPerCountyDf = higherEdDf.join(countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");
        allLogs.show("Institutions per county id", institPerCountyDf, 3, false, false);

        allLogs.show("Higher education institutions inner-joined with HUD",
                institPerCountyDf
                        .filter(higherEdDf.col("zip").equalTo(27517)), 5, false, true);

        {
            // --------------------------
            // - "Temporary" drop columns

            // Note:
            // This block is not doing anything except illustrating that the drop()
            // method needs to be used carefully.

            // Dropping all zip columns
            allLogs.showAndSaveToCsv("Attempt to drop the zip column",
                    institPerCountyDf
                            .drop("zip"), 3, false, false);

            // Dropping the zip column inherited from the higher ed dataframe
            allLogs.showAndSaveToCsv("Attempt to drop the zip column (higherEdDf)",
                    institPerCountyDf
                            .drop(higherEdDf.col("zip")), 3, false, false);
            // --------------------------
        }

        // Institutions per county name
        institPerCountyDf = institPerCountyDf.join(censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");
        allLogs.show("Institutions per county name", institPerCountyDf, 3, false, false);

        allLogs.show("Higher education institutions left-joined with census",
                institPerCountyDf
                        .filter(higherEdDf.col("zip").equalTo(27517)), 5, false, true);

        // Final clean up
        institPerCountyDf = institPerCountyDf
                .drop(higherEdDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                .distinct();
        allLogs.showAndSaveToCsv("Final list", institPerCountyDf, 3, false, false);

        allLogs.showAndSaveToCsv("Higher education institutions in ZIP Code 27517 (NC)",
                institPerCountyDf
                        .filter(higherEdDf.col("zip").equalTo(27517)), 3, false, false);
        allLogs.showAndSaveToCsv("Higher education institutions in ZIP Code 02138 (MA)",
                institPerCountyDf
                        .filter(higherEdDf.col("zip").equalTo(2138)), 3, false, false);
        allLogs.showAndSaveToCsv("Institutions with improper counties",
                institPerCountyDf
                        .filter("county is null"), 3, false, false);

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
/**
 23:10:42.236: [1] Census data
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

 23:10:43.051: [1] Census data has 3220. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/1_csv
 // //
 23:10:44.426: [2] Higher education institutions (DAPIP)
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

 23:10:45.240: [2] Higher education institutions (DAPIP) has 9439. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/2_csv
 // //
 23:10:46.073: [3] Counties / ZIP Codes (HUD)
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

 23:10:46.725: [3] Counties / ZIP Codes (HUD) has 53993. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/3_csv
 // //
 23:10:46.864: Institutions per county id
 +-----------------------------------+-----+------+-----+
 |location                           |zip  |county|zip  |
 +-----------------------------------+-----+------+-----+
 |Community College of the Air Force |36112|1101  |36112|
 |Alabama A & M University           |35762|1089  |35762|
 |University of Alabama at Birmingham|35233|1073  |35233|
 +-----------------------------------+-----+------+-----+
 only showing top 3 rows

 23:10:47.436: Institutions per county id has 11978.
 // //
 23:10:48.139: Higher education institutions inner-joined with HUD
 +--------------------------+-----+------+-----+
 |location                  |zip  |county|zip  |
 +--------------------------+-----+------+-----+
 |English Learning Institute|27517|37135 |27517|
 |English Learning Institute|27517|37063 |27517|
 |English Learning Institute|27517|37037 |27517|
 +--------------------------+-----+------+-----+

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

 23:10:48.621: Higher education institutions inner-joined with HUD has 3.
 // //
 23:10:49.086: Attempt to drop the zip column
 +-----------------------------------+------+
 |location                           |county|
 +-----------------------------------+------+
 |Community College of the Air Force |1101  |
 |Alabama A & M University           |1089  |
 |University of Alabama at Birmingham|1073  |
 +-----------------------------------+------+
 only showing top 3 rows

 23:10:49.889: Attempt to drop the zip column has 11978. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/4_csv
 // //
 23:10:50.259: Attempt to drop the zip column (higherEdDf)
 +-----------------------------------+------+-----+
 |location                           |county|zip  |
 +-----------------------------------+------+-----+
 |Community College of the Air Force |1101  |36112|
 |Alabama A & M University           |1089  |35762|
 |University of Alabama at Birmingham|1073  |35233|
 +-----------------------------------+------+-----+
 only showing top 3 rows

 23:10:51.002: Attempt to drop the zip column (higherEdDf) has 11978. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/5_csv
 // //
 23:10:51.339: Institutions per county name
 +-----------------------------------+-----+------+-----+--------+--------------------------+-------+
 |location                           |zip  |county|zip  |countyId|county                    |pop2017|
 +-----------------------------------+-----+------+-----+--------+--------------------------+-------+
 |Community College of the Air Force |36112|1101  |36112|1101    |Montgomery County, Alabama|226646 |
 |Alabama A & M University           |35762|1089  |35762|1089    |Madison County, Alabama   |361046 |
 |University of Alabama at Birmingham|35233|1073  |35233|1073    |Jefferson County, Alabama |659197 |
 +-----------------------------------+-----+------+-----+--------+--------------------------+-------+
 only showing top 3 rows

 23:10:51.738: Institutions per county name has 11978.
 // //
 23:10:52.197: Higher education institutions left-joined with census
 +--------------------------+-----+------+-----+--------+------------------------------+-------+
 |location                  |zip  |county|zip  |countyId|county                        |pop2017|
 +--------------------------+-----+------+-----+--------+------------------------------+-------+
 |English Learning Institute|27517|37135 |27517|37135   |Orange County, North Carolina |144946 |
 |English Learning Institute|27517|37063 |27517|37063   |Durham County, North Carolina |311640 |
 |English Learning Institute|27517|37037 |27517|37037   |Chatham County, North Carolina|71472  |
 +--------------------------+-----+------+-----+--------+------------------------------+-------+

 root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

 23:10:52.710: Higher education institutions left-joined with census has 3.
 // //
 23:10:53.163: Final list
 +----------------------------------------+-----+-----------------------------+-------+
 |location                                |zip  |county                       |pop2017|
 +----------------------------------------+-----+-----------------------------+-------+
 |California State University - Sacramento|95819|Sacramento County, California|1530615|
 |Clearwater Christian College            |33759|Pinellas County, Florida     |970637 |
 |Florida Southern College                |33801|Polk County, Florida         |686483 |
 +----------------------------------------+-----+-----------------------------+-------+
 only showing top 3 rows

 23:10:56.061: Final list has 11942. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/6_csv
 // //
 23:10:57.523: Higher education institutions in ZIP Code 27517 (NC)
 +--------------------------+-----+------------------------------+-------+
 |location                  |zip  |county                        |pop2017|
 +--------------------------+-----+------------------------------+-------+
 |English Learning Institute|27517|Chatham County, North Carolina|71472  |
 |English Learning Institute|27517|Durham County, North Carolina |311640 |
 |English Learning Institute|27517|Orange County, North Carolina |144946 |
 +--------------------------+-----+------------------------------+-------+

 23:11:02.973: Higher education institutions in ZIP Code 27517 (NC) has 3. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/7_csv
 // //
 23:11:04.589: Higher education institutions in ZIP Code 02138 (MA)
 +-----------------------------------------------+----+-------------------------------+-------+
 |location                                       |zip |county                         |pop2017|
 +-----------------------------------------------+----+-------------------------------+-------+
 |Longy School of Music                          |2138|Middlesex County, Massachusetts|1602947|
 |The Academy at Harvard Square English Institute|2138|Middlesex County, Massachusetts|1602947|
 |Weston Jesuit School of Theology               |2138|Middlesex County, Massachusetts|1602947|
 +-----------------------------------------------+----+-------------------------------+-------+
 only showing top 3 rows
tra
 23:11:08.113: Higher education institutions in ZIP Code 02138 (MA) has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/8_csv
 // //
 23:11:09.784: Institutions with improper counties
 +--------------------------------+-----+------+-------+
 |location                        |zip  |county|pop2017|
 +--------------------------------+-----+------+-------+
 |Mariacy Beauty Academy          |96910|null  |null   |
 |Pacific Islands University      |96913|null  |null   |
 |University of the Virgin Islands|802  |null  |null   |
 +--------------------------------+-----+------+-------+
 only showing top 3 rows

 23:11:12.322: Institutions with improper counties has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/9_csv
 // //
 23:11:12.930: The aggDf list
 +------------------------------+--------+-----+
 |county                        |pop2017 |count|
 +------------------------------+--------+-----+
 |Los Angeles County, California|10163507|418  |
 |Cook County, Illinois         |5211263 |183  |
 |New York County, New York     |1664727 |177  |
 |Orange County, California     |3190400 |126  |
 |Miami-Dade County, Florida    |2751796 |122  |
 +------------------------------+--------+-----+
 only showing top 5 rows

 23:11:56.109: The aggDf list has 2035. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/10_csv
 // //
 23:12:11.966: The popDf list
 +------------------------------------+-------+-----+------------------+
 |county                              |pop2017|count|institutionPer10k |
 +------------------------------------+-------+-----+------------------+
 |Trujillo Alto Municipio, Puerto Rico|66675  |38   |5.699287589051369 |
 |San Lorenzo Municipio, Puerto Rico  |37379  |14   |3.745418550523021 |
 |Morovis Municipio, Puerto Rico      |31092  |11   |3.5378875595008363|
 |Manatí Municipio, Puerto Rico       |39103  |12   |3.0688182492391887|
 |Gurabo Municipio, Puerto Rico       |47109  |14   |2.9718312848924833|
 +------------------------------------+-------+-----+------------------+
 only showing top 5 rows

 23:12:53.722: The popDf list has 1323. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_21HigherEdInstitutionPerCountyApp/11_csv
 */
}
