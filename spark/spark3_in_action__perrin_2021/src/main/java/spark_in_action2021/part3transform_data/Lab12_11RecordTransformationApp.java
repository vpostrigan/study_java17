package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Transforming records.
 *
 * @author jgp
 */
public class Lab12_11RecordTransformationApp {

    public static void main(String[] args) {
        Lab12_11RecordTransformationApp app = new Lab12_11RecordTransformationApp();
        app.start();
    }

    private void start() {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Record transformations")
                .master("local")
                .getOrCreate();

        // Ingestion of the census data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/chapter12/census/PEP_2017_PEPANNRES.csv");
        df.sample(.01).show(2, false);

        // Renaming and dropping the columns we do not need
        df = df.drop("GEO.id")
                .withColumnRenamed("GEO.id2", "id")
                .withColumnRenamed("GEO.display-label", "label")
                .withColumnRenamed("rescen42010", "real2010")
                .drop("resbase42010")
                .withColumnRenamed("respop72010", "est2010")
                .withColumnRenamed("respop72011", "est2011")
                .withColumnRenamed("respop72012", "est2012")
                .withColumnRenamed("respop72013", "est2013")
                .withColumnRenamed("respop72014", "est2014")
                .withColumnRenamed("respop72015", "est2015")
                .withColumnRenamed("respop72016", "est2016")
                .withColumnRenamed("respop72017", "est2017");
        df.printSchema();
        df.show(5);

        // Creates the additional columns
        df = df
                .withColumn("countyState",
                        functions.split(df.col("label"), ", "))
                .withColumn("stateId", functions.expr("int(id/100)"))
                .withColumn("countyId", functions.expr("id%1000"));
        df.printSchema();
        df.sample(.01).show(5, false);

        df = df
                .withColumn("state", df.col("countyState").getItem(1))
                .withColumn("county", df.col("countyState").getItem(0))
                .drop("countyState");
        df.printSchema();
        df.sample(.01).show(5, false);

        // Performs some statistics on the intermediate dataframe
        Dataset<Row> statDf = df
                .withColumn("diff", functions.expr("est2010-real2010"))
                .withColumn("growth", functions.expr("est2017-est2010"))
                .drop("id")
                .drop("label")
                .drop("real2010")
                .drop("est2010")
                .drop("est2011")
                .drop("est2012")
                .drop("est2013")
                .drop("est2014")
                .drop("est2015")
                .drop("est2016")
                .drop("est2017");
        statDf.printSchema();
        statDf.sample(.01).show(5, false);
    }

/**
 +--------------+-------+----------------------+-----------+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 |GEO.id        |GEO.id2|GEO.display-label     |rescen42010|resbase42010|respop72010|respop72011|respop72012|respop72013|respop72014|respop72015|respop72016|respop72017|
 +--------------+-------+----------------------+-----------+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 |0500000US01093|1093   |Marion County, Alabama|30776      |30776       |30816      |30650      |30480      |30220      |30200      |30116      |29922      |29833      |
 |0500000US01099|1099   |Monroe County, Alabama|23068      |23070       |23010      |22800      |22587      |22176      |21931      |21720      |21572      |21327      |
 +--------------+-------+----------------------+-----------+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 only showing top 2 rows



 root
 |-- id: integer (nullable = true)
 |-- label: string (nullable = true)
 |-- real2010: integer (nullable = true)
 |-- est2010: integer (nullable = true)
 |-- est2011: integer (nullable = true)
 |-- est2012: integer (nullable = true)
 |-- est2013: integer (nullable = true)
 |-- est2014: integer (nullable = true)
 |-- est2015: integer (nullable = true)
 |-- est2016: integer (nullable = true)
 |-- est2017: integer (nullable = true)

 +----+--------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+
 |  id|               label|real2010|est2010|est2011|est2012|est2013|est2014|est2015|est2016|est2017|
 +----+--------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+
 |1001|Autauga County, A...|   54571|  54750|  55199|  54927|  54695|  54864|  54838|  55278|  55504|
 |1003|Baldwin County, A...|  182265| 183110| 186534| 190048| 194736| 199064| 202863| 207509| 212628|
 |1005|Barbour County, A...|   27457|  27332|  27351|  27175|  26947|  26749|  26264|  25774|  25270|
 |1007|Bibb County, Alabama|   22915|  22872|  22745|  22658|  22503|  22533|  22561|  22633|  22668|
 |1009|Blount County, Al...|   57322|  57381|  57562|  57595|  57623|  57546|  57590|  57562|  58013|
 +----+--------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+
 only showing top 5 rows



 root
 |-- id: integer (nullable = true)
 |-- label: string (nullable = true)
 |-- real2010: integer (nullable = true)
 |-- est2010: integer (nullable = true)
 |-- est2011: integer (nullable = true)
 |-- est2012: integer (nullable = true)
 |-- est2013: integer (nullable = true)
 |-- est2014: integer (nullable = true)
 |-- est2015: integer (nullable = true)
 |-- est2016: integer (nullable = true)
 |-- est2017: integer (nullable = true)
 |-- countyState: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)

 +-----+-------------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+---------------------------------+-------+--------+
 |id   |label                          |real2010|est2010|est2011|est2012|est2013|est2014|est2015|est2016|est2017|countyState                      |stateId|countyId|
 +-----+-------------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+---------------------------------+-------+--------+
 |2110 |Juneau City and Borough, Alaska|31275   |31394  |32162  |32395  |32570  |32490  |32612  |32405  |32094  |[Juneau City and Borough, Alaska]|21     |110     |
 |5131 |Sebastian County, Arkansas     |125744  |125755 |126926 |127441 |127067 |126683 |127387 |127567 |128107 |[Sebastian County, Arkansas]     |51     |131     |
 |12055|Highlands County, Florida      |98786   |98635  |98451  |98205  |98071  |98482  |99891  |101558 |102883 |[Highlands County, Florida]      |120    |55      |
 |13161|Jeff Davis County, Georgia     |15068   |15107  |15158  |15198  |15050  |14937  |15015  |14970  |15025  |[Jeff Davis County, Georgia]     |131    |161     |
 |13211|Morgan County, Georgia         |17868   |17898  |17903  |17806  |17683  |17897  |17951  |18121  |18412  |[Morgan County, Georgia]         |132    |211     |
 +-----+-------------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+---------------------------------+-------+--------+
 only showing top 5 rows



 root
 |-- id: integer (nullable = true)
 |-- label: string (nullable = true)
 |-- real2010: integer (nullable = true)
 |-- est2010: integer (nullable = true)
 |-- est2011: integer (nullable = true)
 |-- est2012: integer (nullable = true)
 |-- est2013: integer (nullable = true)
 |-- est2014: integer (nullable = true)
 |-- est2015: integer (nullable = true)
 |-- est2016: integer (nullable = true)
 |-- est2017: integer (nullable = true)
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- county: string (nullable = true)

 +-----+---------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------+--------+-------------------+
 |id   |label                      |real2010|est2010|est2011|est2012|est2013|est2014|est2015|est2016|est2017|stateId|countyId|state   |county             |
 +-----+---------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------+--------+-------------------+
 |2060 |Bristol Bay Borough, Alaska|997     |1002   |1030   |980    |952    |949    |904    |905    |867    |20     |60      |Alaska  |Bristol Bay Borough|
 |5145 |White County, Arkansas     |77076   |77336  |78005  |78545  |78471  |78368  |78835  |78838  |79016  |51     |145     |Arkansas|White County       |
 |8013 |Boulder County, Colorado   |294567  |295930 |300518 |305028 |309749 |312505 |317968 |321173 |322514 |80     |13      |Colorado|Boulder County     |
 |12129|Wakulla County, Florida    |30776   |30825  |30971  |30853  |31000  |31408  |31518  |31885  |32120  |121    |129     |Florida |Wakulla County     |
 |13117|Forsyth County, Georgia    |175511  |176767 |182034 |187126 |194137 |202945 |211384 |220067 |227967 |131    |117     |Georgia |Forsyth County     |
 +-----+---------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------+--------+-------------------+
 only showing top 5 rows



 root
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- county: string (nullable = true)
 |-- diff: integer (nullable = true)
 |-- growth: integer (nullable = true)

 +-------+--------+--------+-----------------+----+------+
 |stateId|countyId|state   |county           |diff|growth|
 +-------+--------+--------+-----------------+----+------+
 |10     |37      |Alabama |Coosa County     |243 |-1028 |
 |50     |35      |Arkansas|Crittenden County|58  |-2210 |
 |120    |77      |Florida |Liberty County   |-17 |-106  |
 |120    |87      |Florida |Monroe County    |129 |3794  |
 |131    |193     |Georgia |Macon County     |-99 |-1327 |
 +-------+--------+--------+-----------------+----+------+
 only showing top 5 rows
 */

}
