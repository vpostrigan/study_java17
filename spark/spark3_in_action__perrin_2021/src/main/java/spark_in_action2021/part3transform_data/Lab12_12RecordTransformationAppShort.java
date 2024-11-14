package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

/**
 * Transforming records.
 *
 * @author jgp
 */
public class Lab12_12RecordTransformationAppShort {

    public static void main(String[] args) {
        Lab12_12RecordTransformationAppShort app = new Lab12_12RecordTransformationAppShort();
        app.start();
    }

    private void start() {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Record transformations short")
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
                .withColumn("state", split(df.col("label"), ", ").getItem(1))
                .withColumn("county", split(df.col("label"), ", ").getItem(0))
                .withColumn("stateId", expr("int(id/100)"))
                .withColumn("countyId", expr("id%1000"));
        df.printSchema();
        df.sample(.01).show(5, false);

        // Performs some statistics on the intermediate dataframe
        Dataset<Row> statDf = df
                .withColumn("diff", expr("est2010-real2010"))
                .withColumn("growth", expr("est2017-est2010"))
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

        // Stats
        statDf = statDf.sort(statDf.col("growth").desc());
        System.out.println("Top 5 counties with the most growth:");
        statDf.show(5, false);

        statDf = statDf.sort(statDf.col("growth"));
        System.out.println("Top 5 counties with the most loss:");
        statDf.show(5, false);
    }
/**
 +--------------+-------+--------------------------+-----------+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 |GEO.id        |GEO.id2|GEO.display-label         |rescen42010|resbase42010|respop72010|respop72011|respop72012|respop72013|respop72014|respop72015|respop72016|respop72017|
 +--------------+-------+--------------------------+-----------+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 |0500000US05025|5025   |Cleveland County, Arkansas|8689       |8692        |8678       |8672       |8607       |8515       |8397       |8284       |8260       |8202       |
 |0500000US05117|5117   |Prairie County, Arkansas  |8715       |8719        |8724       |8600       |8484       |8377       |8349       |8301       |8271       |8248       |
 +--------------+-------+--------------------------+-----------+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
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
 |-- state: string (nullable = true)
 |-- county: string (nullable = true)
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)

 +-----+---------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+--------+-----------------+-------+--------+
 |id   |label                      |real2010|est2010|est2011|est2012|est2013|est2014|est2015|est2016|est2017|state   |county           |stateId|countyId|
 +-----+---------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+--------+-----------------+-------+--------+
 |8007 |Archuleta County, Colorado |12084   |12045  |12014  |12124  |12205  |12222  |12382  |12836  |13315  |Colorado|Archuleta County |80     |7       |
 |17085|Jo Daviess County, Illinois|22678   |22644  |22628  |22533  |22386  |22326  |22064  |21861  |21594  |Illinois|Jo Daviess County|170    |85      |
 |18055|Greene County, Indiana     |33165   |33196  |33018  |32928  |32728  |32629  |32399  |32223  |32177  |Indiana |Greene County    |180    |55      |
 |19133|Monona County, Iowa        |9243    |9248   |9235   |9089   |9028   |8894   |8863   |8799   |8740   |Iowa    |Monona County    |191    |133     |
 |21029|Bullitt County, Kentucky   |74319   |74509  |75209  |75925  |76897  |78053  |78707  |79209  |80246  |Kentucky|Bullitt County   |210    |29      |
 +-----+---------------------------+--------+-------+-------+-------+-------+-------+-------+-------+-------+--------+-----------------+-------+--------+
 only showing top 5 rows



 root
 |-- state: string (nullable = true)
 |-- county: string (nullable = true)
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)
 |-- diff: integer (nullable = true)
 |-- growth: integer (nullable = true)

 +----------+-----------------+-------+--------+----+------+
 |state     |county           |stateId|countyId|diff|growth|
 +----------+-----------------+-------+--------+----+------+
 |Alabama   |Chilton County   |10     |21      |18  |406   |
 |Alabama   |Sumter County    |11     |119     |-33 |-1043 |
 |California|Glenn County     |60     |21      |8   |-36   |
 |California|Orange County    |60     |59      |6884|173284|
 |Colorado  |San Miguel County|81     |113     |-3  |611   |
 +----------+-----------------+-------+--------+----+------+
 only showing top 5 rows



 Top 5 counties with the most growth:
 +----------+------------------+-------+--------+-----+------+
 |state     |county            |stateId|countyId|diff |growth|
 +----------+------------------+-------+--------+-----+------+
 |Texas     |Harris County     |482    |201     |15395|545126|
 |Arizona   |Maricopa County   |40     |13      |7527 |482389|
 |California|Los Angeles County|60     |37      |5885 |339017|
 |Washington|King County       |530    |33      |6129 |251271|
 |Nevada    |Clark County      |320    |3       |1637 |251173|
 +----------+------------------+-------+--------+-----+------+
 only showing top 5 rows

 Top 5 counties with the most loss:
 +-----------+------------------+-------+--------+-----+------+
 |state      |county            |stateId|countyId|diff |growth|
 +-----------+------------------+-------+--------+-----+------+
 |Michigan   |Wayne County      |261    |163     |-5515|-61453|
 |Puerto Rico|San Juan Municipio|721    |127     |-1082|-56956|
 |Ohio       |Cuyahoga County   |390    |35      |-1922|-29686|
 |Puerto Rico|Bayamón Municipio |720    |21      |-470 |-28081|
 |Puerto Rico|Ponce Municipio   |721    |113     |-604 |-24864|
 +-----------+------------------+-------+--------+-----+------+
 only showing top 5 rows
 */
}
