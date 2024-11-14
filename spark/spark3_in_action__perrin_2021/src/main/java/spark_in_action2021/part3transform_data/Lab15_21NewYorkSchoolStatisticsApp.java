package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.floor;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NYC schools analytics.
 *
 * @author jgp
 */
public class Lab15_21NewYorkSchoolStatisticsApp {
    private static Logger log = LoggerFactory.getLogger(Lab15_21NewYorkSchoolStatisticsApp.class);

    private SparkSession spark = null;

    public static void main(String[] args) {
        Lab15_21NewYorkSchoolStatisticsApp app = new Lab15_21NewYorkSchoolStatisticsApp();
        app.start();
    }

    private void start() {
        spark = SparkSession.builder()
                .appName("NYC schools analytics")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> masterDf =
                loadDataUsing2018Format(
                        "data/chapter15/nyc_school_attendance/2018*.csv.gz");
        masterDf.show(5);
/*
+--------+----------+--------+-------+------+--------+----------+
|schoolId|      date|enrolled|present|absent|released|schoolYear|
+--------+----------+--------+-------+------+--------+----------+
|  01M015|2018-09-05|     172|     19|   153|       0|      2018|
|  01M015|2018-09-06|     171|     17|   154|       0|      2018|
|  01M015|2018-09-07|     172|     14|   158|       0|      2018|
|  01M015|2018-09-12|     173|      7|   166|       0|      2018|
|  01M015|2018-09-13|     173|      9|   164|       0|      2018|
+--------+----------+--------+-------+------+--------+----------+
only showing top 5 rows
 */
        Dataset<Row> d0 =
                loadDataUsing2015Format(
                        "data/chapter15/nyc_school_attendance/2015*.csv.gz");
        d0.show(5);
/*
+--------+----------+----------+--------+-------+------+--------+
|schoolId|      date|schoolYear|enrolled|present|absent|released|
+--------+----------+----------+--------+-------+------+--------+
|  01M015|2016-01-04|      2015|     168|    157|    11|       0|
|  01M015|2016-01-05|      2015|     168|    153|    15|       0|
|  01M015|2016-01-06|      2015|     168|    163|     5|       0|
|  01M015|2016-01-07|      2015|     168|    154|    14|       0|
|  01M015|2016-01-08|      2015|     168|    158|    10|       0|
+--------+----------+----------+--------+-------+------+--------+
only showing top 5 rows
 */
        masterDf = masterDf.unionByName(d0);

        d0 = loadDataUsing2006Format(
                "data/chapter15/nyc_school_attendance/200*.csv.gz",
                "data/chapter15/nyc_school_attendance/2012*.csv.gz");
        d0.show(5);
/*
+--------+----------+----------+--------+-------+------+--------+
|schoolId|      date|schoolYear|enrolled|present|absent|released|
+--------+----------+----------+--------+-------+------+--------+
|  01M015|2012-09-06|      2012|     165|    140|    25|       0|
|  01M015|2012-09-07|      2012|     168|    144|    24|       0|
|  01M015|2012-09-10|      2012|     167|    154|    13|       0|
|  01M015|2012-09-11|      2012|     169|    154|    15|       0|
|  01M015|2012-09-12|      2012|     170|    159|    11|       0|
+--------+----------+----------+--------+-------+------+--------+
only showing top 5 rows
 */
        masterDf = masterDf.unionByName(d0);
        d0 = null;

        masterDf = masterDf.cache();

        // Shows at most 5 rows from the dataframe - this is the dataframe we
        // can use to build our aggregations on
        log.debug("Dataset contains {} rows", masterDf.count());
        masterDf.sample(.5).show(5);
        masterDf.printSchema();
/*
2022-10-31 12:23:24.138 -DEBUG --- [           main] 15_21NewYorkSchoolStatisticsApp.java:64): Dataset contains 3398803 rows
+--------+----------+--------+-------+------+--------+----------+
|schoolId|      date|enrolled|present|absent|released|schoolYear|
+--------+----------+--------+-------+------+--------+----------+
|  01M015|2018-09-07|     172|     14|   158|       0|      2018|
|  01M015|2018-09-12|     173|      7|   166|       0|      2018|
|  01M015|2018-09-13|     173|      9|   164|       0|      2018|
|  01M015|2018-09-14|     173|     11|   162|       0|      2018|
|  01M015|2018-09-17|     173|     10|   163|       0|      2018|
+--------+----------+--------+-------+------+--------+----------+
only showing top 5 rows

root
 |-- schoolId: string (nullable = true)
 |-- date: date (nullable = true)
 |-- enrolled: integer (nullable = true)
 |-- present: integer (nullable = true)
 |-- absent: integer (nullable = true)
 |-- released: integer (nullable = true)
 |-- schoolYear: string (nullable = true)
 */

        // //

        // Unique schools
        Dataset<Row> uniqueSchoolsDf = masterDf
                .select("schoolId")
                .distinct();
        log.debug("Dataset contains {} unique schools", uniqueSchoolsDf.count());
/*
2022-10-31 12:23:28.228 -DEBUG --- [           main] 15_21NewYorkSchoolStatisticsApp.java:72): Dataset contains 1865 unique schools
 */

        // Calculating the average enrollment for each school
        Dataset<Row> averageEnrollmentDf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .avg("enrolled", "present", "absent")
                .orderBy("schoolId", "schoolYear");
        log.info("Average enrollment for each school");
        averageEnrollmentDf.show(20);
/*
2022-10-31 12:23:28.327 - INFO --- [           main] 15_21NewYorkSchoolStatisticsApp.java:79): Average enrollment for each school
+--------+----------+------------------+------------------+------------------+
|schoolId|schoolYear|     avg(enrolled)|      avg(present)|       avg(absent)|
+--------+----------+------------------+------------------+------------------+
|  01M015|      2006|248.68279569892474|223.90860215053763|24.774193548387096|
|  01M015|      2007| 251.5837837837838|225.72972972972974|24.843243243243244|
|  01M015|      2008|243.82967032967034|215.57692307692307|28.071428571428573|
|  01M015|      2009|213.20111731843576| 190.0949720670391| 22.76536312849162|
|  01M015|      2010|199.28176795580112|180.02209944751382| 19.12707182320442|
|  01M015|      2011|186.19672131147541| 172.5737704918033|13.382513661202186|
|  01M015|      2012| 184.5561797752809|168.73033707865167| 15.52247191011236|
|  01M015|      2013|193.94444444444446|177.88888888888889| 15.46111111111111|
|  01M015|      2014|184.23626373626374|170.77472527472528|13.043956043956044|
|  01M015|      2015|167.79775280898878| 156.0056179775281|11.634831460674157|
|  01M015|      2016|173.95454545454547|162.53977272727272|11.329545454545455|
|  01M015|      2017|187.06741573033707|175.20224719101122|11.674157303370787|
|  01M015|      2018|171.86486486486487| 9.864864864864865|             162.0|
|  01M019|      2006| 310.4193548387097|282.86559139784947|27.123655913978496|
|  01M019|      2007|333.04324324324324|303.43783783783783|29.605405405405406|
|  01M019|      2008|326.04945054945057| 294.7142857142857|31.335164835164836|
|  01M019|      2009| 320.6145251396648|  293.804469273743|26.067039106145252|
|  01M019|      2010|322.65193370165747| 296.6685082872928|25.502762430939228|
|  01M019|      2011| 328.1803278688525| 304.7103825136612| 22.94535519125683|
|  01M019|      2012|300.15168539325845| 277.2865168539326|22.089887640449437|
+--------+----------+------------------+------------------+------------------+
only showing top 20 rows
 */

        // Evolution of # of students in the schools
        Dataset<Row> studentCountPerYearDf = averageEnrollmentDf
                .withColumnRenamed("avg(enrolled)", "enrolled")
                .groupBy(col("schoolYear"))
                .agg(sum("enrolled").as("enrolled"))
                .withColumn(
                        "enrolled",
                        floor("enrolled").cast(DataTypes.LongType))
                .orderBy("schoolYear");
        log.info("Evolution of # of students per year");
        studentCountPerYearDf.show(20);
/*
2022-10-31 12:23:31.706 - INFO --- [           main] 15_21NewYorkSchoolStatisticsApp.java:91): Evolution of # of students per year
+----------+--------+
|schoolYear|enrolled|
+----------+--------+
|      2006|  994597|
|      2007|  978064|
|      2008|  976091|
|      2009|  987968|
|      2010|  990097|
|      2011|  990235|
|      2012|  986900|
|      2013|  985040|
|      2014|  983189|
|      2015|  977576|
|      2016|  971130|
|      2017|  963861|
|      2018|  954701|
+----------+--------+
 */

        Row maxStudentRow = studentCountPerYearDf
                .orderBy(col("enrolled").desc())
                .first();
        String year = maxStudentRow.getString(0);
        long max = maxStudentRow.getLong(1);
        log.debug("{} was the year with most students, "
                + "the district served {} students.", year, max);
/*
2022-10-31 12:23:55.426 -DEBUG --- [           main] 15_21NewYorkSchoolStatisticsApp.java:98): 2006 was the year with most students, the district served 994597 students.
 */

        // Evolution of # of students in the schools
        Dataset<Row> relativeStudentCountPerYearDf = studentCountPerYearDf
                .withColumn("max", lit(max))
                .withColumn("delta", expr("max - enrolled"))
                .drop("max")
                .orderBy("schoolYear");
        log.info("Variation on the enrollment from {}:", year);
        relativeStudentCountPerYearDf.show(20);
/*
2022-10-31 12:23:55.646 - INFO --- [           main] 5_21NewYorkSchoolStatisticsApp.java:107): Variation on the enrollment from 2006:
+----------+--------+-----+
|schoolYear|enrolled|delta|
+----------+--------+-----+
|      2006|  994597|    0|
|      2007|  978064|16533|
|      2008|  976091|18506|
|      2009|  987968| 6629|
|      2010|  990097| 4500|
|      2011|  990235| 4362|
|      2012|  986900| 7697|
|      2013|  985040| 9557|
|      2014|  983189|11408|
|      2015|  977576|17021|
|      2016|  971130|23467|
|      2017|  963861|30736|
|      2018|  954701|39896|
+----------+--------+-----+
 */
        // Most enrolled per school for each year
        Dataset<Row> maxEnrolledPerSchooldf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .max("enrolled")
                .orderBy("schoolId", "schoolYear");
        log.info("Maximum enrollement per school and year");
        maxEnrolledPerSchooldf.show(20);
/*
2022-10-31 12:24:07.602 - INFO --- [           main] 5_21NewYorkSchoolStatisticsApp.java:115): Maximum enrollement per school and year
+--------+----------+-------------+
|schoolId|schoolYear|max(enrolled)|
+--------+----------+-------------+
|  01M015|      2006|          256|
|  01M015|      2007|          263|
|  01M015|      2008|          256|
|  01M015|      2009|          222|
|  01M015|      2010|          210|
|  01M015|      2011|          197|
|  01M015|      2012|          191|
|  01M015|      2013|          202|
|  01M015|      2014|          188|
|  01M015|      2015|          177|
|  01M015|      2016|          184|
|  01M015|      2017|          194|
|  01M015|      2018|          174|
|  01M019|      2006|          324|
|  01M019|      2007|          338|
|  01M019|      2008|          335|
|  01M019|      2009|          326|
|  01M019|      2010|          329|
|  01M019|      2011|          331|
|  01M019|      2012|          304|
+--------+----------+-------------+
only showing top 20 rows
 */
        // Min absent per school for each year
        Dataset<Row> minAbsenteeDf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .min("absent")
                .orderBy("schoolId", "schoolYear");
        log.info("Minimum absenteeism per school and year");
        minAbsenteeDf.show(20);
/*
2022-10-31 12:24:09.250 - INFO --- [           main] 5_21NewYorkSchoolStatisticsApp.java:123): Minimum absenteeism per school and year
+--------+----------+-----------+
|schoolId|schoolYear|min(absent)|
+--------+----------+-----------+
|  01M015|      2006|          9|
|  01M015|      2007|         10|
|  01M015|      2008|          7|
|  01M015|      2009|          8|
|  01M015|      2010|          4|
|  01M015|      2011|          2|
|  01M015|      2012|          3|
|  01M015|      2013|          1|
|  01M015|      2014|          0|
|  01M015|      2015|          2|
|  01M015|      2016|          2|
|  01M015|      2017|          1|
|  01M015|      2018|        150|
|  01M019|      2006|          9|
|  01M019|      2007|          9|
|  01M019|      2008|         11|
|  01M019|      2009|          7|
|  01M019|      2010|          3|
|  01M019|      2011|          5|
|  01M019|      2012|          5|
+--------+----------+-----------+
only showing top 20 rows
 */
        // Min absent per school for each year, as a % of enrolled
        Dataset<Row> absenteeRatioDf = masterDf
                .groupBy(col("schoolId"), col("schoolYear"))
                .agg(
                        max("enrolled").alias("enrolled"),
                        avg("absent").as("absent"));
        absenteeRatioDf.show(5);
/*
+--------+----------+--------+------------------+
|schoolId|schoolYear|enrolled|            absent|
+--------+----------+--------+------------------+
|  05M514|      2018|     145|120.29729729729729|
|  08X123|      2018|     421| 391.6756756756757|
|  15K448|      2018|     704| 631.7297297297297|
|  17K528|      2018|     217|191.35135135135135|
|  24Q550|      2018|     791| 730.1621621621622|
+--------+----------+--------+------------------+
only showing top 5 rows
 */
        absenteeRatioDf = absenteeRatioDf
                .groupBy(col("schoolId"))
                .agg(
                        avg("enrolled").as("avg_enrolled"),
                        avg("absent").as("avg_absent"))
                .withColumn("%", expr("avg_absent / avg_enrolled * 100"))
                .filter(col("avg_enrolled").$greater(10))
                .orderBy("%", "avg_enrolled");
        log.info("Schools with the least absenteeism");
        absenteeRatioDf.show(5);
/*
2022-10-31 12:24:10.771 - INFO --- [           main] 5_21NewYorkSchoolStatisticsApp.java:140): Schools with the least absenteeism
+--------+------------------+--------------------+-------------------+
|schoolId|      avg_enrolled|          avg_absent|                  %|
+--------+------------------+--------------------+-------------------+
|  11X113|              16.0|                 0.0|                0.0|
|  29Q420|              20.0|                 0.0|                0.0|
|  11X416|21.333333333333332|                 0.0|                0.0|
|  19K435|33.333333333333336|                 0.0|                0.0|
|  27Q481|26.333333333333332|0.010810810810810811|0.04105371193978789|
+--------+------------------+--------------------+-------------------+
only showing top 5 rows
 */

        log.info("Schools with the most absenteeism");
        absenteeRatioDf
                .orderBy(col("%").desc())
                .show(5);
/*
2022-10-31 12:24:20.083 - INFO --- [           main] 5_21NewYorkSchoolStatisticsApp.java:143): Schools with the most absenteeism
+--------+------------+------------------+-----------------+
|schoolId|avg_enrolled|        avg_absent|                %|
+--------+------------+------------------+-----------------+
|  25Q379|       154.0| 148.0810810810811|96.15654615654617|
|  75X596|       407.0|371.27027027027026|91.22119662660204|
|  16K898|        46.0|  41.4054054054054| 90.0117508813161|
|  19K907|        41.0|  36.4054054054054|88.79367172050098|
|  09X594|       198.0|174.54054054054055|88.15178815178815|
+--------+------------+------------------+-----------------+
only showing top 5 rows
 */
    }

    /**
     * Loads a data file matching the 2018 format, then prepares it.
     */
    private Dataset<Row> loadDataUsing2018Format(String... fileNames) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("schoolId", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
                DataTypes.createStructField("present", DataTypes.IntegerType, false),
                DataTypes.createStructField("absent", DataTypes.IntegerType, false),
                DataTypes.createStructField("released", DataTypes.IntegerType, false)});

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("dateFormat", "yyyyMMdd")
                .schema(schema)
                .load(fileNames);

        return df
                .withColumn("schoolYear", lit(2018));
    }

    /**
     * Load a data file matching the 2006 format.
     */
    private Dataset<Row> loadDataUsing2006Format(String... fileNames) {
        return loadData(fileNames, "yyyyMMdd");
    }

    /**
     * Load a data file matching the 2015 format.
     */
    private Dataset<Row> loadDataUsing2015Format(String... fileNames) {
        return loadData(fileNames, "MM/dd/yyyy");
    }

    /**
     * Common loader for most datasets, accepts a date format as part of the parameters.
     */
    private Dataset<Row> loadData(String[] fileNames, String dateFormat) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("schoolId", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
                DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
                DataTypes.createStructField("present", DataTypes.IntegerType, false),
                DataTypes.createStructField("absent", DataTypes.IntegerType, false),
                DataTypes.createStructField("released", DataTypes.IntegerType, false)});

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("dateFormat", dateFormat)
                .schema(schema)
                .load(fileNames);

        return df
                .withColumn("schoolYear", substring(col("schoolYear"), 1, 4));
    }

}
