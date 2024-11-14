package spark_in_action2021.part3transform_data

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.slf4j.LoggerFactory

/**
 * NYC schools analytics.
 *
 * @author rambabu.posa
 */
class Lab15_12NewYorkSchoolStatisticsScalaApp {

  private val log = LoggerFactory.getLogger(classOf[Lab15_12NewYorkSchoolStatisticsScalaApp])

  def start(): Unit = {

    val spark = SparkSession.builder
      .appName("NYC schools analytics")
      .master("local[*]")
      .getOrCreate

    var masterDf = loadDataUsing2018Format(spark,
      "data/chapter15/nyc_school_attendance/2018*.csv.gz")

    masterDf = masterDf.unionByName(loadDataUsing2015Format(spark,
      "data/chapter15/nyc_school_attendance/2015*.csv.gz"))

    masterDf = masterDf.unionByName(loadDataUsing2006Format(spark,
      "data/chapter15/nyc_school_attendance/200*.csv.gz",
      "data/chapter15/nyc_school_attendance/2012*.csv.gz"))
    masterDf = masterDf.cache

    // Shows at most 5 rows from the dataframe - this is the dataframe we
    // can use to build our aggregations on
    log.debug("Dataset contains {} rows", masterDf.count)
    masterDf.sample(.5).show(5)
    masterDf.printSchema()

    // Unique schools
    val uniqueSchoolsDf = masterDf.select("schoolId").distinct
    log.debug("Dataset contains {} unique schools", uniqueSchoolsDf.count)

    // Calculating the average enrollment for each school
    val averageEnrollmentDf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .avg("enrolled", "present", "absent")
      .orderBy("schoolId", "schoolYear")

    log.info("Average enrollment for each school")
    averageEnrollmentDf.show(20)

    // Evolution of # of students in the schools
    val studentCountPerYearDf = averageEnrollmentDf
      .withColumnRenamed("avg(enrolled)", "enrolled")
      .groupBy(F.col("schoolYear"))
      .agg(F.sum("enrolled").as("enrolled"))
      .withColumn("enrolled", F.floor("enrolled").cast(DataTypes.LongType))
      .orderBy("schoolYear")

    log.info("Evolution of # of students per year")
    studentCountPerYearDf.show(20)

    val maxStudentRow = studentCountPerYearDf
      .orderBy(F.col("enrolled").desc)
      .first

    val year = maxStudentRow.getString(0)
    val max = maxStudentRow.getLong(1)

    log.debug(s"${year} was the year with most students, the district served ${max} students.")

    // Evolution of # of students in the schools
    val relativeStudentCountPerYearDf = studentCountPerYearDf
      .withColumn("max", F.lit(max))
      .withColumn("delta", F.expr("max - enrolled"))
      .drop("max")
      .orderBy("schoolYear")

    log.info(s"Variation on the enrollment from ${year}:")
    relativeStudentCountPerYearDf.show(20)

    // Most enrolled per school for each year
    val maxEnrolledPerSchooldf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .max("enrolled")
      .orderBy("schoolId", "schoolYear")

    log.info("Maximum enrollement per school and year")
    maxEnrolledPerSchooldf.show(20)

    // Min absent per school for each year
    val minAbsenteeDf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .min("absent")
      .orderBy("schoolId", "schoolYear")

    log.info("Minimum absenteeism per school and year")
    minAbsenteeDf.show(20)

    // Min absent per school for each year, as a % of enrolled
    var absenteeRatioDf = masterDf
      .groupBy(F.col("schoolId"), F.col("schoolYear"))
      .agg(F.max("enrolled").alias("enrolled"), F.avg("absent").as("absent"))

    absenteeRatioDf = absenteeRatioDf
      .groupBy(F.col("schoolId"))
      .agg(F.avg("enrolled").as("avg_enrolled"), F.avg("absent").as("avg_absent"))
      .withColumn("%", F.expr("avg_absent / avg_enrolled * 100"))
      .filter(F.col("avg_enrolled").gt(F.lit(10)))
      .orderBy("%", "avg_enrolled")

    log.info("Schools with the least absenteeism")
    absenteeRatioDf.show(5)

    log.info("Schools with the most absenteeism")
    absenteeRatioDf.orderBy(F.col("%").desc).show(5)

    spark.stop
  }

  /**
   * Loads a data file matching the 2018 format, then prepares it.
   */
  private def loadDataUsing2018Format(spark: SparkSession, fileNames: String*): DataFrame = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("schoolId", DataTypes.StringType, false),
      DataTypes.createStructField("date", DataTypes.DateType, false),
      DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
      DataTypes.createStructField("present", DataTypes.IntegerType, false),
      DataTypes.createStructField("absent", DataTypes.IntegerType, false),
      DataTypes.createStructField("released", DataTypes.IntegerType, false)))

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("dateFormat", "yyyyMMdd")
      .schema(schema)
      .load(fileNames: _*)

    df.withColumn("schoolYear", F.lit(2018))
  }

  /**
   * Load a data file matching the 2006 format.
   */
  private def loadDataUsing2006Format(spark: SparkSession, fileNames: String*) =
    loadData(spark, fileNames, "yyyyMMdd")

  /**
   * Load a data file matching the 2015 format.
   */
  private def loadDataUsing2015Format(spark: SparkSession, fileNames: String*): DataFrame =
    loadData(spark, fileNames, "MM/dd/yyyy")

  /**
   * Common loader for most datasets, accepts a date format as part of the parameters.
   */
  private def loadData(spark: SparkSession, fileNames: Seq[String], dateFormat: String): DataFrame = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("schoolId", DataTypes.StringType, false),
      DataTypes.createStructField("date", DataTypes.DateType, false),
      DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
      DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
      DataTypes.createStructField("present", DataTypes.IntegerType, false),
      DataTypes.createStructField("absent", DataTypes.IntegerType, false),
      DataTypes.createStructField("released", DataTypes.IntegerType, false)))

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("dateFormat", dateFormat)
      .schema(schema)
      .load(fileNames: _*)

    df.withColumn("schoolYear", F.substring(F.col("schoolYear"), 1, 4))
  }

}

object NewYorkSchoolStatisticsScalaApplication {

  def main(args: Array[String]): Unit = {
    val app = new Lab15_12NewYorkSchoolStatisticsScalaApp
    app.start
  }

}
/*
2022-11-06 19:35:08.225 -DEBUG --- [           main] ewYorkSchoolStatisticsScalaApp.scala:36): Dataset contains 3398803 rows
+--------+----------+--------+-------+------+--------+----------+
|schoolId|      date|enrolled|present|absent|released|schoolYear|
+--------+----------+--------+-------+------+--------+----------+
|  01M015|2018-09-05|     172|     19|   153|       0|      2018|
|  01M015|2018-09-20|     174|      7|   167|       0|      2018|
|  01M015|2018-09-24|     174|     13|   161|       0|      2018|
|  01M015|2018-09-25|     174|      9|   165|       0|      2018|
|  01M015|2018-10-02|     172|      7|   165|       0|      2018|
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

2022-11-06 19:35:12.156 -DEBUG --- [           main] ewYorkSchoolStatisticsScalaApp.scala:42): Dataset contains 1865 unique schools
2022-11-06 19:35:12.204 - INFO --- [           main] ewYorkSchoolStatisticsScalaApp.scala:50): Average enrollment for each school
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

2022-11-06 19:35:17.795 - INFO --- [           main] ewYorkSchoolStatisticsScalaApp.scala:61): Evolution of # of students per year
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

2022-11-06 19:35:44.003 -DEBUG --- [           main] ewYorkSchoolStatisticsScalaApp.scala:71): 2006 was the year with most students, the district served 994597 students.
2022-11-06 19:35:44.191 - INFO --- [           main] ewYorkSchoolStatisticsScalaApp.scala:80): Variation on the enrollment from 2006:
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

2022-11-06 19:35:55.351 - INFO --- [           main] ewYorkSchoolStatisticsScalaApp.scala:89): Maximum enrollement per school and year
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

2022-11-06 19:35:57.001 - INFO --- [           main] ewYorkSchoolStatisticsScalaApp.scala:98): Minimum absenteeism per school and year
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

2022-11-06 19:35:58.779 - INFO --- [           main] wYorkSchoolStatisticsScalaApp.scala:113): Schools with the least absenteeism
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

2022-11-06 19:36:06.949 - INFO --- [           main] wYorkSchoolStatisticsScalaApp.scala:116): Schools with the most absenteeism
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