package spark_in_action2021.part3transform_data

import java.util.Properties

import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * NYC schools analytics.
 *
 * @author rambabu.posa
 */
class NewYorkSchoolsToPostgreSqlScalaApp {
  private val log = LoggerFactory.getLogger(classOf[NewYorkSchoolsToPostgreSqlScalaApp])

  def start(): Unit = {
    val t0: Long = System.currentTimeMillis

    val spark = SparkSession.builder
      .appName("NYC schools to PostgreSQL")
      .master("local[*]")
      .getOrCreate

    val t1 = System.currentTimeMillis

    var df = loadDataUsing2018Format(spark,
      Seq("data/chapter15/nyc_school_attendance/2018*.csv.gz"))

    df = df.unionByName(loadDataUsing2015Format(spark,
      Seq("data/chapter15/nyc_school_attendance/2015*.csv.gz")))

    df = df.unionByName(loadDataUsing2006Format(spark,
      Seq("data/chapter15/nyc_school_attendance/200*.csv.gz",
        "data/chapter15/nyc_school_attendance/2012*.csv.gz")))

    val t2 = System.currentTimeMillis

    val dbConnectionUrl = "jdbc:postgresql://localhost/postgres"

    // Properties to connect to the database,
    // the JDBC driver is part of our build.sbt
    val prop = new Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "somePassword")

    // Write in a table called ch02
    df.write.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "ch13_nyc_schools", prop)
    val t3 = System.currentTimeMillis

    log.info(s"Dataset contains ${df.count} rows, processed in ${t3 - t0} ms.")
    log.info(s"Spark init ... ${t1 - t0} ms.")
    log.info(s"Ingestion .... ${t2 - t1} ms.")
    log.info(s"Output ....... ${t3 - t2} ms.")

    df.sample(.5).show(5)
    df.printSchema()

    spark.stop
  }

  /**
   * Loads a data file matching the 2018 format, then prepares it.
   */
  private def loadDataUsing2018Format(spark: SparkSession, fileNames: Seq[String]): DataFrame = {
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

    df.withColumn("schoolYear", lit(2018))
  }

  /**
   * Load a data file matching the 2006 format.
   */
  private def loadDataUsing2006Format(spark: SparkSession, fileNames: Seq[String]): DataFrame =
    loadData(spark, fileNames, "yyyyMMdd")

  /**
   * Load a data file matching the 2015 format.
   */
  private def loadDataUsing2015Format(spark: SparkSession, fileNames: Seq[String]): DataFrame =
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

    df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4))
  }

}

object NewYorkSchoolsToPostgreSqlScalaApplication {

  def main(args: Array[String]): Unit = {
    val app = new NewYorkSchoolsToPostgreSqlScalaApp
    app.start
  }

}
/*
2022-11-06 19:48:28.969 - INFO --- [           main] orkSchoolsToPostgreSqlScalaApp.scala:53): Dataset contains 3398803 rows, processed in 70903 ms.
2022-11-06 19:48:28.969 - INFO --- [           main] orkSchoolsToPostgreSqlScalaApp.scala:54): Spark init ... 3537 ms.
2022-11-06 19:48:28.970 - INFO --- [           main] orkSchoolsToPostgreSqlScalaApp.scala:55): Ingestion .... 3080 ms.
2022-11-06 19:48:28.970 - INFO --- [           main] orkSchoolsToPostgreSqlScalaApp.scala:56): Output ....... 64286 ms.
+--------+----------+--------+-------+------+--------+----------+
|schoolId|      date|enrolled|present|absent|released|schoolYear|
+--------+----------+--------+-------+------+--------+----------+
|  01M015|2018-09-05|     172|     19|   153|       0|      2018|
|  01M015|2018-09-06|     171|     17|   154|       0|      2018|
|  01M015|2018-09-07|     172|     14|   158|       0|      2018|
|  01M015|2018-09-12|     173|      7|   166|       0|      2018|
|  01M015|2018-09-18|     174|      7|   167|       0|      2018|
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