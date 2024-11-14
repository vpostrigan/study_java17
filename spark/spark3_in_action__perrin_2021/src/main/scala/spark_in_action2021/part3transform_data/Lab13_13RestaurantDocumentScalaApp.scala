package spark_in_action2021.part3transform_data

import java.util.Arrays

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession, functions => F}
import org.slf4j.LoggerFactory

/**
 * Building a nested document.
 *
 * @author rambabu.posa
 */
class Lab13_13RestaurantDocumentScalaApp {
  private val log = LoggerFactory.getLogger(classOf[Lab13_13RestaurantDocumentScalaApp])

  val TEMP_COL = "temp_column"

  def start(): Unit = {
    val spark = SparkSession.builder
      .appName("Building a restaurant fact sheet")
      .master("local[*]")
      .getOrCreate

    // Ingests businesses into dataframe
    val businessDf = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/orangecounty_restaurants/businesses.CSV")

    // Ingests businesses into dataframe
    val inspectionDf = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/orangecounty_restaurants/inspections.CSV")

    // Shows at most 3 rows from the dataframe
    businessDf.show(3)
    businessDf.printSchema()

    inspectionDf.show(3)
    inspectionDf.printSchema()

    val factSheetDf = nestedJoin(businessDf, inspectionDf, "business_id",
      "business_id", "inner", "inspections")

    factSheetDf.show(3)
    factSheetDf.printSchema()

    spark.stop
  }

  /**
   * Builds a nested document from two dataframes.
   *
   * @param leftDf       : The left or master document.
   * @param rightDf      : The right or details.
   * @param leftJoinCol  : Column to link on in the left dataframe.
   * @param rightJoinCol : Column to link on in the right dataframe.
   * @param joinType     : Type of joins, any type supported by Spark.
   * @param nestedCol    : Name of the nested column.
   * @return
   */
  def nestedJoin(leftDf: Dataset[Row],
                 rightDf: Dataset[Row],
                 leftJoinCol: String,
                 rightJoinCol: String,
                 joinType: String,
                 nestedCol: String): Dataset[Row] = {

    // Performs the join
    var resDf = leftDf.join(rightDf, rightDf.col(rightJoinCol) === leftDf.col(leftJoinCol), joinType)

    // Makes a list of the left columns (the columns in the master)
    //val leftColumns: Array[Column] = getColumns(leftDf)
    val leftColumns = getColumns(leftDf)

    if (log.isDebugEnabled) {
      log.debug("  We have {} columns to work with: {}", leftColumns.length, leftColumns)
      log.debug("Schema and data:")
      resDf.printSchema()
      resDf.show(3)
    }

    // Copies all the columns from the left/master
    val allColumns: Array[Column] = Arrays.copyOf(leftColumns, leftColumns.length + 1)

    // Adds a column, which is a structure containing all the columns from
    // the details
    allColumns(leftColumns.length) = F.struct(getColumns(rightDf): _*).alias(TEMP_COL)

    // Performs a select on all columns
    resDf = resDf.select(allColumns: _*)

    if (log.isDebugEnabled) {
      log.debug("  Before nested join, we have {} rows.", resDf.count)
      resDf.printSchema()
      resDf.show(3)
    }

    resDf = resDf.groupBy(leftColumns: _*).agg(F.collect_list(F.col(TEMP_COL)).as(nestedCol))

    if (log.isDebugEnabled) {
      resDf.printSchema()
      resDf.show(3)
      log.debug("  After nested join, we have {} rows.", resDf.count)
    }

    resDf
  }

  private def getColumns(df: Dataset[Row]): Array[Column] = {
    val fieldnames = df.columns
    val columns = new Array[Column](fieldnames.length)
    var i = 0
    for (fieldname <- fieldnames) {
      columns(i) = df.col(fieldname)
      i = i + 1
    }
    columns
  }

}

object RestaurantDocumentScalaApplication {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val app = new Lab13_13RestaurantDocumentScalaApp
    app.start
  }

}
/*
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
|business_id|                name|            address|       city|state|postal_code|latitude|longitude|phone_number|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
| 4068010013|     BURGER KING 212| 600 JONES FERRY RD|   CARRBORO|   NC|      27510|    null|     null|+19199298395|
| 4068010016|CAROL WOODS CAFET...|750 WEAVER DAIRY RD|CHAPEL HILL|   NC|      27514|    null|     null|+19199183203|
| 4068010027|    COUNTRY JUNCTION|      402 WEAVER ST|   CARRBORO|   NC|      27510|    null|     null|+19199292462|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)

+-----------+-----+--------+-------+
|business_id|score|    date|   type|
+-----------+-----+--------+-------+
| 4068010013| 95.5|20121029|routine|
| 4068010013| 92.5|20130606|routine|
| 4068010013|   97|20130920|routine|
+-----------+-----+--------+-------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)

2022-10-23 23:05:51.908 -DEBUG --- [           main] 3_13RestaurantDocumentScalaApp.scala:78):   We have 9 columns to work with: [business_id, name, address, city, state, postal_code, latitude, longitude, phone_number]
2022-10-23 23:05:51.908 -DEBUG --- [           main] 3_13RestaurantDocumentScalaApp.scala:79): Schema and data:
root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)

+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
|business_id|           name|           address|    city|state|postal_code|latitude|longitude|phone_number|business_id|score|    date|   type|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 95.5|20121029|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 92.5|20130606|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013|   97|20130920|routine|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
only showing top 3 rows

2022-10-23 23:05:53.181 -DEBUG --- [           main] 3_13RestaurantDocumentScalaApp.scala:95):   Before nested join, we have 5317 rows.
root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- temp_column: struct (nullable = false)
 |    |-- business_id: string (nullable = true)
 |    |-- score: string (nullable = true)
 |    |-- date: string (nullable = true)
 |    |-- type: string (nullable = true)

+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+--------------------+
|business_id|           name|           address|    city|state|postal_code|latitude|longitude|phone_number|         temp_column|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+--------------------+
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395|{4068010013, 95.5...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395|{4068010013, 92.5...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395|{4068010013, 97, ...|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+--------------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- inspections: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- business_id: string (nullable = true)
 |    |    |-- score: string (nullable = true)
 |    |    |-- date: string (nullable = true)
 |    |    |-- type: string (nullable = true)

+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
|business_id|                name|             address|       city|state|postal_code|latitude|longitude|phone_number|         inspections|
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
| 4068011069|      FIREHOUSE SUBS|1726 FORDHAM BLVD...|CHAPEL HILL|   NC|      27514|    null|     null|+19849994793|[{4068011069, 99,...|
| 4068010196|AMANTE GOURMET PIZZA|       300 E MAIN ST|   CARRBORO|   NC|      27510|    null|     null|+19199293330|[{4068010196, 94,...|
| 4068010460|      COSMIC CANTINA|128 E FRANKLIN ST...|CHAPEL HILL|   NC|      27514|    null|     null|+19199603955|[{4068010460, 97,...|
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
only showing top 3 rows

2022-10-23 23:05:58.386 -DEBUG --- [           main] _13RestaurantDocumentScalaApp.scala:105):   After nested join, we have 301 rows.
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
|business_id|                name|             address|       city|state|postal_code|latitude|longitude|phone_number|         inspections|
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
| 4068011069|      FIREHOUSE SUBS|1726 FORDHAM BLVD...|CHAPEL HILL|   NC|      27514|    null|     null|+19849994793|[{4068011069, 99,...|
| 4068010196|AMANTE GOURMET PIZZA|       300 E MAIN ST|   CARRBORO|   NC|      27510|    null|     null|+19199293330|[{4068010196, 94,...|
| 4068010460|      COSMIC CANTINA|128 E FRANKLIN ST...|CHAPEL HILL|   NC|      27514|    null|     null|+19199603955|[{4068010460, 97,...|
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- inspections: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- business_id: string (nullable = true)
 |    |    |-- score: string (nullable = true)
 |    |    |-- date: string (nullable = true)
 |    |    |-- type: string (nullable = true)

 */