package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Custom UDF to check if in range.
 *
 * @author rambabu.posa
 */
object Lab14_21OpenedLibrariesSqlScalaApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Custom UDF to check if in range")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("isOpen", new Lab14_12IsOpenScalaUdf, DataTypes.BooleanType)

    val librariesDf = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("encoding", "cp1252")
      .load("data/chapter14/south_dublin_libraries/sdlibraries.csv")
      .drop("Administrative_Authority", "Address1", "Address2", "Town", "Postcode",
        "County", "Phone", "Email", "Website", "Image", "WGS84_Latitude", "WGS84_Longitude")

    librariesDf.show(false)
    librariesDf.printSchema()

    val dateTimeDf = createDataframe(spark)
    dateTimeDf.show(false)
    dateTimeDf.printSchema()

    val df = librariesDf.crossJoin(dateTimeDf)
    df.createOrReplaceTempView("libraries")
    df.show(false)

    val sqlQuery = "SELECT Council_ID, Name, date, " +
      "isOpen(Opening_Hours_Monday, Opening_Hours_Tuesday, " +
      "Opening_Hours_Wednesday, Opening_Hours_Thursday, " +
      "Opening_Hours_Friday, Opening_Hours_Saturday, 'closed', date) AS open" +
      " FROM libraries "

    // Using SQL
    val finalDf = spark.sql(sqlQuery)

    finalDf.show()

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("date_str", DataTypes.StringType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("2019-03-11 14:30:00"))
    rows.add(RowFactory.create("2019-04-27 16:00:00"))
    rows.add(RowFactory.create("2020-01-26 05:00:00"))

    spark.createDataFrame(rows, schema)
      .withColumn("date", F.to_timestamp(F.col("date_str")))
      .drop("date_str")
  }

}
/*
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+
|Council_ID|Name                                |Opening_Hours_Monday                                              |Opening_Hours_Tuesday                                             |Opening_Hours_Wednesday                                           |Opening_Hours_Thursday                                            |Opening_Hours_Friday                      |Opening_Hours_Saturday|
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+
|SD1       |County Library                      |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD2       |Ballyroan Library                   |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD3       |Castletymon Library                 |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD4       |Clondalkin Library                  |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD5       |Lucan Library                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD6       |Whitechurch Library                 |14:00-17:00 and 18:00-20:00                                       |14:00-17:00 and 18:00-20:00                                       |09:45-13:00 and 14:00-17:00                                       |14:00-17:00 and 18:00-20:00                                       |Closed                                    |Closed                |
|SD7       |The John Jennings Library (Stewarts)|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-16:00 - closed for lunch 12:30-13:00|Closed                |
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+

root
 |-- Council_ID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Opening_Hours_Monday: string (nullable = true)
 |-- Opening_Hours_Tuesday: string (nullable = true)
 |-- Opening_Hours_Wednesday: string (nullable = true)
 |-- Opening_Hours_Thursday: string (nullable = true)
 |-- Opening_Hours_Friday: string (nullable = true)
 |-- Opening_Hours_Saturday: string (nullable = true)

+-------------------+
|date               |
+-------------------+
|2019-03-11 14:30:00|
|2019-04-27 16:00:00|
|2020-01-26 05:00:00|
+-------------------+

root
 |-- date: timestamp (nullable = true)

+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+-------------------+
|Council_ID|Name                                |Opening_Hours_Monday                                              |Opening_Hours_Tuesday                                             |Opening_Hours_Wednesday                                           |Opening_Hours_Thursday                                            |Opening_Hours_Friday                      |Opening_Hours_Saturday|date               |
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+-------------------+
|SD1       |County Library                      |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-03-11 14:30:00|
|SD1       |County Library                      |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-04-27 16:00:00|
|SD1       |County Library                      |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2020-01-26 05:00:00|
|SD2       |Ballyroan Library                   |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-03-11 14:30:00|
|SD2       |Ballyroan Library                   |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-04-27 16:00:00|
|SD2       |Ballyroan Library                   |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2020-01-26 05:00:00|
|SD3       |Castletymon Library                 |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-16:30                               |09:45-16:30           |2019-03-11 14:30:00|
|SD3       |Castletymon Library                 |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-16:30                               |09:45-16:30           |2019-04-27 16:00:00|
|SD3       |Castletymon Library                 |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-16:30                               |09:45-16:30           |2020-01-26 05:00:00|
|SD4       |Clondalkin Library                  |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-03-11 14:30:00|
|SD4       |Clondalkin Library                  |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-04-27 16:00:00|
|SD4       |Clondalkin Library                  |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2020-01-26 05:00:00|
|SD5       |Lucan Library                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-03-11 14:30:00|
|SD5       |Lucan Library                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2019-04-27 16:00:00|
|SD5       |Lucan Library                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |2020-01-26 05:00:00|
|SD6       |Whitechurch Library                 |14:00-17:00 and 18:00-20:00                                       |14:00-17:00 and 18:00-20:00                                       |09:45-13:00 and 14:00-17:00                                       |14:00-17:00 and 18:00-20:00                                       |Closed                                    |Closed                |2019-03-11 14:30:00|
|SD6       |Whitechurch Library                 |14:00-17:00 and 18:00-20:00                                       |14:00-17:00 and 18:00-20:00                                       |09:45-13:00 and 14:00-17:00                                       |14:00-17:00 and 18:00-20:00                                       |Closed                                    |Closed                |2019-04-27 16:00:00|
|SD6       |Whitechurch Library                 |14:00-17:00 and 18:00-20:00                                       |14:00-17:00 and 18:00-20:00                                       |09:45-13:00 and 14:00-17:00                                       |14:00-17:00 and 18:00-20:00                                       |Closed                                    |Closed                |2020-01-26 05:00:00|
|SD7       |The John Jennings Library (Stewarts)|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-16:00 - closed for lunch 12:30-13:00|Closed                |2019-03-11 14:30:00|
|SD7       |The John Jennings Library (Stewarts)|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-16:00 - closed for lunch 12:30-13:00|Closed                |2019-04-27 16:00:00|
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+-------------------+
only showing top 20 rows

Day of the week: 2
Processing range #0: 09:45-20:00
Checking between 32400 and 72000
Day of the week: 7
Processing range #0: 09:45-16:30
Checking between 32400 and 57600
Day of the week: 1
Day of the week: 2
Processing range #0: 09:45-20:00
Checking between 32400 and 72000
Day of the week: 7
Processing range #0: 09:45-16:30
Checking between 32400 and 57600
Day of the week: 1
Day of the week: 2
Processing range #0: 09:45-17:00
Checking between 32400 and 61200
Day of the week: 7
Processing range #0: 09:45-16:30
Checking between 32400 and 57600
Day of the week: 1
Day of the week: 2
Processing range #0: 09:45-20:00
Checking between 32400 and 72000
Day of the week: 7
Processing range #0: 09:45-16:30
Checking between 32400 and 57600
Day of the week: 1
Day of the week: 2
Processing range #0: 09:45-20:00
Checking between 32400 and 72000
Day of the week: 7
Processing range #0: 09:45-16:30
Checking between 32400 and 57600
Day of the week: 1
Day of the week: 2
Processing range #0: 14:00-17:00
Checking between 50400 and 61200
Day of the week: 7
Day of the week: 1
Day of the week: 2
Processing range #0: 10:00-17:00 (16:00 July
Checking between 36000 and 61200
Day of the week: 7
Day of the week: 1
+----------+--------------------+-------------------+-----+
|Council_ID|                Name|               date| open|
+----------+--------------------+-------------------+-----+
|       SD1|      County Library|2019-03-11 14:30:00| true|
|       SD1|      County Library|2019-04-27 16:00:00| true|
|       SD1|      County Library|2020-01-26 05:00:00|false|
|       SD2|   Ballyroan Library|2019-03-11 14:30:00| true|
|       SD2|   Ballyroan Library|2019-04-27 16:00:00| true|
|       SD2|   Ballyroan Library|2020-01-26 05:00:00|false|
|       SD3| Castletymon Library|2019-03-11 14:30:00| true|
|       SD3| Castletymon Library|2019-04-27 16:00:00| true|
|       SD3| Castletymon Library|2020-01-26 05:00:00|false|
|       SD4|  Clondalkin Library|2019-03-11 14:30:00| true|
|       SD4|  Clondalkin Library|2019-04-27 16:00:00| true|
|       SD4|  Clondalkin Library|2020-01-26 05:00:00|false|
|       SD5|       Lucan Library|2019-03-11 14:30:00| true|
|       SD5|       Lucan Library|2019-04-27 16:00:00| true|
|       SD5|       Lucan Library|2020-01-26 05:00:00|false|
|       SD6| Whitechurch Library|2019-03-11 14:30:00| true|
|       SD6| Whitechurch Library|2019-04-27 16:00:00|false|
|       SD6| Whitechurch Library|2020-01-26 05:00:00|false|
|       SD7|The John Jennings...|2019-03-11 14:30:00| true|
|       SD7|The John Jennings...|2019-04-27 16:00:00|false|
+----------+--------------------+-------------------+-----+
only showing top 20 rows
 */