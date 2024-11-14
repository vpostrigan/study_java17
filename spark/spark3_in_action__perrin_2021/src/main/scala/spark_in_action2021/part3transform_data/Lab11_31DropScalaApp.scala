package spark_in_action2021.part3transform_data

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{SparkSession, functions => F}
import org.slf4j.LoggerFactory
import spark_in_action2021.part2consume_data.Lab10_45StreamRecordInMemoryScalaApp

/**
  * Dropping data using SQL
  *
  * @author rambabu.posa
  */
object Lab11_31DropScalaApp {

  private val log = LoggerFactory.getLogger(classOf[Lab10_45StreamRecordInMemoryScalaApp])

  def main(args: Array[String]): Unit = {
    log.debug("-> start()")

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SQL")
      .master("local")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1981", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1982", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1983", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1984", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1985", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1986", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1987", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1988", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1989", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1990", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1991", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1992", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1993", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1994", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1995", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1996", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1997", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1998", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1999", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2000", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2001", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2002", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2003", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2004", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2005", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2006", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2007", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2008", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2009", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2010", DataTypes.DoubleType, false)
    ))

    // Reads a CSV file with header (as specified in the schema), called
    // populationbycountry19802010millions.csv, stores it in a dataframe
    var df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/populationbycountry19802010millions.csv")

    for(i <- Range(1981, 2010)) {
      df = df.drop(df.col("yr" + i))
    }

    // Creates a new column with the evolution of the population between 1980 and 2010
    df = df.withColumn("evolution", F.expr("round((yr2010 - yr1980) * 1000000)"))
    df.createOrReplaceTempView("geodata")

    log.debug("Territories in original dataset: {}", df.count)
    val query =
      """
        |SELECT * FROM geodata
        |WHERE geo is not null
        | and geo != 'Africa'
        | and geo != 'North America'
        | and geo != 'World'
        | and geo != 'Asia & Oceania'
        | and geo != 'Central & South America'
        | and geo != 'Europe'
        | and geo != 'Eurasia'
        | and geo != 'Middle East'
        | order by yr2010 desc
      """.stripMargin

    val cleanedDf = spark.sql(query)

    log.debug("Territories in cleaned dataset: {}", cleanedDf.count)
    cleanedDf.show(20, false)

    spark.stop
  }

  /**
2022-07-25 15:03:27.131 -DEBUG --- [           main] App$.main(Lab11_31DropScalaApp.scala:75): Territories in original dataset: 232
2022-07-25 15:03:27.601 -DEBUG --- [           main] App$.main(Lab11_31DropScalaApp.scala:93): Territories in cleaned dataset: 224
+----------------+---------+----------+-----------+
|geo             |yr1980   |yr2010    |evolution  |
+----------------+---------+----------+-----------+
|China           |984.73646|1330.14129|3.4540483E8|
|India           |684.8877 |1173.10802|4.8822032E8|
|United States   |227.22468|310.23286 |8.300818E7 |
|Indonesia       |151.0244 |242.96834 |9.194394E7 |
|Brazil          |123.01963|201.10333 |7.80837E7  |
|Pakistan        |85.21912 |184.40479 |9.918567E7 |
|Bangladesh      |87.93733 |156.11846 |6.818113E7 |
|Nigeria         |74.82127 |152.21734 |7.739607E7 |
|Russia          |null     |139.39021 |null       |
|Japan           |116.80731|126.80443 |9997120.0  |
|Mexico          |68.34748 |112.46886 |4.412138E7 |
|Philippines     |50.94018 |99.90018  |4.896E7    |
|Vietnam         |53.7152  |89.57113  |3.585593E7 |
|Ethiopia        |38.6052  |88.01349  |4.940829E7 |
|Germany         |null     |82.28299  |null       |
|Egypt           |42.63422 |80.47187  |3.783765E7 |
|Turkey          |45.04797 |77.80412  |3.275615E7 |
|Iran            |39.70873 |76.9233   |3.721457E7 |
|Congo (Kinshasa)|29.01255 |70.91644  |4.190389E7 |
|Thailand        |47.02576 |67.0895   |2.006374E7 |
+----------------+---------+----------+-----------+
only showing top 20 rows
   */
}
