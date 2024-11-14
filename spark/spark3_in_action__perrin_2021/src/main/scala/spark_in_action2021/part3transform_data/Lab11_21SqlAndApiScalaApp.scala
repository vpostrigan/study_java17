package spark_in_action2021.part3transform_data

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Simple SQL select on ingested data after preparing the data with the dataframe API.
 *
 * @author rambabu.posa
 */
object Lab11_21SqlAndApiScalaApp {

  def main(args: Array[String]): Unit = {

    // Create a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SQL")
      .master("local")
      .getOrCreate

    // Create the schema for the whole dataset
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

    for (i <- Range(1981, 2010)) {
      df = df.drop(df.col("yr" + i))
    }

    // Creates a new column with the evolution of the population between
    // 1980
    // and 2010
    df = df.withColumn("evolution", F.expr("round((yr2010 - yr1980) * 1000000)"))
    df.createOrReplaceTempView("geodata")

    val query1 =
      """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution <= 0
        |ORDER BY evolution
        |LIMIT 25
      """.stripMargin
    val negativeEvolutionDf = spark.sql(query1)

    // Shows at most 15 rows from the dataframe
    negativeEvolutionDf.show(15, false)

    val query2 =
      """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution > 999999
        |ORDER BY evolution DESC
        |LIMIT 25
      """.stripMargin

    val moreThanAMillionDf = spark.sql(query2)
    moreThanAMillionDf.show(15, false)

    // Good to stop SparkSession at the end of the application
    spark.stop()

  }

  /**
+-------------------------+--------+--------+----------+
|geo                      |yr1980  |yr2010  |evolution |
+-------------------------+--------+--------+----------+
|Bulgaria                 |8.84353 |7.14879 |-1694740.0|
|Hungary                  |10.71112|9.99234 |-718780.0 |
|Romania                  |22.13004|21.95928|-170760.0 |
|Guyana                   |0.75935 |0.74849 |-10860.0  |
|Montserrat               |0.01177 |0.00512 |-6650.0   |
|Cook Islands             |0.01801 |0.01149 |-6520.0   |
|Netherlands Antilles     |0.23244 |0.22869 |-3750.0   |
|Dominica                 |0.07389 |0.07281 |-1080.0   |
|Saint Pierre and Miquelon|0.00599 |0.00594 |-50.0     |
+-------------------------+--------+--------+----------+

+-----------------------+----------+----------+------------+
|geo                    |yr1980    |yr2010    |evolution   |
+-----------------------+----------+----------+------------+
|World                  |4451.32679|6853.01941|2.40169262E9|
|Asia & Oceania         |2469.81743|3799.67028|1.32985285E9|
|Africa                 |478.96479 |1015.47842|5.3651363E8 |
|India                  |684.8877  |1173.10802|4.8822032E8 |
|China                  |984.73646 |1330.14129|3.4540483E8 |
|Central & South America|293.05856 |480.01228 |1.8695372E8 |
|North America          |320.27638 |456.59331 |1.3631693E8 |
|Middle East            |93.78699  |212.33692 |1.1854993E8 |
|Pakistan               |85.21912  |184.40479 |9.918567E7  |
|Indonesia              |151.0244  |242.96834 |9.194394E7  |
|United States          |227.22468 |310.23286 |8.300818E7  |
|Brazil                 |123.01963 |201.10333 |7.80837E7   |
|Nigeria                |74.82127  |152.21734 |7.739607E7  |
|Europe                 |529.50082 |606.00344 |7.650262E7  |
|Bangladesh             |87.93733  |156.11846 |6.818113E7  |
+-----------------------+----------+----------+------------+
only showing top 15 rows
   */
}
