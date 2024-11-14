package spark_in_action2021.part3transform_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Simple SQL select on ingested data, using a global view
  *
  * @author rambabu.posa
  */
object Lab11_12SimpleSelectGlobalViewScalaApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SELECT using SQL")
      .master("local")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false))
    )

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/populationbycountry19802010millions.csv")

    df.createOrReplaceGlobalTempView("geodata")
    df.printSchema()

    val query1 =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin
    val smallCountriesDf = spark.sql(query1)

    // Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
    smallCountriesDf.show(10, false)

    val query2 =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 >= 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin

    // Create a new session and query the same data
    val spark2 = spark.newSession
    val slightlyBiggerCountriesDf = spark2.sql(query2)

    slightlyBiggerCountriesDf.show(10, false)

    spark.stop
  }

/**
  root
 |-- geo: string (nullable = true)
 |-- yr1980: double (nullable = true)

+---------------------------------+-------+
|geo                              |yr1980 |
+---------------------------------+-------+
|Falkland Islands (Islas Malvinas)|0.002  |
|Niue                             |0.002  |
|Saint Pierre and Miquelon        |0.00599|
|Saint Helena                     |0.00647|
|Turks and Caicos Islands         |0.00747|
+---------------------------------+-------+

+--------------------+-------+
|geo                 |yr1980 |
+--------------------+-------+
|United Arab Emirates|1.00029|
|Trinidad and Tobago |1.09051|
|Oman                |1.18548|
|Lesotho             |1.35857|
|Kuwait              |1.36977|
+--------------------+-------+
   */
}
