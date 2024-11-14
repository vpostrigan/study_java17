package spark_in_action2021.part3transform_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Simple SQL select on ingested data
  *
  * @author rambabu.posa
  */
object Lab11_11SimpleSelectScalaApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SELECT using SQL")
      .master("local")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField]
      (DataTypes.createStructField("geo", DataTypes.StringType, true),
       DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)))

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/populationbycountry19802010millions.csv")

    df.createOrReplaceTempView("geodata")
    df.printSchema()

    val query =
      """
        |SELECT * FROM geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
      """.stripMargin

    val smallCountries = spark.sql(query)

    // Shows at most 10 rows from the dataframe (which is limited to 5 anyway)
    smallCountries.show(10, false)

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
   */
}
