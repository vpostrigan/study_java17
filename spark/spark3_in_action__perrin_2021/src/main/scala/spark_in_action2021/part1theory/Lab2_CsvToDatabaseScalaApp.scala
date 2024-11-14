package spark_in_action2021.part1theory

import java.util.Properties

import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * CSV to a relational database.
 *
 * @author rambabu.posa
 */
object Lab2_CsvToDatabaseScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("CSV to DB")
      .master("local[*]")
      .getOrCreate

    // Step 1: Ingestion
    // ---------
    // Reads a CSV file with header, called authors.csv, stores it in a dataframe
    var df = spark.read.format("csv")
      .option("header", "true")
      .load("data/authors.csv")


    // Step 2: Transform
    // ---------
    // Creates a new column called "name" as the concatenation of lname, a
    // virtual column containing ", " and the fname column
    df = df.withColumn("name", concat(col("lname"), lit(", "), col("fname")))

    // Step 3: Save
    // ----
    // The connection URL, assuming your PostgreSQL instance runs locally on the
    // default port, and the database we use is "spark_labs"
    val dbConnectionUrl = "jdbc:mysql://localhost:3306/myDB"

    // Properties to connect to the database, the JDBC driver is part of our pom.xml
    val prop = new Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "password")

    // Write in a table called ch02
    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(dbConnectionUrl, "ch02", prop)

    // Good to stop SparkSession at the end of the application
    spark.stop

    println("Process complete")
  }

}