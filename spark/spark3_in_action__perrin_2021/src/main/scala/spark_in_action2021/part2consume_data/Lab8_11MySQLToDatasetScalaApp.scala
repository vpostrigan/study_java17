package spark_in_action2021.part2consume_data

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author rambabu.posa
 */
object Lab8_11MySQLToDatasetScalaApp {

  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "password")
    props.put("allowPublicKeyRetrieval", "true")
    props.put("useSSL", "false")

    val mysqlURL = "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST"

    var df = spark.read.jdbc(mysqlURL, "actor", props)

    df = df.orderBy(df.col("last_name"))

    // Displays the dataframe and some of its metadata
    df.show(5)
    df.printSchema()
    println(s"The dataframe contains ${df.count} record(s).")

    spark.stop
  }

}