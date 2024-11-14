package spark_in_action2021.part4

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Author/Organizer analytics.
 *
 * @author rambabu.posa
 */
object Lab17_23MeetingsPerOrganizerScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("JSON Lines to Dataframe")
      // To use Databricks Delta Lake, we should add delta core packages to SparkSession
      .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
      .master("local[*]")
      .getOrCreate

    val df: Dataset[Row] = spark.read
      .format("delta")
      .load("/tmp/delta_grand_debat_events")

    val df2 = df
      .groupBy(col("authorType"))
      .count
      .orderBy(col("authorType").asc_nulls_last)

    df2.show(25, 0, false)
    df2.printSchema()

    spark.stop
  }

}
