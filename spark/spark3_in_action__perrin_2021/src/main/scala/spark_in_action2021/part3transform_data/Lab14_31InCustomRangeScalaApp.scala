package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession,functions=>F}

/**
 * Custom UDF to check if in range.
 *
 * @author rambabu.posa
 */
object Lab14_31InCustomRangeScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Custom UDF to check if in range")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("inRange", new Lab14_32InRangeScalaUdf, DataTypes.BooleanType)

    var df: Dataset[Row] = createDataframe(spark)
    df.show(false)

    df = df
      .withColumn("date", F.date_format(F.col("time"), "yyyy-MM-dd HH:mm:ss.SSSS"))
      .withColumn("h", F.hour(F.col("date")))
      .withColumn("m", F.minute(F.col("date")))
      .withColumn("s", F.second(F.col("date")))
      .withColumn("event", F.expr("h*3600 + m*60 +s"))
      .drop("date","h","m","s")

    df.show(false)

    df = df
      .withColumn("between",
        F.callUDF("inRange", F.col("range"), F.col("event")))

    df.show(false)

    spark.stop

  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.StringType, false),
      DataTypes.createStructField("time", DataTypes.StringType, false),
      DataTypes.createStructField("range", DataTypes.StringType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("id1", "2019-03-11 05:00:00", "00h00-07h30;23h30-23h59"))
    rows.add(RowFactory.create("id2", "2019-03-11 09:00:00", "00h00-07h30;23h30-23h59"))
    rows.add(RowFactory.create("id3", "2019-03-11 10:30:00", "00h00-07h30;23h30-23h59"))

    spark.createDataFrame(rows, schema)
  }

}
/*
+---+-------------------+-----------------------+
|id |time               |range                  |
+---+-------------------+-----------------------+
|id1|2019-03-11 05:00:00|00h00-07h30;23h30-23h59|
|id2|2019-03-11 09:00:00|00h00-07h30;23h30-23h59|
|id3|2019-03-11 10:30:00|00h00-07h30;23h30-23h59|
+---+-------------------+-----------------------+

+---+-------------------+-----------------------+-----+
|id |time               |range                  |event|
+---+-------------------+-----------------------+-----+
|id1|2019-03-11 05:00:00|00h00-07h30;23h30-23h59|18000|
|id2|2019-03-11 09:00:00|00h00-07h30;23h30-23h59|32400|
|id3|2019-03-11 10:30:00|00h00-07h30;23h30-23h59|37800|
+---+-------------------+-----------------------+-----+

-> call(00h00-07h30;23h30-23h59, 18000)
Processing range #0: 00h00-07h30
Checking between 0 and 27000
-> call(00h00-07h30;23h30-23h59, 32400)
Processing range #0: 00h00-07h30
Checking between 0 and 27000
Processing range #1: 23h30-23h59
Checking between 84600 and 86340
-> call(00h00-07h30;23h30-23h59, 37800)
Processing range #0: 00h00-07h30
Checking between 0 and 27000
Processing range #1: 23h30-23h59
Checking between 84600 and 86340
+---+-------------------+-----------------------+-----+-------+
|id |time               |range                  |event|between|
+---+-------------------+-----------------------+-----+-------+
|id1|2019-03-11 05:00:00|00h00-07h30;23h30-23h59|18000|true   |
|id2|2019-03-11 09:00:00|00h00-07h30;23h30-23h59|32400|false  |
|id3|2019-03-11 10:30:00|00h00-07h30;23h30-23h59|37800|false  |
+---+-------------------+-----------------------+-----+-------+
 */