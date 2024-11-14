package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession, functions => F}

/**
 * Self join.
 *
 * @author rambabu.posa
 */
object Lab13_46SelfJoinScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Self join")
      .master("local[*]")
      .getOrCreate

    val df = createDataframe(spark)
    df.show(false)

    val rightDf = df
      .withColumnRenamed("acct", "acct2")
      .withColumnRenamed("bssn", "bssn2")
      .withColumnRenamed("name", "name2")
      .drop("tid")

    val joinedDf = df.join(rightDf, df.col("acct") === rightDf.col("acct2"), "leftsemi")
      .drop(rightDf.col("acct2"))
      .drop(rightDf.col("name2"))
      .drop(rightDf.col("bssn2"))

    joinedDf.show(false)

    val listDf = joinedDf
      .groupBy(F.col("acct"))
      .agg(F.collect_list("bssn"), F.collect_list("name"))

    listDf.show(false)

    val setDf = joinedDf
      .groupBy(F.col("acct"))
      .agg(F.collect_set("bssn"), F.collect_set("name"))

    setDf.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): DataFrame = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("tid", DataTypes.IntegerType, false),
      DataTypes.createStructField("acct", DataTypes.IntegerType, false),
      DataTypes.createStructField("bssn", DataTypes.IntegerType, false),
      DataTypes.createStructField("name", DataTypes.StringType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(int2Integer(1), int2Integer(123), int2Integer(111), "Peter"))
    rows.add(RowFactory.create(int2Integer(2), int2Integer(123), int2Integer(222), "Paul"))
    rows.add(RowFactory.create(int2Integer(3), int2Integer(456), int2Integer(333), "John"))
    rows.add(RowFactory.create(int2Integer(4), int2Integer(567), int2Integer(444), "Casey"))

    spark.createDataFrame(rows, schema)
  }
}
/*
+---+----+----+-----+
|tid|acct|bssn|name |
+---+----+----+-----+
|1  |123 |111 |Peter|
|2  |123 |222 |Paul |
|3  |456 |333 |John |
|4  |567 |444 |Casey|
+---+----+----+-----+

+---+----+----+-----+
|tid|acct|bssn|name |
+---+----+----+-----+
|1  |123 |111 |Peter|
|2  |123 |222 |Paul |
|3  |456 |333 |John |
|4  |567 |444 |Casey|
+---+----+----+-----+

+----+------------------+------------------+
|acct|collect_list(bssn)|collect_list(name)|
+----+------------------+------------------+
|456 |[333]             |[John]            |
|567 |[444]             |[Casey]           |
|123 |[111, 222]        |[Peter, Paul]     |
+----+------------------+------------------+

+----+-----------------+-----------------+
|acct|collect_set(bssn)|collect_set(name)|
+----+-----------------+-----------------+
|456 |[333]            |[John]           |
|567 |[444]            |[Casey]          |
|123 |[222, 111]       |[Paul, Peter]    |
+----+-----------------+-----------------+
 */