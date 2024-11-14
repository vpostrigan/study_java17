package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Additions via UDF.
 *
 * @author rambabu.posa
 */
object Lab14_41AdditionScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Addition")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("add_int", new Lab14_41IntegerAdditionScalaUdf, DataTypes.IntegerType)
    spark.udf.register("add_string", new Lab14_41StringAdditionScalaUdf, DataTypes.StringType)

    var df = createDataframe(spark)
    df.show(false)

    df = df
      .withColumn("concat",
        F.callUDF("add_string", F.col("fname"), F.col("lname")))

    df.show(false)

    df = df
      .withColumn("score",
      F.callUDF("add_int", F.col("score1"), F.col("score2")))

    df.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("fname", DataTypes.StringType, false),
      DataTypes.createStructField("lname", DataTypes.StringType, false),
      DataTypes.createStructField("score1", DataTypes.IntegerType, false),
      DataTypes.createStructField("score2", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("Jean-Georges", "Perrin", int2Integer(123), int2Integer(456)))
    rows.add(RowFactory.create("Jacek", "Laskowski", int2Integer(147), int2Integer(758)))
    rows.add(RowFactory.create("Holden", "Karau", int2Integer(258), int2Integer(369)))

    spark.createDataFrame(rows, schema)
  }

}
/*
+------------+---------+------+------+
|fname       |lname    |score1|score2|
+------------+---------+------+------+
|Jean-Georges|Perrin   |123   |456   |
|Jacek       |Laskowski|147   |758   |
|Holden      |Karau    |258   |369   |
+------------+---------+------+------+

+------------+---------+------+------+------------------+
|fname       |lname    |score1|score2|concat            |
+------------+---------+------+------+------------------+
|Jean-Georges|Perrin   |123   |456   |Jean-GeorgesPerrin|
|Jacek       |Laskowski|147   |758   |JacekLaskowski    |
|Holden      |Karau    |258   |369   |HoldenKarau       |
+------------+---------+------+------+------------------+

+------------+---------+------+------+------------------+-----+
|fname       |lname    |score1|score2|concat            |score|
+------------+---------+------+------+------------------+-----+
|Jean-Georges|Perrin   |123   |456   |Jean-GeorgesPerrin|579  |
|Jacek       |Laskowski|147   |758   |JacekLaskowski    |905  |
|Holden      |Karau    |258   |369   |HoldenKarau       |627  |
+------------+---------+------+------+------------------+-----+
 */