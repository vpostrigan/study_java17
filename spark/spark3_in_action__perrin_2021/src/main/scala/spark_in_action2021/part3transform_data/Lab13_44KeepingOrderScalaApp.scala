package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Keeping the order of rows during transformations.
 *
 * @author rambabu.posa
 */
object Lab13_44KeepingOrderScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Splitting a dataframe to collect it")
      .master("local[*]")
      .getOrCreate

    var df: Dataset[Row] = createDataframe(spark)
    df.show()

    df = df.withColumn("__idx", F.monotonically_increasing_id)
    df.show()

    df = df.dropDuplicates("col2")
      .orderBy("__idx")
      .drop("__idx")

    df.show()

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("col1", DataTypes.IntegerType, false),
      DataTypes.createStructField("col2", DataTypes.StringType, false),
      DataTypes.createStructField("sum", DataTypes.DoubleType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(int2Integer(1), "a", double2Double(3555204326.27)))
    rows.add(RowFactory.create(int2Integer(4), "b", double2Double(22273491.72)))
    rows.add(RowFactory.create(int2Integer(5), "c", double2Double(219175.0)))
    rows.add(RowFactory.create(int2Integer(3), "a", double2Double(219175.0)))
    rows.add(RowFactory.create(int2Integer(2), "c", double2Double(75341433.37)))
    spark.createDataFrame(rows, schema)
  }

}
/*
+----+----+---------------+
|col1|col2|            sum|
+----+----+---------------+
|   1|   a|3.55520432627E9|
|   4|   b|  2.227349172E7|
|   5|   c|       219175.0|
|   3|   a|       219175.0|
|   2|   c|  7.534143337E7|
+----+----+---------------+

+----+----+---------------+-----+
|col1|col2|            sum|__idx|
+----+----+---------------+-----+
|   1|   a|3.55520432627E9|    0|
|   4|   b|  2.227349172E7|    1|
|   5|   c|       219175.0|    2|
|   3|   a|       219175.0|    3|
|   2|   c|  7.534143337E7|    4|
+----+----+---------------+-----+

+----+----+---------------+
|col1|col2|            sum|
+----+----+---------------+
|   1|   a|3.55520432627E9|
|   4|   b|  2.227349172E7|
|   5|   c|       219175.0|
+----+----+---------------+
 */