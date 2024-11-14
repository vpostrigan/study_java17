package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.functions.{array, callUDF}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import scala.collection.mutable

/**
 * Column Additions via UDF.
 *
 * @author rambabu.posa
 */
object Lab14_71ColumnAdditionScalaApp {
  private val COL_COUNT = 8

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Column addition")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("add", new Lab14_72ColumnAdditionScalaUdf, DataTypes.IntegerType)

    var df = createDataframe(spark)
    df.show(false)

    var cols = List[Column]()
    for (i <- 0 until COL_COUNT) {
      cols = cols :+ df.col("c" + i)
    }

    val col = array(cols: _*)

    df = df.withColumn("sum", callUDF("add", col))
    df.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("c0", DataTypes.IntegerType, false),
      DataTypes.createStructField("c1", DataTypes.IntegerType, false),
      DataTypes.createStructField("c2", DataTypes.IntegerType, false),
      DataTypes.createStructField("c3", DataTypes.IntegerType, false),
      DataTypes.createStructField("c4", DataTypes.IntegerType, false),
      DataTypes.createStructField("c5", DataTypes.IntegerType, false),
      DataTypes.createStructField("c6", DataTypes.IntegerType, false),
      DataTypes.createStructField("c7", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(int2Integer(1), int2Integer(2), int2Integer(4), int2Integer(8),
      int2Integer(16), int2Integer(32), int2Integer(64), int2Integer(128)))
    rows.add(RowFactory.create(int2Integer(0), int2Integer(0), int2Integer(0), int2Integer(0),
      int2Integer(0), int2Integer(0), int2Integer(0), int2Integer(0)))
    rows.add(RowFactory.create(int2Integer(1), int2Integer(1), int2Integer(1), int2Integer(1),
      int2Integer(1), int2Integer(1), int2Integer(1), int2Integer(1)))

    spark.createDataFrame(rows, schema)
  }

}
