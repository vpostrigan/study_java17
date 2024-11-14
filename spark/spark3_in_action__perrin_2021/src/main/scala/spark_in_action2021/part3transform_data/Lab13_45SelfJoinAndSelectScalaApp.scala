package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Self join.
 *
 * @author rambabu.posa
 */
object Lab13_45SelfJoinAndSelectScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Self join")
      .master("local[*]")
      .getOrCreate

    val inputDf = createDataframe(spark)
    inputDf.show(false)

    val left = inputDf.withColumnRenamed("dst", "dst2")
    left.show()

    val right = inputDf.withColumnRenamed("src", "dst2")
    right.show()

    val r = left.join(right, left.col("dst2") === right.col("dst2"))
    r.show()

    val resultOption1Df = r.select(F.col("src"), F.col("dst"))
    resultOption1Df.show()

    val resultOption2Df = r.select(F.col("src"), F.col("dst"))
    resultOption2Df.show()

    val resultOption3Df = r.select("src", "dst")
    resultOption3Df.show()

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("src", DataTypes.StringType, false),
      DataTypes.createStructField("predicate", DataTypes.StringType, false),
      DataTypes.createStructField("dst", DataTypes.StringType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("a", "r1", ":b1"))
    rows.add(RowFactory.create("a", "r2", "k"))
    rows.add(RowFactory.create("b1", "r3", ":b4"))
    rows.add(RowFactory.create("b1", "r10", "d"))
    rows.add(RowFactory.create(":b4", "r4", "f"))
    rows.add(RowFactory.create(":b4", "r5", ":b5"))
    rows.add(RowFactory.create(":b5", "r9", "t"))
    rows.add(RowFactory.create(":b5", "r10", "e"))
    spark.createDataFrame(rows, schema)
  }

}
/*
+---+---------+---+
|src|predicate|dst|
+---+---------+---+
|a  |r1       |:b1|
|a  |r2       |k  |
|b1 |r3       |:b4|
|b1 |r10      |d  |
|:b4|r4       |f  |
|:b4|r5       |:b5|
|:b5|r9       |t  |
|:b5|r10      |e  |
+---+---------+---+

+---+---------+----+
|src|predicate|dst2|
+---+---------+----+
|  a|       r1| :b1|
|  a|       r2|   k|
| b1|       r3| :b4|
| b1|      r10|   d|
|:b4|       r4|   f|
|:b4|       r5| :b5|
|:b5|       r9|   t|
|:b5|      r10|   e|
+---+---------+----+

+----+---------+---+
|dst2|predicate|dst|
+----+---------+---+
|   a|       r1|:b1|
|   a|       r2|  k|
|  b1|       r3|:b4|
|  b1|      r10|  d|
| :b4|       r4|  f|
| :b4|       r5|:b5|
| :b5|       r9|  t|
| :b5|      r10|  e|
+----+---------+---+

+---+---------+----+----+---------+---+
|src|predicate|dst2|dst2|predicate|dst|
+---+---------+----+----+---------+---+
| b1|       r3| :b4| :b4|       r5|:b5|
| b1|       r3| :b4| :b4|       r4|  f|
|:b4|       r5| :b5| :b5|      r10|  e|
|:b4|       r5| :b5| :b5|       r9|  t|
+---+---------+----+----+---------+---+

+---+---+
|src|dst|
+---+---+
| b1|:b5|
| b1|  f|
|:b4|  e|
|:b4|  t|
+---+---+

+---+---+
|src|dst|
+---+---+
| b1|:b5|
| b1|  f|
|:b4|  e|
|:b4|  t|
+---+---+

+---+---+
|src|dst|
+---+---+
| b1|:b5|
| b1|  f|
|:b4|  e|
|:b4|  t|
+---+---+
 */
