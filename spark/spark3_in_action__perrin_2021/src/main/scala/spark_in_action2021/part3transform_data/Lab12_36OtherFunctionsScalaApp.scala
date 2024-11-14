package spark_in_action2021.part3transform_data

import java.util.{ArrayList, Random}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

object Lab12_36OtherFunctionsScalaApp {

  /**
   * Use of from_unixtime().
   *
   * @author rambabu.posa
   */
  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("from_unixtime()")
      .master("local[*]")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("event", DataTypes.IntegerType, false),
      DataTypes.createStructField("ts", DataTypes.StringType, false)))

    // Building a df with a sequence of chronological timestamps
    val rows = new ArrayList[Row]
    var now: Long = System.currentTimeMillis / 1000
    for (i <- 0 until 1000) {
      rows.add(RowFactory.create(int2Integer(i), String.valueOf(now)))
      now += new Random().nextInt(3) + 1
    }

    var df = spark.createDataFrame(rows, schema)
    df.show()

    // Turning the timestamps to dates
    df = df.withColumn("date", from_unixtime(col("ts")))
    df.show()

    // Collecting the result and printing ou
    val timeRows = df.collectAsList
    import scala.collection.JavaConversions._
    for (r <- timeRows) {
      printf("[%d] : %s (%s)\n", r.getInt(0), r.getString(1), r.getString(2))
    }
  }

  /**
   * Use of from_unixtime() and unix_timestamp().
   *
   * @author rambabu.posa
   */
  def main2(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("expr()")
      .master("local[*]")
      .getOrCreate

    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("event", DataTypes.IntegerType, false),
      DataTypes.createStructField("original_ts", DataTypes.StringType, false)))

    // Building a df with a sequence of chronological timestamps
    val rows = new ArrayList[Row]
    var now: Long = System.currentTimeMillis / 1000

    for (i <- 0.until(1000)) {
      rows.add(RowFactory.create(int2Integer(i), String.valueOf(now)))
      now += new Random().nextInt(3) + 1
    }

    var df = spark.createDataFrame(rows, schema)
    df.show()
    df.printSchema()

    // Turning the timestamps to Timestamp datatype
    df = df.withColumn("date",
      F.from_unixtime(F.col("original_ts")).cast(DataTypes.TimestampType))
    df.show()
    df.printSchema()

    // Turning back the timestamps to epoch
    df = df.withColumn("epoch",
      F.unix_timestamp(F.col("date")))
    df.show()
    df.printSchema()

    // Collecting the result and printing ou
    import scala.collection.JavaConversions._
    for (row <- df.collectAsList) {
      printf("[%d] : %s (%s)\n", row.getInt(0), row.getAs("epoch"), row.getAs("date"))
    }

    spark.stop
  }

  /**
   * Use of expr() Spark API.
   *
   * @author rambabu.posa
   */
  def main3(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("expr()")
      .master("local[*]")
      .getOrCreate

    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("title", DataTypes.StringType, false),
      DataTypes.createStructField("start", DataTypes.IntegerType, false),
      DataTypes.createStructField("end", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("bla", int2Integer(10), int2Integer(30)))
    var df: Dataset[Row] = spark.createDataFrame(rows, schema)
    df.show()

    df = df.withColumn("time_spent", F.expr("end - start"))
      .drop("start")
      .drop("end")

    df.show()

    spark.stop
  }

  /**
   * Keeping the order of rows during transformations.
   *
   * @author rambabu.posa
   */
  def main4(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Splitting a dataframe to collect it")
      .master("local[*]")
      .getOrCreate

    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("col1", DataTypes.IntegerType, false),
      DataTypes.createStructField("col2", DataTypes.StringType, false),
      DataTypes.createStructField("sum", DataTypes.DoubleType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(int2Integer(1), "a", double2Double(3555204326.27)))
    rows.add(RowFactory.create(int2Integer(4), "b", double2Double(22273491.72)))
    rows.add(RowFactory.create(int2Integer(5), "c", double2Double(219175.0)))
    rows.add(RowFactory.create(int2Integer(3), "a", double2Double(219175.0)))
    rows.add(RowFactory.create(int2Integer(2), "c",double2Double( 75341433.37)))

    var df: Dataset[Row] = spark.createDataFrame(rows, schema)
    df.show()

    df = df.withColumn("__idx", F.monotonically_increasing_id)
    df.show()

    df = df
      .dropDuplicates("col2")
      .orderBy("__idx")
      .drop("__idx")

    df.show()

    spark.stop
  }

  /**
   * Self join.
   *
   * @author rambabu.posa
   */
  def main5(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Self join")
      .master("local[*]")
      .getOrCreate

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

    val inputDf: Dataset[Row] = spark.createDataFrame(rows, schema)
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

  /**
   * Self join.
   *
   * @author rambabu.posa
   */
  def main6(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Self join")
      .master("local[*]")
      .getOrCreate

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

    val df: Dataset[Row] = spark.createDataFrame(rows, schema)
    df.show(false)

    val rightDf = df
      .withColumnRenamed("acct", "acct2")
      .withColumnRenamed("bssn", "bssn2")
      .withColumnRenamed("name", "name2")
      .drop("tid")

    val joinedDf = df
      .join(rightDf, df.col("acct") === rightDf.col("acct2"), "leftsemi")
      .drop(rightDf.col("acct2"))
      .drop(rightDf.col("name2"))
      .drop(rightDf.col("bssn2"))

    joinedDf.show(false)

    val listDf = joinedDf
      .groupBy(joinedDf.col("acct"))
      .agg(F.collect_list("bssn"), F.collect_list("name"))

    listDf.show(false)

    val setDf = joinedDf
      .groupBy(joinedDf.col("acct"))
      .agg(F.collect_set("bssn"), F.collect_set("name"))

    setDf.show(false)

    spark.stop
  }

}
