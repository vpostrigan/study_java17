package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * asin function: inverse sine of a value in radians
 *
 * @author rambabu.posa
 */
object Lab13_54AsinScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("asin function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/functions/trigo_arc.csv")

    val df2 = df.withColumn("asin", F.asin(F.col("val")))
                .withColumn("asin_by_name", F.asin("val"))

    df2.show()

    spark.stop
  }

}
/*
+-----+--------------------+--------------------+
|  val|                asin|        asin_by_name|
+-----+--------------------+--------------------+
|    0|                 0.0|                 0.0|
|    1|  1.5707963267948966|  1.5707963267948966|
|   .5|  0.5235987755982989|  0.5235987755982989|
|  .25| 0.25268025514207865| 0.25268025514207865|
| -.25|-0.25268025514207865|-0.25268025514207865|
|-0.25|-0.25268025514207865|-0.25268025514207865|
+-----+--------------------+--------------------+
 */