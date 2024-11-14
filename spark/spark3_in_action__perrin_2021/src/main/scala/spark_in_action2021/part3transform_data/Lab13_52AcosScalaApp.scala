package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * acos function: inverse cosine of a value in radians
 *
 * @author rambabu.posa
 */
object Lab13_52AcosScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("acos function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/functions/trigo_arc.csv")

    val df2 = df.withColumn("acos", F.acos(F.col("val")))
                .withColumn("acos_by_name", F.acos("val"))

    df2.show()

    spark.stop
  }

}
/*
+-----+------------------+------------------+
|  val|              acos|      acos_by_name|
+-----+------------------+------------------+
|    0|1.5707963267948966|1.5707963267948966|
|    1|               0.0|               0.0|
|   .5|1.0471975511965979|1.0471975511965979|
|  .25| 1.318116071652818| 1.318116071652818|
| -.25|1.8234765819369754|1.8234765819369754|
|-0.25|1.8234765819369754|1.8234765819369754|
+-----+------------------+------------------+
 */