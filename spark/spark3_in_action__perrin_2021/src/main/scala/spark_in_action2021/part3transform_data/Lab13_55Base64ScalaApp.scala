package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * base64 function
 *
 * @author rambabu.posa
 */
object Lab13_55Base64ScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("base64 function")
      .master("local[*]")
      .getOrCreate

    var df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/functions/strings.csv")

    df = df.withColumn("base64", F.base64(F.col("fname")))

    df.show(5)

    spark.stop
  }

}
/*
+-------+-------+------------+
|  fname|  lname|      base64|
+-------+-------+------------+
|Georges| Perrin|R2Vvcmdlcw==|
| Holden|  Karau|    SG9sZGVu|
|  Matei|Zaharia|    TWF0ZWk=|
|  Ginni|Rometty|    R2lubmk=|
| Arvind|Krishna|    QXJ2aW5k|
+-------+-------+------------+
 */