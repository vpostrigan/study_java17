package spark_in_action2021.part3transform_data

import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * unbase64 function
 *
 * @author rambabu.posa
 */
object Lab13_56Unbase64ScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("unbase64 function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/chapter13/functions/strings.csv")

    println("Output as array of bytes:")
    val df2 = df
      .withColumn("base64", F.base64(F.col("fname")))
      .withColumn("unbase64", F.unbase64(F.col("base64")))

    df2.show(5)

    println("Output as strings:")
    val df3 = df2.withColumn("name", F.col("unbase64").cast(StringType))

    df3.show(5)

    spark.stop
  }

}
/*
Output as array of bytes:
+-------+-------+------------+--------------------+
|  fname|  lname|      base64|            unbase64|
+-------+-------+------------+--------------------+
|Georges| Perrin|R2Vvcmdlcw==|[47 65 6F 72 67 6...|
| Holden|  Karau|    SG9sZGVu| [48 6F 6C 64 65 6E]|
|  Matei|Zaharia|    TWF0ZWk=|    [4D 61 74 65 69]|
|  Ginni|Rometty|    R2lubmk=|    [47 69 6E 6E 69]|
| Arvind|Krishna|    QXJ2aW5k| [41 72 76 69 6E 64]|
+-------+-------+------------+--------------------+

Output as strings:
+-------+-------+------------+--------------------+-------+
|  fname|  lname|      base64|            unbase64|   name|
+-------+-------+------------+--------------------+-------+
|Georges| Perrin|R2Vvcmdlcw==|[47 65 6F 72 67 6...|Georges|
| Holden|  Karau|    SG9sZGVu| [48 6F 6C 64 65 6E]| Holden|
|  Matei|Zaharia|    TWF0ZWk=|    [4D 61 74 65 69]|  Matei|
|  Ginni|Rometty|    R2lubmk=|    [47 69 6E 6E 69]|  Ginni|
| Arvind|Krishna|    QXJ2aW5k| [41 72 76 69 6E 64]| Arvind|
+-------+-------+------------+--------------------+-------+
 */