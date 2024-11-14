package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

/**
 * Automatic flattening of JSON structure in Spark.
 *
 * @author rambabu.posa
 */
object Lab13_21FlattenJsonScalaApp {
  val ARRAY_TYPE = "Array"
  val STRUCT_TYPE = "Struc"

  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Automatic flattening of a JSON document")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    val invoicesDf = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/chapter13/json/nested_array.json")

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3)
    invoicesDf.printSchema()

    val flatInvoicesDf = flattenNestedStructure(spark, invoicesDf)
    flatInvoicesDf.show(20, false)
    flatInvoicesDf.printSchema()

    spark.stop
  }

  /**
   * Implemented as a public static so you can copy it easily in your utils
   * library.
   *
   * @param spark
   * @param df
   * @return
   */
  def flattenNestedStructure(spark: SparkSession, df: DataFrame): DataFrame = {
    var recursion = false
    var processedDf = df
    val schema = df.schema
    val fields = schema.fields

    for (field <- fields) {
      field.dataType.toString.substring(0, 5) match {
        case ARRAY_TYPE =>
          // Explodes array
          processedDf = processedDf.withColumnRenamed(field.name, field.name + "_tmp")
          processedDf = processedDf.withColumn(field.name, F.explode(F.col(field.name + "_tmp")))
          processedDf = processedDf.drop(field.name + "_tmp")
          recursion = true

        case STRUCT_TYPE =>
          // Mapping
          /**
           * field.toDDL = `author` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `publisher` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
           * field.toDDL = `books` STRUCT<`salesByMonth`: ARRAY<BIGINT>, `title`: STRING>
           */
          println(s"field.toDDL = ${field.toDDL}")
          val ddl = field.toDDL.split("`") // fragile :(
          var i = 3
          while (i < ddl.length) {
            processedDf = processedDf.withColumn(field.name + "_" + ddl(i), F.col(field.name + "." + ddl(i)))
            i += 2
          }
          processedDf = processedDf.drop(field.name)
          recursion = true
        case _ =>
          processedDf = processedDf
      }
    }

    if (recursion)
      processedDf = flattenNestedStructure(spark, processedDf)

    processedDf
  }

}
/*
+--------------------+--------------------+----------+--------------------+
|              author|               books|      date|           publisher|
+--------------------+--------------------+----------+--------------------+
|{Chapel Hill, USA...|[{[10, 25, 30, 45...|2019-10-05|{Shelter Island, ...|
+--------------------+--------------------+----------+--------------------+

root
 |-- author: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- state: string (nullable = true)
 |-- books: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- salesByMonth: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
 |    |    |-- title: string (nullable = true)
 |-- date: string (nullable = true)
 |-- publisher: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- state: string (nullable = true)

field.toDDL = `author` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
field.toDDL = `publisher` STRUCT<`city`: STRING, `country`: STRING, `name`: STRING, `state`: STRING>
field.toDDL = `books` STRUCT<`salesByMonth`: ARRAY<BIGINT>, `title`: STRING>
+----------+-----------+--------------+-------------------+--------------+--------------+-----------------+--------------------+---------------+----------------------------+------------------+
|date      |author_city|author_country|author_name        |author_state  |publisher_city|publisher_country|publisher_name      |publisher_state|books_title                 |books_salesByMonth|
+----------+-----------+--------------+-------------------+--------------+--------------+-----------------+--------------------+---------------+----------------------------+------------------+
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark with Java             |10                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark with Java             |25                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark with Java             |30                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark with Java             |45                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark with Java             |80                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark with Java             |6                 |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 2nd Edition|11                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 2nd Edition|24                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 2nd Edition|33                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 2nd Edition|48                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 2nd Edition|800               |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 2nd Edition|126               |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 1st Edition|31                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 1st Edition|124               |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 1st Edition|333               |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 1st Edition|418               |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 1st Edition|60                |
|2019-10-05|Chapel Hill|USA           |Jean Georges Perrin|North Carolina|Shelter Island|USA              |Manning Publications|New York       |Spark in Action, 1st Edition|122               |
+----------+-----------+--------------+-------------------+--------------+--------------+-----------------+--------------------+---------------+----------------------------+------------------+

root
 |-- date: string (nullable = true)
 |-- author_city: string (nullable = true)
 |-- author_country: string (nullable = true)
 |-- author_name: string (nullable = true)
 |-- author_state: string (nullable = true)
 |-- publisher_city: string (nullable = true)
 |-- publisher_country: string (nullable = true)
 |-- publisher_name: string (nullable = true)
 |-- publisher_state: string (nullable = true)
 |-- books_title: string (nullable = true)
 |-- books_salesByMonth: long (nullable = true)
 */