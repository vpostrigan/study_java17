package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Using JSONpath-like in SQL queries.
 *
 * @author rambabu.posa
 */
object Lab12_32QueryOnJsonScalaApp {

  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Query on a JSON doc")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    val df = spark.read.format("json")
      .option("multiline", true)
      .load("data/chapter12/json/store.json")

    // Explode the array
    val df2 = df.withColumn("items", F.explode(F.col("store.book")))

    // Creates a view so I can use SQL
    df2.createOrReplaceTempView("books")
    val sqlQuery = "SELECT items.author FROM books WHERE items.category = 'reference'"
    val authorsOfReferenceBookDf = spark.sql(sqlQuery)
    authorsOfReferenceBookDf.show(false)

    spark.stop
  }

}
