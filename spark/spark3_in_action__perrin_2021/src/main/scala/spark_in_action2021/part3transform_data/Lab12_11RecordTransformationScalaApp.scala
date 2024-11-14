package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Transforming records.
 *
 * @author rambabu.posa
 */
object Lab12_11RecordTransformationScalaApp {

  def main(args: Array[String]): Unit = {
    // Creation of the session
    val spark: SparkSession = SparkSession.builder
      .appName("Record transformations")
      .master("local[*]")
      .getOrCreate

    // Ingestion of the census data
    var df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "cp1252")
      .load("data/chapter12/census/PEP_2017_PEPANNRES.csv")

    // Renaming and dropping the columns we do not need
    df = df.drop("GEO.id")
            .withColumnRenamed("GEO.id2", "id")
            .withColumnRenamed("GEO.display-label", "label")
            .withColumnRenamed("rescen42010", "real2010")
            .drop("resbase42010")
            .withColumnRenamed("respop72010", "est2010")
            .withColumnRenamed("respop72011", "est2011")
            .withColumnRenamed("respop72012", "est2012")
            .withColumnRenamed("respop72013", "est2013")
            .withColumnRenamed("respop72014", "est2014")
            .withColumnRenamed("respop72015", "est2015")
            .withColumnRenamed("respop72016", "est2016")
            .withColumnRenamed("respop72017", "est2017")
    df.printSchema()
    df.show(5)

    // Creates the additional columns
    df = df
      .withColumn("countyState", F.split(F.col("label"), ", "))
      .withColumn("stateId", F.expr("int(id/1000)"))
      .withColumn("countyId", F.expr("id%1000"))
    df.printSchema()
    df.sample(.01).show(5, false)

    df = df
      .withColumn("state", F.col("countyState").getItem(1))
      .withColumn("county", F.col("countyState").getItem(0))
      .drop("countyState")
    df.printSchema()
    df.sample(.01).show(5, false)

    // I could split the column in one operation if I wanted:
    //val countyStateDf: Dataset[Row] = intermediateDf
    //  .withColumn("state", F.split(F.col("label"), ", ").getItem(1))
    //  .withColumn("county", F.split(F.col("label"), ", ").getItem(0))

    // Performs some statistics on the intermediate dataframe
    var statDf = df
      .withColumn("diff", F.expr("est2010-real2010"))
      .withColumn("growth", F.expr("est2017-est2010"))
      .drop("id")
      .drop("label")
      .drop("real2010")
      .drop("est2010")
      .drop("est2011")
      .drop("est2012")
      .drop("est2013")
      .drop("est2014")
      .drop("est2015")
      .drop("est2016")
      .drop("est2017")

    statDf.printSchema()
    statDf.sample(.01).show(5, false)

    // Extras: see how you can sort!
    // statDf = statDf.sort(F.col("growth").desc)
    // println("Top 5 counties with the most growth:")
    // statDf.show(5, false)
    //
    // statDf = statDf.sort(F.col("growth"))
    // println("Top 5 counties with the most loss:")
    // statDf.show(5, false)

    spark.stop
  }

}
