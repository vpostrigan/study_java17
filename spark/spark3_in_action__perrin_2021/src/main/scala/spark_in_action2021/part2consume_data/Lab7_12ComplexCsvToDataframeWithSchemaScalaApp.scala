package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}
import spark_in_action2021.SchemaInspectorScala

/**
 * CSV ingestion in a dataframe with a Schema.
 *
 * @author rambabu.posa
 */
object Lab7_12ComplexCsvToDataframeWithSchemaScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Complex CSV with a schema to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Creates the schema
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.IntegerType, false),
      DataTypes.createStructField("authorId", DataTypes.IntegerType, true),
      DataTypes.createStructField("bookTitle", DataTypes.StringType, false),
      // nullable, but this will be ignore
      DataTypes.createStructField("releaseDate", DataTypes.DateType, true),
      DataTypes.createStructField("url", DataTypes.StringType, false)))

    // GitHub version only: dumps the schema
    SchemaInspectorScala.print(schema)

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("dateFormat", "MM/dd/yyyy")
      .option("quote", "*")
      .schema(schema)
      .load("data/chapter7/books.csv")

    // GitHub version only: dumps the schema
    SchemaInspectorScala.print(Some("Schema ...... "), schema)
    SchemaInspectorScala.print("Dataframe ... ", df)

    // Shows at most 20 rows from the dataframe
    df.show(30, 25, false)
    df.printSchema()

  }

}