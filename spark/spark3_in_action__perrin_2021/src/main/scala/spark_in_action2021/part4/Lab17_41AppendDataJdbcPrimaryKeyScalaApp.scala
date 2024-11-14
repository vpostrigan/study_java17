package spark_in_action2021.part4

import java.util.ArrayList
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._

/**
 * Appends content of a dataframe to a PostgreSQL database.
 *
 * Check for additional information in the README.md file in the same
 * repository.
 *
 * @author rambabu.posa
 *
 */
object Lab17_41AppendDataJdbcPrimaryKeyScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Addition")
      .master("local[*]")
      .getOrCreate

    val df: Dataset[Row] = createDataframe(spark)
    df.show(false)

    // Write in a table called ch17_lab900_pkey
    df.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/spark_labs")
      .option("dbtable", "ch17_lab900_pkey")
      .option("driver", "org.postgresql.Driver")
      .option("user", "jgp")
      .option("password", "Spark<3Java")
      .save()

    spark.stop
  }

  private def createDataframe(spark: SparkSession): DataFrame = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("fname", DataTypes.StringType, false),
      DataTypes.createStructField("lname", DataTypes.StringType, false),
      DataTypes.createStructField("id", DataTypes.IntegerType, false),
      DataTypes.createStructField("score", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("Matei", "Zaharia", int2Integer(34), int2Integer(456)))
    rows.add(RowFactory.create("Jean-Georges", "Perrin", int2Integer(23), int2Integer(3)))
    rows.add(RowFactory.create("Jacek", "Laskowski", int2Integer(12), int2Integer(758)))
    rows.add(RowFactory.create("Holden", "Karau", int2Integer(31), int2Integer(369)))

    spark.createDataFrame(rows, schema)
  }

}
