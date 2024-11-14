package spark_in_action2021.part3transform_data

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author rambabu.posa
 */
object JsonInvoiceDisplayScalaApp {

  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Processing of invoices")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    val invoicesDf = spark.read.format("json")
      .option("multiline", true)
      .load("data/chapter12/invoice/good-invoice*.json")
    invoicesDf.show(3)
    invoicesDf.printSchema()

    val invoiceAmountDf = invoicesDf.select("totalPaymentDue.*")
    invoiceAmountDf.show(5)
    invoiceAmountDf.printSchema()

    val elementsOrderedByAccountDf = invoicesDf
      .select(F.col("accountId"),
        F.explode(F.col("referencesOrder")).as("order"))

    elementsOrderedByAccountDf.show(10)
    elementsOrderedByAccountDf.printSchema()

    val elementsOrderedByAccountDf2 = elementsOrderedByAccountDf
      .withColumn("type", F.col("order.orderedItem.@type"))
      .withColumn("description", F.col("order.orderedItem.description"))
      .withColumn("name", F.col("order.orderedItem.name"))
    // TODO: failing to drop struct field
    //.drop("order")

    elementsOrderedByAccountDf2.show(10)
    elementsOrderedByAccountDf2.printSchema()

    spark.stop
  }

}
