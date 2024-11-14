package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author jgp
 */
public class Lab12_33JsonInvoiceDisplayApp {

    public static void main(String[] args) {
        Lab12_33JsonInvoiceDisplayApp app =
                new Lab12_33JsonInvoiceDisplayApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Processing of invoices")
                .master("local")
                .getOrCreate();

        // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
        Dataset<Row> invoicesDf = spark.read().format("json")
                .option("multiline", true)
                .load("data/chapter12/invoice/good-invoice*.json");
        invoicesDf.show(3);
        invoicesDf.printSchema();

        Dataset<Row> invoiceAmountDf = invoicesDf.select("totalPaymentDue.*");
        invoiceAmountDf.show(5);
        invoiceAmountDf.printSchema();

        Dataset<Row> elementsOrderedByAccountDf = invoicesDf.select(
                invoicesDf.col("accountId"),
                functions.explode(invoicesDf.col("referencesOrder")).as("order"));
        elementsOrderedByAccountDf.show(3);

        elementsOrderedByAccountDf = elementsOrderedByAccountDf
                .withColumn("type", elementsOrderedByAccountDf.col("order.orderedItem.@type"))
                .withColumn("description", elementsOrderedByAccountDf.col("order.orderedItem.description"))
                .withColumn("name", elementsOrderedByAccountDf.col("order.orderedItem.name"));
        // TODO: we need to use different approch to drop struct column
        //.drop(elementsOrderedByAccountDf.col("order"));

        elementsOrderedByAccountDf.show(10);
        elementsOrderedByAccountDf.printSchema();
    }
/*
+------------------+-------+-------------------+---------------+--------------------+--------------------+--------------------+--------------------+----------+--------------+--------------------+--------+--------------------+--------------------+--------------------+
|          @context|  @type|          accountId|  billingPeriod|              broker|            customer|         description|   minimumPaymentDue|paymentDue|paymentDueDate|       paymentStatus|provider|     referencesOrder|     totalPaymentDue|                 url|
+------------------+-------+-------------------+---------------+--------------------+--------------------+--------------------+--------------------+----------+--------------+--------------------+--------+--------------------+--------------------+--------------------+
|http://schema.org/|Invoice|xxxx-xxxx-xxxx-7563|           null|{LocalBusiness, A...|{Person, Jean Geo...|                null|{PriceSpecificati...|      null|    2016-01-30|http://schema.org...|    null|[{Order, furnace,...|{PriceSpecificati...|                null|
|http://schema.org/|Invoice|xxxx-xxxx-xxxx-1234|           null|{LocalBusiness, A...|  {Person, Jane Doe}|                null|{PriceSpecificati...|      null|    2015-01-30|http://schema.org...|    null|[{Order, furnace,...|{PriceSpecificati...|                null|
|http://schema.org/|Invoice|xxxx-xxxx-xxxx-1234|2014-12-21/P30D|{BankOrCreditUnio...|  {Person, Jane Doe}|January 2015 Visa...|{PriceSpecificati...|      null|    2015-01-30|http://schema.org...|    null|                null|{PriceSpecificati...|http://acmebank.c...|
+------------------+-------+-------------------+---------------+--------------------+--------------------+--------------------+--------------------+----------+--------------+--------------------+--------+--------------------+--------------------+--------------------+
only showing top 3 rows

root
 |-- @context: string (nullable = true)
 |-- @type: string (nullable = true)
 |-- accountId: string (nullable = true)
 |-- billingPeriod: string (nullable = true)
 |-- broker: struct (nullable = true)
 |    |-- @type: string (nullable = true)
 |    |-- name: string (nullable = true)
 |-- customer: struct (nullable = true)
 |    |-- @type: string (nullable = true)
 |    |-- name: string (nullable = true)
 |-- description: string (nullable = true)
 |-- minimumPaymentDue: struct (nullable = true)
 |    |-- @type: string (nullable = true)
 |    |-- price: double (nullable = true)
 |    |-- priceCurrency: string (nullable = true)
 |-- paymentDue: string (nullable = true)
 |-- paymentDueDate: string (nullable = true)
 |-- paymentStatus: string (nullable = true)
 |-- provider: struct (nullable = true)
 |    |-- @type: string (nullable = true)
 |    |-- name: string (nullable = true)
 |-- referencesOrder: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- @type: string (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- orderDate: string (nullable = true)
 |    |    |-- orderNumber: string (nullable = true)
 |    |    |-- orderedItem: struct (nullable = true)
 |    |    |    |-- @type: string (nullable = true)
 |    |    |    |-- description: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- productId: string (nullable = true)
 |    |    |-- paymentMethod: string (nullable = true)
 |-- totalPaymentDue: struct (nullable = true)
 |    |-- @type: string (nullable = true)
 |    |-- price: double (nullable = true)
 |    |-- priceCurrency: string (nullable = true)
 |-- url: string (nullable = true)

+------------------+------+-------------+
|             @type| price|priceCurrency|
+------------------+------+-------------+
|PriceSpecification|4500.0|          USD|
|PriceSpecification|   0.0|          USD|
|PriceSpecification| 200.0|          USD|
|PriceSpecification|  70.0|          USD|
+------------------+------+-------------+

root
 |-- @type: string (nullable = true)
 |-- price: double (nullable = true)
 |-- priceCurrency: string (nullable = true)

+-------------------+--------------------+
|          accountId|               order|
+-------------------+--------------------+
|xxxx-xxxx-xxxx-7563|{Order, furnace, ...|
|xxxx-xxxx-xxxx-7563|{Order, furnace i...|
|xxxx-xxxx-xxxx-1234|{Order, furnace, ...|
+-------------------+--------------------+
only showing top 3 rows

+-------------------+--------------------+-------+--------------------+-----------------+
|          accountId|               order|   type|         description|             name|
+-------------------+--------------------+-------+--------------------+-----------------+
|xxxx-xxxx-xxxx-7563|{Order, furnace, ...|Product|                null|ACME Furnace 3500|
|xxxx-xxxx-xxxx-7563|{Order, furnace i...|Service|furnace installat...|             null|
|xxxx-xxxx-xxxx-1234|{Order, furnace, ...|Product|                null|ACME Furnace 3000|
|xxxx-xxxx-xxxx-1234|{Order, furnace i...|Service|furnace installation|             null|
+-------------------+--------------------+-------+--------------------+-----------------+

root
 |-- accountId: string (nullable = true)
 |-- order: struct (nullable = true)
 |    |-- @type: string (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- orderDate: string (nullable = true)
 |    |-- orderNumber: string (nullable = true)
 |    |-- orderedItem: struct (nullable = true)
 |    |    |-- @type: string (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- productId: string (nullable = true)
 |    |-- paymentMethod: string (nullable = true)
 |-- type: string (nullable = true)
 |-- description: string (nullable = true)
 |-- name: string (nullable = true)
 */
}
