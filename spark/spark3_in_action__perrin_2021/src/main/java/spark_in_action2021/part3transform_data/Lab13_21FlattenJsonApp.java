package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark_in_action2021.Logs;

/**
 * Automatic flattening of JSON structure in Spark.
 *
 * @author jgp
 */
public class Lab13_21FlattenJsonApp {
    public static final String ARRAY_TYPE = "Array";
    public static final String STRUCT_TYPE = "Struc";

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab13_21FlattenJsonApp app = new Lab13_21FlattenJsonApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        SparkSession spark = SparkSession.builder()
                .appName("Automatic flattening of a JSON document")
                .master("local")
                .getOrCreate();

        // Reads a JSON, stores it in a dataframe
        Dataset<Row> invoicesDf = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/chapter13/json/nested_array.json");
        allLogs.showAndSaveToCsv("[1] data", invoicesDf, 3, true, true);

        Dataset<Row> flatInvoicesDf = flattenNestedStructure(spark, invoicesDf);
        allLogs.showAndSaveToCsv("[2] result", flatInvoicesDf, 20, false, true);
/*
18:52:59.876: [1] data
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
18:53:01.548: [1] data has 1. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_21FlattenJsonApp/1_csv
 */
/*
18:53:26.275: [2] result
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

18:53:27.367: [2] result has 18. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_21FlattenJsonApp/2_csv
 */
    }

    /**
     * Implemented as a public static so you can copy it easily in your utils library.
     */
    public static Dataset<Row> flattenNestedStructure(SparkSession spark, Dataset<Row> df) {
        boolean recursion = false;

        Dataset<Row> processedDf = df;
        StructType schema = df.schema();
        StructField[] fields = schema.fields();

        for (StructField field : fields) {
            switch (field.dataType().toString().substring(0, 5)) {
                case ARRAY_TYPE:
                    // Explodes array
                    processedDf = processedDf.withColumnRenamed(field.name(), field.name() + "_tmp");
                    processedDf = processedDf.withColumn(field.name(),
                            explode(processedDf.col(field.name() + "_tmp")));
                    processedDf = processedDf.drop(field.name() + "_tmp");
                    recursion = true;
                    break;
                case STRUCT_TYPE:
                    // Mapping
                    StructField[] structFields = ((StructType) field.dataType()).fields();
                    for (StructField structField : structFields) {
                        processedDf = processedDf.withColumn(field.name() + "_" + structField.name(),
                                processedDf.col(field.name() + "." + structField.name()));
                    }
                    processedDf = processedDf.drop(field.name());
                    recursion = true;
                    break;
            }
        }
        if (recursion) {
            processedDf = flattenNestedStructure(spark, processedDf);
        }
        return processedDf;
    }

}
