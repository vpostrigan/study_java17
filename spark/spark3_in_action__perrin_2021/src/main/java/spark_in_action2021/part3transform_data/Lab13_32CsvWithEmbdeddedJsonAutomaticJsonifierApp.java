package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import spark_in_action2021.Logs;

/**
 * Ingesting a CSV with embedded JSON.
 *
 * @author jgp
 */
public class Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp implements Serializable {
    private static final long serialVersionUID = 19713L;

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp app =
                new Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        SparkSession spark = SparkSession.builder()
                .appName("Processing of invoices")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("delimiter", "|")
                .option("inferSchema", true)
                .csv("data/chapter13/misc/csv_with_embedded_json2.csv");
        allLogs.showAndSaveToCsv("[1] data csv", df, 5, false, true);
/*
22:03:18.146: [1] data csv
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|deptId|dept     |budget|emp_json                                                                                                                                                                                                                                                                                                                                                                                   |location|
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|45    |finance  |45.567|{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}                                                                                                                       |OH      |
|78    |marketing|123.89|{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}|FL      |
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+

root
 |-- deptId: integer (nullable = true)
 |-- dept: string (nullable = true)
 |-- budget: double (nullable = true)
 |-- emp_json: string (nullable = true)
 |-- location: string (nullable = true)

22:03:18.903: [1] data csv has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp/1_csv
 */

        Dataset<String> ds = df.map(new Jsonifier("emp_json"), Encoders.STRING());
        allLogs.showAndSaveToCsv("[2] ds", df, 5, false, true);
/*
22:03:19.158: [2] ds
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|deptId|dept     |budget|emp_json                                                                                                                                                                                                                                                                                                                                                                                   |location|
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|45    |finance  |45.567|{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}                                                                                                                       |OH      |
|78    |marketing|123.89|{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}|FL      |
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+

root
 |-- deptId: integer (nullable = true)
 |-- dept: string (nullable = true)
 |-- budget: double (nullable = true)
 |-- emp_json: string (nullable = true)
 |-- location: string (nullable = true)

22:03:19.431: [2] ds has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp/2_csv
 */

        Dataset<Row> dfJson = spark.read().json(ds);
        allLogs.showAndSaveToCsv("[3] dfJson", df, 5, false, true);
/*
22:03:19.900: [3] dfJson
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|deptId|dept     |budget|emp_json                                                                                                                                                                                                                                                                                                                                                                                   |location|
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|45    |finance  |45.567|{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}                                                                                                                       |OH      |
|78    |marketing|123.89|{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}|FL      |
+------+---------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+

root
 |-- deptId: integer (nullable = true)
 |-- dept: string (nullable = true)
 |-- budget: double (nullable = true)
 |-- emp_json: string (nullable = true)
 |-- location: string (nullable = true)

22:03:20.143: [3] dfJson has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp/3_csv
 */
        dfJson = dfJson
                .withColumn("emp", explode(dfJson.col("employee")))
                .drop("employee");
        allLogs.showAndSaveToCsv("[4] dfJson", dfJson, 5, false, true);
/*
22:03:20.262: [4] dfJson
+------+---------+------+--------+--------------------------------------------------------------------+
|budget|dept     |deptId|location|emp                                                                 |
+------+---------+------+--------+--------------------------------------------------------------------+
|45.567|finance  |45    |OH      |{{Columbus, 1234 West Broad St, 8505}, {John, Doe}}                 |
|45.567|finance  |45    |OH      |{{Salinas, 4321 North Meecham Rd, 300}, {Alex, Messi}}              |
|123.89|marketing|78    |FL      |{{Miami, 9 East Main St, 007}, {Michael, Scott}}                    |
|123.89|marketing|78    |FL      |{{Miami, 3 Main St, #78}, {Jane, Doe}}                              |
|123.89|marketing|78    |FL      |{{Fort Lauderdale, 314 Great Circle Court, 4590}, {Sacha, Gutierez}}|
+------+---------+------+--------+--------------------------------------------------------------------+

root
 |-- budget: double (nullable = true)
 |-- dept: string (nullable = true)
 |-- deptId: long (nullable = true)
 |-- location: string (nullable = true)
 |-- emp: struct (nullable = true)
 |    |-- address: struct (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- street: string (nullable = true)
 |    |    |-- unit: string (nullable = true)
 |    |-- name: struct (nullable = true)
 |    |    |-- firstName: string (nullable = true)
 |    |    |-- lastName: string (nullable = true)

22:03:20.599: [4] dfJson has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp/4_csv
 */
        dfJson = dfJson
                .withColumn("emp_name",
                        concat(
                                dfJson.col("emp.name.firstName"),
                                lit(" "),
                                dfJson.col("emp.name.lastName")))
                .withColumn("emp_address",
                        concat(
                                dfJson.col("emp.address.street"),
                                lit(" "),
                                dfJson.col("emp.address.unit")))
                .withColumn("emp_city", dfJson.col("emp.address.city"))
                .drop("emp");
        allLogs.showAndSaveToCsv("[5] result", dfJson, 5, false, true);
/*
22:03:20.871: [5] Result
+------+---------+------+--------+--------------+---------------------------+---------------+
|budget|dept     |deptId|location|emp_name      |emp_address                |emp_city       |
+------+---------+------+--------+--------------+---------------------------+---------------+
|45.567|finance  |45    |OH      |John Doe      |1234 West Broad St 8505    |Columbus       |
|45.567|finance  |45    |OH      |Alex Messi    |4321 North Meecham Rd 300  |Salinas        |
|123.89|marketing|78    |FL      |Michael Scott |9 East Main St 007         |Miami          |
|123.89|marketing|78    |FL      |Jane Doe      |3 Main St #78              |Miami          |
|123.89|marketing|78    |FL      |Sacha Gutierez|314 Great Circle Court 4590|Fort Lauderdale|
+------+---------+------+--------+--------------+---------------------------+---------------+

root
 |-- budget: double (nullable = true)
 |-- dept: string (nullable = true)
 |-- deptId: long (nullable = true)
 |-- location: string (nullable = true)
 |-- emp_name: string (nullable = true)
 |-- emp_address: string (nullable = true)
 |-- emp_city: string (nullable = true)

22:03:21.480: [5] Result has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_32CsvWithEmbdeddedJsonAutomaticJsonifierApp/5_csv
 */
    }

    /**
     * Turns a Row into JSON.
     *
     * @author jgp
     */
    private final class Jsonifier implements MapFunction<Row, String> {
        private static final long serialVersionUID = 19714L;

        private StructField[] fields;
        private final List<String> jsonColumns;

        public Jsonifier(String... jsonColumns) {
            this.fields = null;
            this.jsonColumns = Arrays.asList(jsonColumns);
        }

        @Override
        public String call(Row r) throws Exception {
            if (fields == null) {
                fields = r.schema().fields();
            }
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            int fieldIndex = -1;
            boolean isJsonColumn;
            for (StructField f : fields) {
                isJsonColumn = false;
                fieldIndex++;
                if (fieldIndex > 0) {
                    sb.append(',');
                }
                if (this.jsonColumns.contains(f.name())) {
                    isJsonColumn = true;
                }
                if (!isJsonColumn) {
                    sb.append('"');
                    sb.append(f.name());
                    sb.append("\": ");
                }
                String type = f.dataType().toString();
                switch (type) {
                    case "IntegerType":
                        sb.append(r.getInt(fieldIndex));
                        break;
                    case "LongType":
                        sb.append(r.getLong(fieldIndex));
                        break;
                    case "DoubleType":
                        sb.append(r.getDouble(fieldIndex));
                        break;
                    case "FloatType":
                        sb.append(r.getFloat(fieldIndex));
                        break;
                    case "ShortType":
                        sb.append(r.getShort(fieldIndex));
                        break;

                    default:
                        if (isJsonColumn) {
                            // JSON field
                            String s = r.getString(fieldIndex);
                            if (s != null) {
                                s = s.trim();
                                if (s.charAt(0) == '{') {
                                    s = s.substring(1, s.length() - 1);
                                }
                            }
                            sb.append(s);
                        } else {
                            sb.append('"');
                            sb.append(r.getString(fieldIndex));
                            sb.append('"');
                        }
                        break;
                }
            }
            sb.append('}');
            return sb.toString();
        }
    }

}
