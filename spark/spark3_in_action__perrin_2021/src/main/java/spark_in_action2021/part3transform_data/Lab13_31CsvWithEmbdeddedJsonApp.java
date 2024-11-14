package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.Logs;

/**
 * Ingesting a CSV with embedded JSON.
 *
 * @author jgp
 */
public class Lab13_31CsvWithEmbdeddedJsonApp implements Serializable {
    private static final long serialVersionUID = 19711L;

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab13_31CsvWithEmbdeddedJsonApp app = new Lab13_31CsvWithEmbdeddedJsonApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        SparkSession spark = SparkSession.builder()
                .appName("Processing of invoices")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("delimiter", "|")
                .csv("data/chapter13/misc/csv_with_embedded_json.csv");
        allLogs.showAndSaveToCsv("[1] data csv", df, 5, false, true);
/*
19:42:59.375: [1] data csv
+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|dept     |emp_json                                                                                                                                                                                                                                                                                                                                                                                   |location|
+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|finance  |{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}                                                                                                                       |OH      |
|marketing|{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}|FL      |
+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+

root
 |-- dept: string (nullable = true)
 |-- emp_json: string (nullable = true)
 |-- location: string (nullable = true)

19:43:00.006: [1] data csv has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_31CsvWithEmbdeddedJsonApp/1_csv
 */

        Dataset<String> ds = df.map(new Jsonifier(), Encoders.STRING());
        ds = ds.cache(); // If remove cache, Jsonifier will work on all next Dataset transformations
        allLogs.showAndSaveToCsv("[2] map", ds, 5, false, true);
/*
19:43:00.350: [2] map
Row - mkString: finance{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}OH
Row - mkString: marketing{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}FL
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                                                                                                                                                                                                                                             |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{ "dept": "finance", "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}], "location": "OH"}                                                                                                                         |
|{ "dept": "marketing", "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}], "location": "FL"}|
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

root
 |-- value: string (nullable = true)

19:43:03.016: [2] map has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_31CsvWithEmbdeddedJsonApp/2_csv
 */

        Dataset<Row> dfJson = spark.read().json(ds);
        allLogs.showAndSaveToCsv("[3] data json", dfJson, 5, false, true);
/*
19:43:03.355: [3] data json
+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|dept     |employee                                                                                                                                                        |location|
+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|finance  |[{{Columbus, 1234 West Broad St, 8505}, {John, Doe}}, {{Salinas, 4321 North Meecham Rd, 300}, {Alex, Messi}}]                                                   |OH      |
|marketing|[{{Miami, 9 East Main St, 007}, {Michael, Scott}}, {{Miami, 3 Main St, #78}, {Jane, Doe}}, {{Fort Lauderdale, 314 Great Circle Court, 4590}, {Sacha, Gutierez}}]|FL      |
+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+

root
 |-- dept: string (nullable = true)
 |-- employee: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- address: struct (nullable = true)
 |    |    |    |-- city: string (nullable = true)
 |    |    |    |-- street: string (nullable = true)
 |    |    |    |-- unit: string (nullable = true)
 |    |    |-- name: struct (nullable = true)
 |    |    |    |-- firstName: string (nullable = true)
 |    |    |    |-- lastName: string (nullable = true)
 |-- location: string (nullable = true)
19:43:03.569: [3] data json has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_31CsvWithEmbdeddedJsonApp/3_csv
 */
        dfJson = dfJson
                .withColumn("emp", explode(dfJson.col("employee")))
                .drop("employee");
        allLogs.showAndSaveToCsv("[4] data json", dfJson, 5, false, true);
/*
19:43:03.721: [4] data json
+---------+--------+--------------------------------------------------------------------+
|dept     |location|emp                                                                 |
+---------+--------+--------------------------------------------------------------------+
|finance  |OH      |{{Columbus, 1234 West Broad St, 8505}, {John, Doe}}                 |
|finance  |OH      |{{Salinas, 4321 North Meecham Rd, 300}, {Alex, Messi}}              |
|marketing|FL      |{{Miami, 9 East Main St, 007}, {Michael, Scott}}                    |
|marketing|FL      |{{Miami, 3 Main St, #78}, {Jane, Doe}}                              |
|marketing|FL      |{{Fort Lauderdale, 314 Great Circle Court, 4590}, {Sacha, Gutierez}}|
+---------+--------+--------------------------------------------------------------------+

root
 |-- dept: string (nullable = true)
 |-- location: string (nullable = true)
 |-- emp: struct (nullable = true)
 |    |-- address: struct (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- street: string (nullable = true)
 |    |    |-- unit: string (nullable = true)
 |    |-- name: struct (nullable = true)
 |    |    |-- firstName: string (nullable = true)
 |    |    |-- lastName: string (nullable = true)
19:43:04.005: [4] data json has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_31CsvWithEmbdeddedJsonApp/4_csv
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
19:43:04.244: [5] result
+---------+--------+--------------+---------------------------+---------------+
|dept     |location|emp_name      |emp_address                |emp_city       |
+---------+--------+--------------+---------------------------+---------------+
|finance  |OH      |John Doe      |1234 West Broad St 8505    |Columbus       |
|finance  |OH      |Alex Messi    |4321 North Meecham Rd 300  |Salinas        |
|marketing|FL      |Michael Scott |9 East Main St 007         |Miami          |
|marketing|FL      |Jane Doe      |3 Main St #78              |Miami          |
|marketing|FL      |Sacha Gutierez|314 Great Circle Court 4590|Fort Lauderdale|
+---------+--------+--------------+---------------------------+---------------+

root
 |-- dept: string (nullable = true)
 |-- location: string (nullable = true)
 |-- emp_name: string (nullable = true)
 |-- emp_address: string (nullable = true)
 |-- emp_city: string (nullable = true)

19:43:04.613: [5] result has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab13_31CsvWithEmbdeddedJsonApp/5_csv
 */
    }

    /**
     * Turns a Row into JSON. Not very fail safe, but done to illustrate.
     *
     * @author jgp
     */
    private final class Jsonifier implements MapFunction<Row, String> {
        private static final long serialVersionUID = 19712L;

        @Override
        public String call(Row r) throws Exception {
            System.out.println("Row - mkString: " + r.mkString());

            StringBuilder sb = new StringBuilder();
            sb.append("{ \"dept\": \"");
            sb.append(r.getString(0));
            sb.append("\",");
            String s = r.getString(1);
            if (s != null) {
                s = s.trim();
                if (s.charAt(0) == '{') {
                    s = s.substring(1, s.length() - 1);
                }
            }
            sb.append(s);
            sb.append(", \"location\": \"");
            sb.append(r.getString(2));
            sb.append("\"}");
            return sb.toString();
        }
    }

}
