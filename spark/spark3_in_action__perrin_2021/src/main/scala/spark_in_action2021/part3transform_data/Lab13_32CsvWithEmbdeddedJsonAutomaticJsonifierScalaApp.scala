package spark_in_action2021.part3transform_data

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions => F}

/**
 * Ingesting a CSV with embedded JSON.
 *
 * @author rambabu.posa
 */
@SerialVersionUID(19713L)
class CsvWithEmbdeddedJsonAutomaticJsonifierScalaApp extends Serializable {

  /**
   * Turns a Row into JSON.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(19714L)
  final private class Jsonifier(val jsonColumns: String*) extends MapFunction[Row, String] {
    private var fields: Array[StructField] = null

    @throws[Exception]
    override def call(r: Row): String = {
      if (fields == null)
        fields = r.schema.fields
      val sb: StringBuffer = new StringBuffer
      sb.append('{')
      var fieldIndex: Int = -1
      var isJsonColumn: Boolean = false
      for (f <- fields) {
        isJsonColumn = false
        fieldIndex += 1
        if (fieldIndex > 0) sb.append(',')
        if (jsonColumns.contains(f.name)) isJsonColumn = true
        if (!isJsonColumn) {
          sb.append('"')
          sb.append(f.name)
          sb.append("\": ")
        }
        val `type`: String = f.dataType.toString
        `type` match {
          case "IntegerType" =>
            sb.append(r.getInt(fieldIndex))

          case "LongType" =>
            sb.append(r.getLong(fieldIndex))

          case "DoubleType" =>
            sb.append(r.getDouble(fieldIndex))

          case "FloatType" =>
            sb.append(r.getFloat(fieldIndex))

          case "ShortType" =>
            sb.append(r.getShort(fieldIndex))

          case _ =>
            if (isJsonColumn) { // JSON field
              var s: String = r.getString(fieldIndex)
              if (s != null) {
                s = s.trim
                if (s.charAt(0) == '{') s = s.substring(1, s.length - 1)
              }
              sb.append(s)
            }
            else {
              sb.append('"')
              sb.append(r.getString(fieldIndex))
              sb.append('"')
            }

        }
      }
      sb.append('}')
      sb.toString
    }
  }

  def start(): Unit = {
    val spark = SparkSession.builder
      .appName("Processing of invoices")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .option("header", true)
      .option("delimiter", "|")
      .option("inferSchema", true)
      .csv("data/chapter13/misc/csv_with_embedded_json2.csv")

    df.show(5, false)
    df.printSchema()

    val ds = df.map(new Jsonifier("emp_json"), Encoders.STRING)
    ds.show(5, false)
    ds.printSchema()

    var dfJson = spark.read.json(ds)
    dfJson.show(5, false)
    dfJson.printSchema()

    dfJson = dfJson
      .withColumn("emp", F.explode(F.col("employee")))
      .drop("employee")

    dfJson.show(5, false)
    dfJson.printSchema()

    dfJson = dfJson
      .withColumn("emp_name",
        F.concat(F.col("emp.name.firstName"), F.lit(" "), F.col("emp.name.lastName")))
      .withColumn("emp_address",
        F.concat(F.col("emp.address.street"), F.lit(" "), F.col("emp.address.unit")))
      .withColumn("emp_city", F.col("emp.address.city"))
      .drop("emp")

    dfJson.show(5, false)
    dfJson.printSchema()

    spark.stop
  }

}

object CsvWithEmbdeddedJsonAutomaticJsonifierScalaApplication {

  def main(args: Array[String]): Unit = {

    val app = new CsvWithEmbdeddedJsonAutomaticJsonifierScalaApp
    app.start

  }

}
/*
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

+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{"deptId": 45,"dept": "finance","budget": 45.567, "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}],"location": "OH"}                                                                                                                         |
|{"deptId": 78,"dept": "marketing","budget": 123.89, "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}],"location": "FL"}|
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

root
 |-- value: string (nullable = true)

+------+---------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|budget|dept     |deptId|employee                                                                                                                                                        |location|
+------+---------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
|45.567|finance  |45    |[{{Columbus, 1234 West Broad St, 8505}, {John, Doe}}, {{Salinas, 4321 North Meecham Rd, 300}, {Alex, Messi}}]                                                   |OH      |
|123.89|marketing|78    |[{{Miami, 9 East Main St, 007}, {Michael, Scott}}, {{Miami, 3 Main St, #78}, {Jane, Doe}}, {{Fort Lauderdale, 314 Great Circle Court, 4590}, {Sacha, Gutierez}}]|FL      |
+------+---------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+

root
 |-- budget: double (nullable = true)
 |-- dept: string (nullable = true)
 |-- deptId: long (nullable = true)
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
 */