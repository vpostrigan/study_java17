package spark_in_action2021.part3transform_data

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions => F}

/**
 * Ingesting a CSV with embedded JSON.
 *
 * @author rambabu.posa
 */
@SerialVersionUID(19711L)
class Lab13_31CsvWithEmbdeddedJsonScalaApp extends Serializable {

  /**
   * Turns a Row into JSON. Not very fail safe, but done to illustrate.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(19712L)
  final private class Jsonifier extends MapFunction[Row, String] {
    @throws[Exception]
    override def call(r: Row): String = {
      println(r.mkString)
      val sb = new StringBuffer
      sb.append("{ \"dept\": \"")
      sb.append(r.getString(0))
      sb.append("\",")
      var s = r.getString(1).toString
      if (s != null) {
        s = s.trim
        if (s.charAt(0) == '{')
          s = s.substring(1, s.length - 1)
      }
      sb.append(s)
      sb.append(", \"location\": \"")
      sb.append(r.getString(2))
      sb.append("\"}")
      sb.toString
    }
  }

  /**
   * The processing code.
   */
  def start(): Unit = {
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Processing of invoices")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("data/chapter13/misc/csv_with_embedded_json.csv")

    df.show(5, false)
    df.printSchema()

    val ds = df.map(new Jsonifier, Encoders.STRING)
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

object CsvWithEmbdeddedJsonScalaApplication {

  def main(args: Array[String]): Unit = {
    val app = new Lab13_31CsvWithEmbdeddedJsonScalaApp
    app.start
  }

}
/*
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

finance{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}OH
marketing{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}FL
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                                                                                                                                                                                                                                             |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{ "dept": "finance", "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}], "location": "OH"}                                                                                                                         |
|{ "dept": "marketing", "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}], "location": "FL"}|
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

root
 |-- value: string (nullable = true)

finance{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}OH
marketing{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}FL
finance{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}OH
marketing{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}FL
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

finance{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}OH
marketing{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}FL
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

finance{ "employee":[{"name":{"firstName":"John","lastName":"Doe"},"address":{"street":"1234 West Broad St","unit":"8505","city":"Columbus"}},{"name":{"firstName":"Alex","lastName":"Messi"},"address":{"street":"4321 North Meecham Rd","unit":"300","city":"Salinas"}}]}OH
marketing{ "employee":[{"name":{"firstName":"Michael","lastName":"Scott"},"address":{"street":"9 East Main St","unit":"007","city":"Miami"}},{"name":{"firstName":"Jane","lastName":"Doe"},"address":{"street":"3 Main St","unit":"#78","city":"Miami"}},{"name":{"firstName":"Sacha","lastName":"Gutierez"},"address":{"street":"314 Great Circle Court","unit":"4590","city":"Fort Lauderdale"}}]}FL
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

 */