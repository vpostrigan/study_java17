package spark_in_action2021.part3transform_data;

import scala.Function2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.acos;
import static org.apache.spark.sql.functions.add_months;
import static org.apache.spark.sql.functions.asin;
import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.unbase64;
import static org.apache.spark.sql.functions.weekofyear;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.year;
import static org.apache.spark.sql.functions.zip_with;

public class Lab13_51Functions {

    public static void main(String[] args) {
        Lab13_51Functions app = new Lab13_51Functions();
        app.startAbsApp();
        app.startAcosApp();
        app.startAddMonthsApp();
        app.startAsinApp();
        app.startBase64App();
        app.startConcatApp();
        app.startConcatWsApp();
        app.startUnbase64App();
        app.startWeekofyearApp();
        app.startYearApp();
        app.startZipWithApp();
    }

    /**
     * abs function.
     *
     * @author jgp
     */
    private void startAbsApp() {
        SparkSession spark = SparkSession.builder()
                .appName("abs function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/functions.csv");

        df = df.withColumn("abs", abs(col("val")));
        df.show(5);
/*
+---+-----+----+
|key|  val| abs|
+---+-----+----+
|  A|   -5| 5.0|
|  B|    4| 4.0|
|  C| 3.99|3.99|
|  D|-8.75|8.75|
+---+-----+----+
 */
    }

    /**
     * acos function: inverse cosine of a value in radians
     *
     * @author jgp
     */
    private void startAcosApp() {
        SparkSession spark = SparkSession.builder()
                .appName("acos function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/trigo_arc.csv");

        df = df.withColumn("acos", acos(col("val")));
        df = df.withColumn("acos_by_name", acos("val"));

        df.show();
/*
+-----+------------------+------------------+
|  val|              acos|      acos_by_name|
+-----+------------------+------------------+
|    0|1.5707963267948966|1.5707963267948966|
|    1|               0.0|               0.0|
|   .5|1.0471975511965979|1.0471975511965979|
|  .25| 1.318116071652818| 1.318116071652818|
| -.25|1.8234765819369754|1.8234765819369754|
|-0.25|1.8234765819369754|1.8234765819369754|
+-----+------------------+------------------+
 */
    }

    /**
     * add_months function.
     *
     * @author jgp
     */
    private void startAddMonthsApp() {
        SparkSession spark = SparkSession.builder()
                .appName("abs function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/chapter13/functions/dates.csv");

        df = df
                .withColumn("add_months+2", add_months(col("date_time"), 2))
                .withColumn("add_months+val", add_months(col("date_time"), col("val")));

        df.show(5, false);
        df.printSchema();
/*
+-------------------------+---+------------+--------------+
|date_time                |val|add_months+2|add_months+val|
+-------------------------+---+------------+--------------+
|2020-01-01T00:01:15-03:00|1  |2020-03-01  |2020-02-01    |
|1971-10-05T16:45:00+01:00|12 |1971-12-05  |1972-10-05    |
|2019-11-08T09:30:00-05:00|3  |2020-01-08  |2020-02-08    |
|1970-01-01T00:00:00+00:00|4  |1970-03-01  |1970-05-01    |
|2020-01-06T13:29:08+00:00|5  |2020-03-06  |2020-06-06    |
+-------------------------+---+------------+--------------+

root
 |-- date_time: string (nullable = true)
 |-- val: string (nullable = true)
 |-- add_months+2: date (nullable = true)
 |-- add_months+val: date (nullable = true)
 */
    }

    /**
     * asin function: inverse sine of a value in radians
     *
     * @author jgp
     */
    private void startAsinApp() {
        SparkSession spark = SparkSession.builder()
                .appName("acos function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/trigo_arc.csv");

        df = df.withColumn("asin", asin(col("val")));
        df = df.withColumn("asin_by_name", asin("val"));

        df.show();
/*
+-----+--------------------+--------------------+
|  val|                asin|        asin_by_name|
+-----+--------------------+--------------------+
|    0|                 0.0|                 0.0|
|    1|  1.5707963267948966|  1.5707963267948966|
|   .5|  0.5235987755982989|  0.5235987755982989|
|  .25| 0.25268025514207865| 0.25268025514207865|
| -.25|-0.25268025514207865|-0.25268025514207865|
|-0.25|-0.25268025514207865|-0.25268025514207865|
+-----+--------------------+--------------------+
 */
    }

    /**
     * base64 function.
     *
     * @author jgp
     */
    private void startBase64App() {
        SparkSession spark = SparkSession.builder()
                .appName("base64 function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/strings.csv");

        df = df.withColumn("base64", base64(col("fname")));

        df.show(5);
/*
+-------+-------+------------+
|  fname|  lname|      base64|
+-------+-------+------------+
|Georges| Perrin|R2Vvcmdlcw==|
| Holden|  Karau|    SG9sZGVu|
|  Matei|Zaharia|    TWF0ZWk=|
|  Ginni|Rometty|    R2lubmk=|
| Arvind|Krishna|    QXJ2aW5k|
+-------+-------+------------+
 */
    }

    /**
     * concat function.
     *
     * @author jgp
     */
    private void startConcatApp() {
        SparkSession spark = SparkSession.builder()
                .appName("abs function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/strings.csv");

        df = df.withColumn("jeanify", concat(lit("Jean-"), col("fname")));

        df.show(5);
/*
+-------+-------+------------+
|  fname|  lname|     jeanify|
+-------+-------+------------+
|Georges| Perrin|Jean-Georges|
| Holden|  Karau| Jean-Holden|
|  Matei|Zaharia|  Jean-Matei|
|  Ginni|Rometty|  Jean-Ginni|
| Arvind|Krishna| Jean-Arvind|
+-------+-------+------------+
 */
    }

    /**
     * concat function.
     *
     * @author jgp
     */
    private void startConcatWsApp() {
        SparkSession spark = SparkSession.builder()
                .appName("abs function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/strings.csv");

        df = df.withColumn(
                "jeanify",
                concat_ws("-", lit("Jean"), col("fname")));

        df.show(5);
/*
+-------+-------+------------+
|  fname|  lname|     jeanify|
+-------+-------+------------+
|Georges| Perrin|Jean-Georges|
| Holden|  Karau| Jean-Holden|
|  Matei|Zaharia|  Jean-Matei|
|  Ginni|Rometty|  Jean-Ginni|
| Arvind|Krishna| Jean-Arvind|
+-------+-------+------------+
 */
    }

    /**
     * unbase64 function.
     *
     * @author jgp
     */
    private void startUnbase64App() {
        SparkSession spark = SparkSession.builder()
                .appName("unbase64 function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/functions/strings.csv");

        System.out.println("Output as array of bytes:");
        df = df
                .withColumn("base64", base64(col("fname")))
                .withColumn("unbase64", unbase64(col("base64")));
        df.show(5);

        System.out.println("Output as strings:");
        df = df
                .withColumn("name", col("unbase64").cast(DataTypes.StringType));
        df.show(5);
/*
Output as array of bytes:
+-------+-------+------------+--------------------+
|  fname|  lname|      base64|            unbase64|
+-------+-------+------------+--------------------+
|Georges| Perrin|R2Vvcmdlcw==|[47 65 6F 72 67 6...|
| Holden|  Karau|    SG9sZGVu| [48 6F 6C 64 65 6E]|
|  Matei|Zaharia|    TWF0ZWk=|    [4D 61 74 65 69]|
|  Ginni|Rometty|    R2lubmk=|    [47 69 6E 6E 69]|
| Arvind|Krishna|    QXJ2aW5k| [41 72 76 69 6E 64]|
+-------+-------+------------+--------------------+

Output as strings:
+-------+-------+------------+--------------------+-------+
|  fname|  lname|      base64|            unbase64|   name|
+-------+-------+------------+--------------------+-------+
|Georges| Perrin|R2Vvcmdlcw==|[47 65 6F 72 67 6...|Georges|
| Holden|  Karau|    SG9sZGVu| [48 6F 6C 64 65 6E]| Holden|
|  Matei|Zaharia|    TWF0ZWk=|    [4D 61 74 65 69]|  Matei|
|  Ginni|Rometty|    R2lubmk=|    [47 69 6E 6E 69]|  Ginni|
| Arvind|Krishna|    QXJ2aW5k| [41 72 76 69 6E 64]| Arvind|
+-------+-------+------------+--------------------+-------+
 */
    }

    /**
     * week of year function.
     *
     * @author jgp
     */
    private void startWeekofyearApp() {
        SparkSession spark = SparkSession.builder()
                .appName("weekofyear function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/chapter13/functions/dates.csv");

        df = df.withColumn("weekofyear", weekofyear(col("date_time")));

        df.show(5, false);
        df.printSchema();
/*
+-------------------------+---+----------+
|date_time                |val|weekofyear|
+-------------------------+---+----------+
|2020-01-01T00:01:15-03:00|1  |1         |
|1971-10-05T16:45:00+01:00|12 |40        |
|2019-11-08T09:30:00-05:00|3  |45        |
|1970-01-01T00:00:00+00:00|4  |1         |
|2020-01-06T13:29:08+00:00|5  |2         |
+-------------------------+---+----------+

root
 |-- date_time: string (nullable = true)
 |-- val: string (nullable = true)
 |-- weekofyear: integer (nullable = true)
 */
    }

    /**
     * year function.
     *
     * @author jgp
     */
    private void startYearApp() {
        SparkSession spark = SparkSession.builder()
                .appName("year function")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/chapter13/functions/dates.csv");

        df = df.withColumn("year", year(col("date_time")));

        df.show(5, false);
        df.printSchema();
/*
+-------------------+---+----+
|date_time          |val|year|
+-------------------+---+----+
|2020-01-01 05:01:15|1  |2020|
|1971-10-05 17:45:00|12 |1971|
|2019-11-08 16:30:00|3  |2019|
|1970-01-01 02:00:00|4  |1970|
|2020-01-06 15:29:08|5  |2020|
+-------------------+---+----+

root
 |-- date_time: timestamp (nullable = true)
 |-- val: integer (nullable = true)
 |-- year: integer (nullable = true)
 */
    }

    /**
     * zip_with function.
     *
     * @author jgp
     */
    private void startZipWithApp() {
        SparkSession spark = SparkSession.builder()
                .appName("abs function")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("c1", DataTypes.createArrayType(DataTypes.IntegerType), false),
                DataTypes.createStructField("c2", DataTypes.createArrayType(DataTypes.IntegerType), false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(
                new int[]{1010, 1012},
                new int[]{1021, 1023, 1025}));
        rows.add(RowFactory.create(
                new int[]{2010, 2012, 2014},
                new int[]{2021, 2023}));
        rows.add(RowFactory.create(
                new int[]{3010, 3012},
                new int[]{3021, 3023}));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        System.out.println("Input");
        df.show(5, false);

        df = df.withColumn("zip_with",
                zip_with(col("c1"), col("c2"), new ZipWithFunction()));

        System.out.println("After zip_with");
        df.show(5, false);
/*
Input
+------------------+------------------+
|c1                |c2                |
+------------------+------------------+
|[1010, 1012]      |[1021, 1023, 1025]|
|[2010, 2012, 2014]|[2021, 2023]      |
|[3010, 3012]      |[3021, 3023]      |
+------------------+------------------+

After zip_with
+------------------+------------------+----------------+
|c1                |c2                |zip_with        |
+------------------+------------------+----------------+
|[1010, 1012]      |[1021, 1023, 1025]|[2031, 2035, -1]|
|[2010, 2012, 2014]|[2021, 2023]      |[4031, 4035, -1]|
|[3010, 3012]      |[3021, 3023]      |[6031, 6035]    |
+------------------+------------------+----------------+
 */
    }

    /**
     * Takes two values and adds them. If one of the value is null, as the
     * addition cannot be performed, the result is -1.
     * <p>
     * The processing is taking place in the apply() method of the function.
     * It can be implemented as a Java lambda function.
     *
     * @author jgp
     */
    public class ZipWithFunction implements Function2<Column, Column, Column> {
        @Override
        public Column apply(Column v1, Column v2) {
            return when(v1.isNull().or(v2.isNull()), lit(-1))
                    .otherwise(v1.$plus(v2));
        }
    }

}
