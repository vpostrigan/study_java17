package spark_in_action2021.part3transform_data;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

import java.util.Arrays;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Building a nested document.
 *
 * @author jgp
 */
public class Lab13_13RestaurantDocumentApp {
    private static Logger log = LoggerFactory.getLogger(Lab13_13RestaurantDocumentApp.class);

    public static final String TEMP_COL = "temp_column";

    public static void main(String[] args) {
        Lab13_13RestaurantDocumentApp app = new Lab13_13RestaurantDocumentApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Building a restaurant fact sheet")
                .master("local")
                .getOrCreate();

        // Ingests businesses into dataframe
        Dataset<Row> businessDf = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/orangecounty_restaurants/businesses.CSV");
        businessDf.show(3);
        businessDf.printSchema();
/*
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
|business_id|                name|            address|       city|state|postal_code|latitude|longitude|phone_number|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
| 4068010013|     BURGER KING 212| 600 JONES FERRY RD|   CARRBORO|   NC|      27510|    null|     null|+19199298395|
| 4068010016|CAROL WOODS CAFET...|750 WEAVER DAIRY RD|CHAPEL HILL|   NC|      27514|    null|     null|+19199183203|
| 4068010027|    COUNTRY JUNCTION|      402 WEAVER ST|   CARRBORO|   NC|      27510|    null|     null|+19199292462|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 */
        // Ingests businesses into dataframe
        Dataset<Row> inspectionDf = spark.read().format("csv")
                .option("header", true)
                .load("data/chapter13/orangecounty_restaurants/inspections.CSV");
        inspectionDf.show(3);
        inspectionDf.printSchema();
/*
+-----------+-----+--------+-------+
|business_id|score|    date|   type|
+-----------+-----+--------+-------+
| 4068010013| 95.5|20121029|routine|
| 4068010013| 92.5|20130606|routine|
| 4068010013|   97|20130920|routine|
+-----------+-----+--------+-------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)
 */
        Dataset<Row> factSheetDf = nestedJoin(businessDf, inspectionDf,
                "business_id", "business_id",
                "inner",
                "inspections");
        factSheetDf.show(3, false);
        factSheetDf.printSchema();
/*
+-----------+--------------------+--------------------------+-----------+-----+-----------+--------+---------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|business_id|name                |address                   |city       |state|postal_code|latitude|longitude|phone_number|inspections                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
+-----------+--------------------+--------------------------+-----------+-----+-----------+--------+---------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|4068011069 |FIREHOUSE SUBS      |1726 FORDHAM BLVD UNIT 110|CHAPEL HILL|NC   |27514      |null    |null     |+19849994793|[{4068011069, 99, 20180119, routine}, {4068011069, null, 20180205, followup}, {4068011069, 99, 20180815, routine}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|4068010196 |AMANTE GOURMET PIZZA|300 E MAIN ST             |CARRBORO   |NC   |27510      |null    |null     |+19199293330|[{4068010196, 94, 20121112, routine}, {4068010196, 97, 20130304, routine}, {4068010196, 96, 20130520, routine}, {4068010196, 96, 20130816, routine}, {4068010196, 93, 20131007, routine}, {4068010196, null, 20131010, followup}, {4068010196, null, 20131015, followup}, {4068010196, 97.5, 20140109, routine}, {4068010196, 97, 20140522, routine}, {4068010196, 94.5, 20140902, routine}, {4068010196, null, 20140906, followup}, {4068010196, 97, 20141117, routine}, {4068010196, 94.5, 20150218, routine}, {4068010196, null, 20150415, complaint}, {4068010196, 94, 20150505, routine}, {4068010196, 96.5, 20150803, routine}, {4068010196, 97.5, 20151103, routine}, {4068010196, 96, 20160208, routine}, {4068010196, null, 20160218, followup}, {4068010196, 95.5, 20160606, routine}, {4068010196, 96.5, 20160809, routine}, {4068010196, 96, 20161130, routine}, {4068010196, 97, 20170307, routine}, {4068010196, 97, 20170524, routine}, {4068010196, 96, 20170811, routine}, {4068010196, 95, 20171116, routine}, {4068010196, 95.5, 20180220, routine}, {4068010196, 97.5, 20180608, routine}, {4068010196, 97.5, 20180823, routine}, {4068010196, 95.5, 20181119, routine}]|
|4068010460 |COSMIC CANTINA      |128 E FRANKLIN STREET     |CHAPEL HILL|NC   |27514      |null    |null     |+19199603955|[{4068010460, 97, 20121106, routine}, {4068010460, null, 20121204, complaint}, {4068010460, 95.5, 20130208, routine}, {4068010460, 97, 20130605, routine}, {4068010460, 95, 20130830, routine}, {4068010460, 97.5, 20131031, routine}, {4068010460, null, 20131107, followup}, {4068010460, 99, 20140127, routine}, {4068010460, null, 20140205, followup}, {4068010460, 97, 20140410, routine}, {4068010460, 97.5, 20140717, routine}, {4068010460, 99, 20141017, routine}, {4068010460, null, 20141031, followup}, {4068010460, 100, 20150128, routine}, {4068010460, 99.5, 20150415, routine}, {4068010460, 99, 20150714, routine}, {4068010460, 99, 20151020, routine}, {4068010460, 99, 20160112, routine}, {4068010460, 98.5, 20160517, routine}, {4068010460, 99, 20160727, routine}, {4068010460, 100, 20161007, routine}, {4068010460, 100, 20170209, routine}, {4068010460, 100, 20170516, routine}, {4068010460, 100, 20170720, routine}, {4068010460, 100, 20171103, routine}, {4068010460, 100, 20180201, routine}, {4068010460, 100, 20180423, routine}, {4068010460, 100, 20180710, routine}, {4068010460, 100, 20181016, routine}, {4068010460, 100, 20190108, routine}]    |
+-----------+--------------------+--------------------------+-----------+-----+-----------+--------+---------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- inspections: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- business_id: string (nullable = true)
 |    |    |-- score: string (nullable = true)
 |    |    |-- date: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 */
    }

    /**
     * Builds a nested document from two dataframes.
     *
     * @param leftDf       The left or master document.
     * @param rightDf      The right or details.
     * @param leftJoinCol  Column to link on in the left dataframe.
     * @param rightJoinCol Column to link on in the right dataframe.
     * @param joinType     Type of joins, any type supported by Spark.
     * @param nestedCol    Name of the nested column.
     * @return
     */
    public static Dataset<Row> nestedJoin(Dataset<Row> leftDf, Dataset<Row> rightDf,
                                          String leftJoinCol, String rightJoinCol, String joinType,
                                          String nestedCol) {

        // Performs the join
        Dataset<Row> resDf = leftDf.join(rightDf,
                rightDf.col(rightJoinCol).equalTo(leftDf.col(leftJoinCol)),
                joinType);

        // Makes a list of the left columns (the columns in the master)
        Column[] leftColumns = getColumns(leftDf);
        if (log.isDebugEnabled()) {
            log.debug("  We have {} columns to work with: {}", leftColumns.length,
                    Arrays.toString(leftColumns));
            log.debug("Schema and data:");
            resDf.show(3);
            resDf.printSchema();
        }
/*
2022-09-04 21:55:02.790 -DEBUG --- [           main] n(Lab13_13RestaurantDocumentApp.java:87):   We have 9 columns to work with: [business_id, name, address, city, state, postal_code, latitude, longitude, phone_number]
2022-09-04 21:55:02.790 -DEBUG --- [           main] n(Lab13_13RestaurantDocumentApp.java:89): Schema and data:
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
|business_id|           name|           address|    city|state|postal_code|latitude|longitude|phone_number|business_id|score|    date|   type|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 95.5|20121029|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 92.5|20130606|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013|   97|20130920|routine|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)
 */

        // Copies all the columns from the left/master
        Column[] allColumns = Arrays.copyOf(leftColumns, leftColumns.length + 1);

        // Adds a column, which is a structure containing all the columns from the details
        allColumns[leftColumns.length] = struct(getColumns(rightDf)).alias(TEMP_COL);

        // Performs a select on all columns
        resDf = resDf.select(allColumns);
        if (log.isDebugEnabled()) {
            resDf.show(3);
            resDf.printSchema();
        }
/*
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+--------------------+
|business_id|           name|           address|    city|state|postal_code|latitude|longitude|phone_number|         temp_column|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+--------------------+
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395|{4068010013, 95.5...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395|{4068010013, 92.5...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395|{4068010013, 97, ...|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+--------------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- temp_column: struct (nullable = false)
 |    |-- business_id: string (nullable = true)
 |    |-- score: string (nullable = true)
 |    |-- date: string (nullable = true)
 |    |-- type: string (nullable = true)
 */
        //
        resDf = resDf
                .groupBy(leftColumns)
                .agg(collect_list(col(TEMP_COL)).as(nestedCol));

        if (log.isDebugEnabled()) {
            resDf.show(3);
            resDf.printSchema();
            log.debug("  After nested join, we have {} rows.", resDf.count());
        }
/*
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
|business_id|                name|             address|       city|state|postal_code|latitude|longitude|phone_number|         inspections|
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
| 4068011069|      FIREHOUSE SUBS|1726 FORDHAM BLVD...|CHAPEL HILL|   NC|      27514|    null|     null|+19849994793|[{4068011069, 99,...|
| 4068010196|AMANTE GOURMET PIZZA|       300 E MAIN ST|   CARRBORO|   NC|      27510|    null|     null|+19199293330|[{4068010196, 94,...|
| 4068010460|      COSMIC CANTINA|128 E FRANKLIN ST...|CHAPEL HILL|   NC|      27514|    null|     null|+19199603955|[{4068010460, 97,...|
+-----------+--------------------+--------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
only showing top 3 rows

root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- inspections: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- business_id: string (nullable = true)
 |    |    |-- score: string (nullable = true)
 |    |    |-- date: string (nullable = true)
 |    |    |-- type: string (nullable = true)

2022-09-04 21:55:10.059 -DEBUG --- [           main] (Lab13_13RestaurantDocumentApp.java:115):   After nested join, we have 301 rows.
 */
        return resDf;
    }

    private static Column[] getColumns(Dataset<Row> df) {
        String[] fieldnames = df.columns();
        Column[] columns = new Column[fieldnames.length];
        int i = 0;
        for (String fieldname : fieldnames) {
            columns[i++] = df.col(fieldname);
        }
        return columns;
    }

}
