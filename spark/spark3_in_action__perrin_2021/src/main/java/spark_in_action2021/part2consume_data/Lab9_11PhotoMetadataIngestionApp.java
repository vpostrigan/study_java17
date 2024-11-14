package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark_in_action2021.exif.utils.K;

/**
 * Ingest metadata from a directory containing photos, make them available as EXIF.
 * <p>
 * src\main\resources\META-INF\services must contain
 * org.apache.spark.sql.sources.DataSourceRegister
 *
 * @author jgp
 */
public class Lab9_11PhotoMetadataIngestionApp {

    public static void main(String[] args) {
        Lab9_11PhotoMetadataIngestionApp app = new Lab9_11PhotoMetadataIngestionApp();
        app.start();
    }

    private boolean start() {
        SparkSession spark = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local")
                .getOrCreate();

        // Import directory
        String importDirectory = "data/chapter9";

        Dataset<Row> df = spark.read().format("exif")
                .option(K.RECURSIVE, "true")
                .option(K.LIMIT, "100000")
                .option(K.EXTENSIONS, "jpg,jpeg")
                .load(importDirectory);

        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);

/**
 I have imported 14 photos.
 root
 |-- Name: string (nullable = true)
 |-- Size: long (nullable = true)
 |-- Extension: string (nullable = true)
 |-- MimeType: string (nullable = true)
 |-- Date: timestamp (nullable = true)
 |-- Directory: string (nullable = true)
 |-- Filename: string (nullable = false)
 |-- GeoX: float (nullable = true)
 |-- GeoY: float (nullable = true)
 |-- GeoZ: float (nullable = true)
 |-- Height: integer (nullable = true)
 |-- Width: integer (nullable = true)
 |-- FileCreationDate: timestamp (nullable = true)
 |-- FileLastAccessDate: timestamp (nullable = true)
 |-- FileLastModifiedDate: timestamp (nullable = true)
 */

/**
 +--------------------+-------+---------+----------+-------------------+--------------------+--------------------+---------+----------+---------+------+-----+--------------------+--------------------+--------------------+
 |                Name|   Size|Extension|  MimeType|               Date|           Directory|            Filename|     GeoX|      GeoY|     GeoZ|Height|Width|    FileCreationDate|  FileLastAccessDate|FileLastModifiedDate|
 +--------------------+-------+---------+----------+-------------------+--------------------+--------------------+---------+----------+---------+------+-----+--------------------+--------------------+--------------------+
 |A pal of mine (Mi...|1851384|      jpg|image/jpeg|2018-03-24 21:10:53|D:\workspace_stud...|D:\workspace_stud...|44.854095| -93.24203|254.95032|  2320| 3088|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|
 |Coca Cola memorab...| 589607|      jpg|image/jpeg|2018-03-31 17:47:01|D:\workspace_stud...|D:\workspace_stud...|     null|      null|     null|  1080| 1620|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|
 |Ducks (Chapel Hil...|4218303|      jpg|image/jpeg|2016-05-14 02:32:31|D:\workspace_stud...|D:\workspace_stud...|     null|      null|     null|  2317| 5817|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|
 |Ginni Rometty at ...| 469460|      jpg|image/jpeg|2018-03-20 15:34:50|D:\workspace_stud...|D:\workspace_stud...|     null|      null|     null|  1080| 1620|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|
 |Godfrey House (Mi...| 511871|      jpg|image/jpeg|2018-03-24 22:55:01|D:\workspace_stud...|D:\workspace_stud...|44.854633|-93.239494|    233.0|  1080| 1620|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|
 +--------------------+-------+---------+----------+-------------------+--------------------+--------------------+---------+----------+---------+------+-----+--------------------+--------------------+--------------------+
 only showing top 5 rows
 */

        return true;
    }

}
