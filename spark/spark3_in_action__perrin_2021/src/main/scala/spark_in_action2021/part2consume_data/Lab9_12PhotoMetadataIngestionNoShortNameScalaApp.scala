package spark_in_action2021.part2consume_data

import org.apache.spark.sql.SparkSession

/**
 * Ingest metadata from a directory containing photos, make them available as EXIF.
 *
 * @author rambabu.posa
 */
object Lab9_12PhotoMetadataIngestionNoShortNameScalaApp {

  def main(args: Array[String]): Unit = {

    // Get a session
    val spark = SparkSession.builder
      .appName("EXIF to Dataset")
      .master("local")
      .getOrCreate

    // Import directory
    val importDirectory = "data"

    // read the data
    val df = spark.read
      .format("spark_in_action2021.exif.ExifDirectoryDataSourceShortnameAdvertiserScala")
      .option("recursive", "true")
      .option("limit", "100000")
      .option("extensions", "jpg,jpeg")
      .load(importDirectory)

    println(s"I have imported ${df.count} photos.")

    df.printSchema()
    df.show(5)

/**
I have imported 14 photos.
root
 |-- Name: string (nullable = true)
 |-- Size: long (nullable = true)
 |-- Extension: string (nullable = true)
 |-- MimeType: string (nullable = true)
 |-- FileCreationDate: timestamp (nullable = true)
 |-- FileLastAccessDate: timestamp (nullable = true)
 |-- FileLastModifiedDate: timestamp (nullable = true)
 |-- Date: timestamp (nullable = true)
 |-- Directory: string (nullable = true)
 |-- Filename: string (nullable = false)
 |-- GeoX: float (nullable = true)
 |-- GeoY: float (nullable = true)
 |-- GeoZ: float (nullable = true)
 |-- Height: integer (nullable = true)
 |-- Width: integer (nullable = true)
*/

/**
+--------------------+-------+---------+----------+--------------------+--------------------+--------------------+-------------------+--------------------+--------------------+---------+----------+---------+------+-----+
|                Name|   Size|Extension|  MimeType|    FileCreationDate|  FileLastAccessDate|FileLastModifiedDate|               Date|           Directory|            Filename|     GeoX|      GeoY|     GeoZ|Height|Width|
+--------------------+-------+---------+----------+--------------------+--------------------+--------------------+-------------------+--------------------+--------------------+---------+----------+---------+------+-----+
|A pal of mine (Mi...|1851384|      jpg|image/jpeg|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|2018-03-24 21:10:53|D:\workspace_stud...|D:\workspace_stud...|44.854095| -93.24203|254.95032|  2320| 3088|
|Coca Cola memorab...| 589607|      jpg|image/jpeg|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|2018-03-31 17:47:01|D:\workspace_stud...|D:\workspace_stud...|     null|      null|     null|  1080| 1620|
|Ducks (Chapel Hil...|4218303|      jpg|image/jpeg|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|2016-05-14 02:32:31|D:\workspace_stud...|D:\workspace_stud...|     null|      null|     null|  2317| 5817|
|Ginni Rometty at ...| 469460|      jpg|image/jpeg|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|2018-03-20 15:34:50|D:\workspace_stud...|D:\workspace_stud...|     null|      null|     null|  1080| 1620|
|Godfrey House (Mi...| 511871|      jpg|image/jpeg|2022-06-12 16:23:...|2022-06-12 16:23:...| 2021-06-13 20:37:06|2018-03-24 22:55:01|D:\workspace_stud...|D:\workspace_stud...|44.854633|-93.239494|    233.0|  1080| 1620|
+--------------------+-------+---------+----------+--------------------+--------------------+--------------------+-------------------+--------------------+--------------------+---------+----------+---------+------+-----+
only showing top 5 rows
*/
  }

}
