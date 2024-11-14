package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lab17_11ExportWildfiresApp {
    private static Logger log = LoggerFactory.getLogger(Lab17_11ExportWildfiresApp.class);

    private static String MODIS_FILE;
    private static String VIIRS_FILE;
    private static String TMP_STORAGE;
    private static String MODIS_URL;
    private static String VIIRS_URL;

    static {
        MODIS_FILE = "MODIS_C6_Global_24h.csv";
        VIIRS_FILE = "VNP14IMGTDL_NRT_Global_24h.csv";
        TMP_STORAGE = "/tmp";

        MODIS_URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/"
                + MODIS_FILE;
        VIIRS_URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/"
                + VIIRS_FILE;

        // NEW
        // https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv
        // https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv

        MODIS_FILE = "MODIS_C6_1_Global_24h.csv";
        VIIRS_FILE = "SUOMI_VIIRS_C2_Global_24h.csv";

        MODIS_URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/"
                + MODIS_FILE;
        VIIRS_URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/"
                + VIIRS_FILE;

        MODIS_FILE = TMP_STORAGE + "/" + MODIS_FILE;
        VIIRS_FILE = TMP_STORAGE + "/" + VIIRS_FILE;
    }

    public static void main(String[] args) {
        Lab17_11ExportWildfiresApp app = new Lab17_11ExportWildfiresApp();
        app.start();
    }

    private boolean start() {
        Dataset<Row> wildfireDf = process();
        wildfireDf.write().format("parquet")
                .mode(SaveMode.Overwrite)
                .save(TMP_STORAGE + "chapter16_fires_parquet");

        Dataset<Row> outputDf = wildfireDf
                .filter("confidence_level = 'high'")
                .repartition(1);
        outputDf.write().format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save(TMP_STORAGE + "chapter16_high_confidence_fires_csv");

        return true;
    }

    Dataset<Row> process() {
        System.out.println(new File(TMP_STORAGE).getAbsolutePath());

        if (!downloadWildfiresDatafiles()) {
            return null;
        }

        SparkSession spark = SparkSession.builder()
                .appName("Wildfire data pipeline")
                .master("local[*]")
                .getOrCreate();

        // Format the VIIRS dataset
        Dataset<Row> viirsDf = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(VIIRS_FILE)
                .withColumn("acq_time_min", expr("acq_time % 100"))
                .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
                // change from the book: to have a proper type in the schema
                .withColumn("acq_time2", unix_timestamp(col("acq_date").cast(DataTypes.DateType)))
                .withColumn("acq_time3",
                        expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
                .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
                .drop("acq_date")
                .drop("acq_time")
                .drop("acq_time_min")
                .drop("acq_time_hr")
                .drop("acq_time2")
                .drop("acq_time3")
                .withColumnRenamed("confidence", "confidence_level")
                .withColumn("brightness", lit(null))
                .withColumn("bright_t31", lit(null));
        viirsDf.show();
        viirsDf.printSchema();

        // This piece of code shows the repartition by confidence level, so you
        // can compare when you convert the confidence as a % to a level for the
        // MODIS dataset.
        Dataset<Row> df = viirsDf
                .groupBy("confidence_level")
                .count();
        long count = viirsDf.count();
        df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2));
        df.show();

        // Format the MODIF dataset
        int low = 40;
        int high = 100;
        Dataset<Row> modisDf = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(MODIS_FILE)
                .withColumn("acq_time_min", expr("acq_time % 100"))
                .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
                // change from the book: to have a proper type in the schema
                .withColumn("acq_time2", unix_timestamp(col("acq_date").cast(DataTypes.DateType)))
                .withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
                .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
                .drop("acq_date")
                .drop("acq_time")
                .drop("acq_time_min")
                .drop("acq_time_hr")
                .drop("acq_time2")
                .drop("acq_time3")
                .withColumn("confidence_level",
                        when(
                                col("confidence").$less$eq(low), "low"))
                .withColumn("confidence_level",
                        when(
                                col("confidence").$greater(low)
                                        .and(col("confidence").$less(high)),
                                "nominal")
                                .otherwise(col("confidence_level")))
                .withColumn("confidence_level",
                        when(
                                isnull(col("confidence_level")), "high")
                                .otherwise(col("confidence_level")))
                .drop("confidence")
                .withColumn("bright_ti4", lit(null))
                .withColumn("bright_ti5", lit(null));
        modisDf.show();
        modisDf.printSchema();

        // This piece of code shows the repartition by confidence level, so you
        // can compare when you convert the confidence as a % to a level for the
        // MODIS dataset.
        df = modisDf
                .groupBy("confidence_level")
                .count();
        count = modisDf.count();
        df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2));
        df.show();

        Dataset<Row> wildfireDf = viirsDf.unionByName(modisDf);
        wildfireDf.show();
        wildfireDf.printSchema();

        log.info("# of partitions: {}", wildfireDf.rdd().getNumPartitions());
        return wildfireDf;
    }

    private boolean downloadWildfiresDatafiles() {
        log.trace("-> downloadWildfiresDatafiles()");

        // Download the MODIS data file
        if (!download(MODIS_URL, MODIS_FILE)) {
            return false;
        }

        // Download the VIIRS data file
        if (!download(VIIRS_URL, VIIRS_FILE)) {
            return false;
        }

        return true;
    }

    /**
     * Downloads data files to local temp value.
     */
    private boolean download(String fromFile, String toFile) {
        try {
            URL website = new URL(fromFile);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(toFile);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.close();
            rbc.close();
        } catch (IOException e) {
            log.debug("Error while downloading '{}', got: {}", fromFile,
                    e.getMessage(), e);
            return false;
        }

        log.debug("{} downloaded successfully.", toFile);
        return true;
    }

}
/*
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
|latitude|longitude|bright_ti4|scan|track|satellite|confidence_level|version|bright_ti5| frp|daynight|       acq_datetime|brightness|bright_t31|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
|41.24279| 36.46227|     318.4|0.41| 0.45|        N|         nominal| 2.0NRT|    283.01|2.03|       N|2022-11-03 00:01:00|      null|      null|
|43.24212| 23.88553|    316.64|0.54| 0.42|        N|         nominal| 2.0NRT|    286.59|2.79|       N|2022-11-03 00:01:00|      null|      null|
|43.23824| 23.88465|    298.19|0.54| 0.42|        N|         nominal| 2.0NRT|    285.12|1.43|       N|2022-11-03 00:01:00|      null|      null|
|43.53453| 18.27356|    301.26|0.34| 0.56|        N|         nominal| 2.0NRT|    278.48| 1.3|       N|2022-11-03 00:01:00|      null|      null|
|41.02554| 36.09987|    297.87| 0.4| 0.44|        N|         nominal| 2.0NRT|    277.92|0.53|       N|2022-11-03 00:01:00|      null|      null|
|43.32331| 18.56864|    299.52|0.32| 0.55|        N|         nominal| 2.0NRT|    280.27|0.85|       N|2022-11-03 00:01:00|      null|      null|
|43.31853| 18.56237|    299.62|0.32| 0.55|        N|         nominal| 2.0NRT|    278.82|1.33|       N|2022-11-03 00:01:00|      null|      null|
|43.31836| 18.56538|    303.13|0.32| 0.55|        N|         nominal| 2.0NRT|    279.46|0.85|       N|2022-11-03 00:01:00|      null|      null|
|43.31819| 18.56836|     295.2|0.32| 0.55|        N|         nominal| 2.0NRT|    279.38|0.85|       N|2022-11-03 00:01:00|      null|      null|
|43.31801| 18.57149|     301.2|0.32| 0.55|        N|         nominal| 2.0NRT|    279.57|1.17|       N|2022-11-03 00:01:00|      null|      null|
|43.31782| 18.57482|    306.57|0.32| 0.55|        N|         nominal| 2.0NRT|    279.33|1.17|       N|2022-11-03 00:01:00|      null|      null|
|43.30166| 18.58797|    313.97|0.32| 0.55|        N|         nominal| 2.0NRT|    280.88|1.63|       N|2022-11-03 00:01:00|      null|      null|
|41.17582|  32.6341|    298.37|0.44| 0.38|        N|         nominal| 2.0NRT|    279.32| 1.3|       N|2022-11-03 00:01:00|      null|      null|
|41.17237| 32.63293|    299.25|0.44| 0.38|        N|         nominal| 2.0NRT|    276.19|0.99|       N|2022-11-03 00:01:00|      null|      null|
|41.26618|  31.4225|    335.45| 0.4| 0.37|        N|         nominal| 2.0NRT|    285.84|2.74|       N|2022-11-03 00:01:00|      null|      null|
|41.26534| 31.42726|    302.36|0.41| 0.37|        N|         nominal| 2.0NRT|    285.45|2.74|       N|2022-11-03 00:01:00|      null|      null|
|41.26199| 31.42622|    305.55|0.41| 0.37|        N|         nominal| 2.0NRT|    284.15|1.22|       N|2022-11-03 00:01:00|      null|      null|
|41.25698| 31.41461|    297.02| 0.4| 0.37|        N|         nominal| 2.0NRT|    280.49|1.85|       N|2022-11-03 00:01:00|      null|      null|
|41.25364| 31.41356|    331.56| 0.4| 0.37|        N|         nominal| 2.0NRT|    281.93|1.85|       N|2022-11-03 00:01:00|      null|      null|
|42.44389| 20.03925|    313.66|0.54| 0.51|        N|         nominal| 2.0NRT|    280.68|4.15|       N|2022-11-03 00:01:00|      null|      null|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
only showing top 20 rows

root
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- bright_ti4: double (nullable = true)
 |-- scan: double (nullable = true)
 |-- track: double (nullable = true)
 |-- satellite: string (nullable = true)
 |-- confidence_level: string (nullable = true)
 |-- version: string (nullable = true)
 |-- bright_ti5: double (nullable = true)
 |-- frp: double (nullable = true)
 |-- daynight: string (nullable = true)
 |-- acq_datetime: string (nullable = true)
 |-- brightness: null (nullable = true)
 |-- bright_t31: null (nullable = true)

+----------------+-----+-----+
|confidence_level|count|    %|
+----------------+-----+-----+
|         nominal|66594|82.21|
|             low|10587|13.07|
|            high| 3821| 4.72|
+----------------+-----+-----+

+---------+---------+----------+----+-----+---------+-------+----------+------+--------+-------------------+----------------+----------+----------+
| latitude|longitude|brightness|scan|track|satellite|version|bright_t31|   frp|daynight|       acq_datetime|confidence_level|bright_ti4|bright_ti5|
+---------+---------+----------+----+-----+---------+-------+----------+------+--------+-------------------+----------------+----------+----------+
|-10.22615|-36.33052|    313.52|2.53| 1.53|        T| 6.1NRT|    289.06| 43.83|       N|2022-11-03 00:18:00|         nominal|      null|      null|
|-10.22887|-36.35327|    344.54|2.54| 1.53|        T| 6.1NRT|     290.1|197.89|       N|2022-11-03 00:18:00|            high|      null|      null|
|-10.16703|-36.17933|    341.49|2.46| 1.51|        T| 6.1NRT|    292.03|166.33|       N|2022-11-03 00:18:00|            high|      null|      null|
| -10.1696|-36.20132|    337.61|2.47| 1.51|        T| 6.1NRT|    291.49|144.49|       N|2022-11-03 00:18:00|            high|      null|      null|
|-10.21895|-36.33791|    309.89|2.53| 1.53|        T| 6.1NRT|    289.21| 33.35|       N|2022-11-03 00:18:00|         nominal|      null|      null|
|-10.22188|-36.36099|    311.96|2.54| 1.53|        T| 6.1NRT|    289.79| 39.72|       N|2022-11-03 00:18:00|         nominal|      null|      null|
|-10.17495|-36.20708|    322.34|2.47| 1.51|        T| 6.1NRT|    290.75| 71.39|       N|2022-11-03 00:18:00|            high|      null|      null|
|-20.53574| 16.35948|    314.44|1.74| 1.29|        A| 6.1NRT|    288.52| 32.39|       N|2022-11-03 00:03:00|         nominal|      null|      null|
| -20.5327| 16.35442|    315.11|1.74| 1.29|        A| 6.1NRT|    288.46| 33.41|       N|2022-11-03 00:03:00|         nominal|      null|      null|
|-32.70778| 19.17226|    314.72| 1.0|  1.0|        A| 6.1NRT|    285.78| 15.71|       N|2022-11-03 00:05:00|         nominal|      null|      null|
|-32.74784| 19.20163|    320.69| 1.0|  1.0|        A| 6.1NRT|    286.88| 20.63|       N|2022-11-03 00:05:00|            high|      null|      null|
|  -13.478|-172.3158|    310.28|3.51| 1.76|        A| 6.1NRT|    294.27| 37.99|       D|2022-11-03 00:41:00|             low|      null|      null|
| -13.5068|-172.5808|    313.17|3.68|  1.8|        A| 6.1NRT|    294.83| 47.36|       D|2022-11-03 00:41:00|             low|      null|      null|
|-23.87759|129.96254|    331.78|1.03| 1.01|        T| 6.1NRT|    312.64| 13.02|       D|2022-11-03 01:15:00|         nominal|      null|      null|
|-23.87911|129.97252|    331.55|1.03| 1.01|        T| 6.1NRT|    313.35| 12.63|       D|2022-11-03 01:15:00|         nominal|      null|      null|
|-23.89095|129.86766|    357.48|1.03| 1.02|        T| 6.1NRT|    315.59| 65.09|       D|2022-11-03 01:15:00|         nominal|      null|      null|
|-23.89249|129.87769|    331.33|1.03| 1.02|        T| 6.1NRT|    313.85| 12.28|       D|2022-11-03 01:15:00|         nominal|      null|      null|
|-23.90155|129.87607|    340.97|1.03| 1.02|        T| 6.1NRT|     313.6| 28.04|       D|2022-11-03 01:15:00|         nominal|      null|      null|
|-23.91048|129.87868|    342.71|1.03| 1.02|        T| 6.1NRT|    311.94| 31.96|       D|2022-11-03 01:15:00|         nominal|      null|      null|
|-23.91204| 129.8887|    337.88|1.03| 1.01|        T| 6.1NRT|    313.09| 23.74|       D|2022-11-03 01:15:00|         nominal|      null|      null|
+---------+---------+----------+----+-----+---------+-------+----------+------+--------+-------------------+----------------+----------+----------+
only showing top 20 rows

root
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- brightness: double (nullable = true)
 |-- scan: double (nullable = true)
 |-- track: double (nullable = true)
 |-- satellite: string (nullable = true)
 |-- version: string (nullable = true)
 |-- bright_t31: double (nullable = true)
 |-- frp: double (nullable = true)
 |-- daynight: string (nullable = true)
 |-- acq_datetime: string (nullable = true)
 |-- confidence_level: string (nullable = true)
 |-- bright_ti4: null (nullable = true)
 |-- bright_ti5: null (nullable = true)

+----------------+-----+-----+
|confidence_level|count|    %|
+----------------+-----+-----+
|         nominal|14756|82.77|
|             low| 2110|11.84|
|            high|  961| 5.39|
+----------------+-----+-----+

+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
|latitude|longitude|bright_ti4|scan|track|satellite|confidence_level|version|bright_ti5| frp|daynight|       acq_datetime|brightness|bright_t31|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
|41.24279| 36.46227|     318.4|0.41| 0.45|        N|         nominal| 2.0NRT|    283.01|2.03|       N|2022-11-03 00:01:00|      null|      null|
|43.24212| 23.88553|    316.64|0.54| 0.42|        N|         nominal| 2.0NRT|    286.59|2.79|       N|2022-11-03 00:01:00|      null|      null|
|43.23824| 23.88465|    298.19|0.54| 0.42|        N|         nominal| 2.0NRT|    285.12|1.43|       N|2022-11-03 00:01:00|      null|      null|
|43.53453| 18.27356|    301.26|0.34| 0.56|        N|         nominal| 2.0NRT|    278.48| 1.3|       N|2022-11-03 00:01:00|      null|      null|
|41.02554| 36.09987|    297.87| 0.4| 0.44|        N|         nominal| 2.0NRT|    277.92|0.53|       N|2022-11-03 00:01:00|      null|      null|
|43.32331| 18.56864|    299.52|0.32| 0.55|        N|         nominal| 2.0NRT|    280.27|0.85|       N|2022-11-03 00:01:00|      null|      null|
|43.31853| 18.56237|    299.62|0.32| 0.55|        N|         nominal| 2.0NRT|    278.82|1.33|       N|2022-11-03 00:01:00|      null|      null|
|43.31836| 18.56538|    303.13|0.32| 0.55|        N|         nominal| 2.0NRT|    279.46|0.85|       N|2022-11-03 00:01:00|      null|      null|
|43.31819| 18.56836|     295.2|0.32| 0.55|        N|         nominal| 2.0NRT|    279.38|0.85|       N|2022-11-03 00:01:00|      null|      null|
|43.31801| 18.57149|     301.2|0.32| 0.55|        N|         nominal| 2.0NRT|    279.57|1.17|       N|2022-11-03 00:01:00|      null|      null|
|43.31782| 18.57482|    306.57|0.32| 0.55|        N|         nominal| 2.0NRT|    279.33|1.17|       N|2022-11-03 00:01:00|      null|      null|
|43.30166| 18.58797|    313.97|0.32| 0.55|        N|         nominal| 2.0NRT|    280.88|1.63|       N|2022-11-03 00:01:00|      null|      null|
|41.17582|  32.6341|    298.37|0.44| 0.38|        N|         nominal| 2.0NRT|    279.32| 1.3|       N|2022-11-03 00:01:00|      null|      null|
|41.17237| 32.63293|    299.25|0.44| 0.38|        N|         nominal| 2.0NRT|    276.19|0.99|       N|2022-11-03 00:01:00|      null|      null|
|41.26618|  31.4225|    335.45| 0.4| 0.37|        N|         nominal| 2.0NRT|    285.84|2.74|       N|2022-11-03 00:01:00|      null|      null|
|41.26534| 31.42726|    302.36|0.41| 0.37|        N|         nominal| 2.0NRT|    285.45|2.74|       N|2022-11-03 00:01:00|      null|      null|
|41.26199| 31.42622|    305.55|0.41| 0.37|        N|         nominal| 2.0NRT|    284.15|1.22|       N|2022-11-03 00:01:00|      null|      null|
|41.25698| 31.41461|    297.02| 0.4| 0.37|        N|         nominal| 2.0NRT|    280.49|1.85|       N|2022-11-03 00:01:00|      null|      null|
|41.25364| 31.41356|    331.56| 0.4| 0.37|        N|         nominal| 2.0NRT|    281.93|1.85|       N|2022-11-03 00:01:00|      null|      null|
|42.44389| 20.03925|    313.66|0.54| 0.51|        N|         nominal| 2.0NRT|    280.68|4.15|       N|2022-11-03 00:01:00|      null|      null|
+--------+---------+----------+----+-----+---------+----------------+-------+----------+----+--------+-------------------+----------+----------+
only showing top 20 rows

root
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- bright_ti4: double (nullable = true)
 |-- scan: double (nullable = true)
 |-- track: double (nullable = true)
 |-- satellite: string (nullable = true)
 |-- confidence_level: string (nullable = true)
 |-- version: string (nullable = true)
 |-- bright_ti5: double (nullable = true)
 |-- frp: double (nullable = true)
 |-- daynight: string (nullable = true)
 |-- acq_datetime: string (nullable = true)
 |-- brightness: double (nullable = true)
 |-- bright_t31: double (nullable = true)

2022-11-07 13:25:05.463 - INFO --- [           main] art(Lab17_11ExportWildfiresApp.java:162): # of partitions: 3
 */