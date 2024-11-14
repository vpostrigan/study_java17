package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Ingestion the 'Grand Débat' files to Delta Lake.
 *
 * @author jgp
 */
public class Lab17_21FeedDeltaLakeApp {

    static final String OUTFILE = "/tmp/chapter17_delta_grand_debat_events";

    public static void main(String[] args) {
        Lab17_21FeedDeltaLakeApp app = new Lab17_21FeedDeltaLakeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Ingestion the 'Grand Débat' files to Delta Lake")
                .master("local[*]")
                .getOrCreate();

        // Create the schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("authorId", DataTypes.StringType, false),
                DataTypes.createStructField("authorType", DataTypes.StringType, true),
                DataTypes.createStructField("authorZipCode", DataTypes.StringType, true),
                DataTypes.createStructField("body", DataTypes.StringType, true),
                DataTypes.createStructField("createdAt", DataTypes.TimestampType, false),
                DataTypes.createStructField("enabled", DataTypes.BooleanType, true),
                DataTypes.createStructField("endAt", DataTypes.TimestampType, true),
                DataTypes.createStructField("fullAddress", DataTypes.StringType, true),
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("lat", DataTypes.DoubleType, true),
                DataTypes.createStructField("link", DataTypes.StringType, true),
                DataTypes.createStructField("lng", DataTypes.DoubleType, true),
                DataTypes.createStructField("startAt", DataTypes.TimestampType, false),
                DataTypes.createStructField("title", DataTypes.StringType, true),
                DataTypes.createStructField("updatedAt", DataTypes.TimestampType, true),
                DataTypes.createStructField("url", DataTypes.StringType, true)});

        // Reads a JSON file, called 20190302 EVENTS.json, stores it in a dataframe
        Dataset<Row> df = spark.read().format("json")
                .schema(schema)
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .load("data/chapter17/france_grand_debat/20190302 EVENTS.json.gz");
        df.show(5);

        df = df
                .withColumn("authorZipCode",
                        col("authorZipCode").cast(DataTypes.IntegerType))
                .withColumn("authorZipCode",
                        when(col("authorZipCode").$less(1000), null)
                                .otherwise(col("authorZipCode")))
                .withColumn("authorZipCode",
                        when(col("authorZipCode").$greater$eq(99999), null)
                                .otherwise(col("authorZipCode")))
                .withColumn("authorDept",
                        expr("int(authorZipCode / 1000)"));
        df.show(15);
        df.printSchema();

        df.write().format("delta")
                .mode("overwrite")
                .save(OUTFILE);

        System.out.println(df.count() + " rows updated.");
    }

}
/*
+--------------------+--------------------+-------------+--------------------+-------------------+-------+-------------------+--------------------+--------------------+----------+----+----------+-------------------+--------------------+-------------------+--------------------+
|            authorId|          authorType|authorZipCode|                body|          createdAt|enabled|              endAt|         fullAddress|                  id|       lat|link|       lng|            startAt|               title|          updatedAt|                 url|
+--------------------+--------------------+-------------+--------------------+-------------------+-------+-------------------+--------------------+--------------------+----------+----+----------+-------------------+--------------------+-------------------+--------------------+
|VXNlcjplYWE1OTUzM...| Citoyen / Citoyenne|        25800|<p><em>L’exactitu...|2019-02-01 17:12:36|   true|2019-02-06 21:00:00|Salle fernier de ...|RXZlbnQ6MzBiOGEwO...|47.1483483|null| 6.3454497|2019-02-06 19:00:00| Grand débat citoyen|2019-02-01 17:12:35|https://granddeba...|
|VXNlcjowODM3NGZjN...|Élu / élue et Ins...|        64100|<p><em>L’exactitu...|2019-02-03 13:56:58|   true|2019-03-11 20:30:00|Centre  Sainte-Ur...|RXZlbnQ6MzBiZWNmN...|43.4962456|null|-1.4716916|2019-03-11 18:30:00|Fiscalité et dépe...|2019-02-03 13:56:57|https://granddeba...|
|VXNlcjo2MjExNTU1Y...|Élu / élue et Ins...|        61300|<p><em>L’exactitu...|2019-02-01 15:46:42|   true|2019-02-22 21:00:00|Salle des Fêtes A...|RXZlbnQ6MzBjMTMwN...|48.9275357|null| 0.1990102|2019-02-22 19:00:00|réunion d'échange...|2019-02-01 15:46:41|https://granddeba...|
|VXNlcjpmNGViMWIwY...| Citoyen / Citoyenne|        33127|“<i>L’exactitude ...|2019-02-09 13:57:00|   true|2019-02-20 22:00:00|Salle C. Monet, 2...|RXZlbnQ6MzBjMTcwY...|44.8403818|null|-0.7759731|2019-02-20 19:00:00|Transition énergé...|2019-02-09 13:57:00|https://granddeba...|
|VXNlcjo5YjZiYmQ2Z...|                null|        89290|“<i>L’exactitude ...|2019-02-20 12:18:04|   true|2019-02-26 21:00:00|salle des joinche...|RXZlbnQ6MzBjNzRiN...|47.8137129|null| 3.6293265|2019-02-26 19:00:00|Grand débat national|2019-02-20 12:18:04|https://granddeba...|
+--------------------+--------------------+-------------+--------------------+-------------------+-------+-------------------+--------------------+--------------------+----------+----+----------+-------------------+--------------------+-------------------+--------------------+
only showing top 5 rows

ANTLR Tool version 4.8 used for code generation does not match the current runtime version 4.7ANTLR Runtime version 4.8 used for parser compilation does not match the current runtime version 4.7ANTLR Tool version 4.8 used for code generation does not match the current runtime version 4.7ANTLR Runtime version 4.8 used for parser compilation does not match the current runtime version 4.7+--------------------+--------------------+-------------+--------------------+-------------------+-------+-------------------+--------------------+--------------------+----------+--------------------+----------+-------------------+--------------------+-------------------+--------------------+----------+
|            authorId|          authorType|authorZipCode|                body|          createdAt|enabled|              endAt|         fullAddress|                  id|       lat|                link|       lng|            startAt|               title|          updatedAt|                 url|authorDept|
+--------------------+--------------------+-------------+--------------------+-------------------+-------+-------------------+--------------------+--------------------+----------+--------------------+----------+-------------------+--------------------+-------------------+--------------------+----------+
|VXNlcjplYWE1OTUzM...| Citoyen / Citoyenne|        25800|<p><em>L’exactitu...|2019-02-01 17:12:36|   true|2019-02-06 21:00:00|Salle fernier de ...|RXZlbnQ6MzBiOGEwO...|47.1483483|                null| 6.3454497|2019-02-06 19:00:00| Grand débat citoyen|2019-02-01 17:12:35|https://granddeba...|        25|
|VXNlcjowODM3NGZjN...|Élu / élue et Ins...|        64100|<p><em>L’exactitu...|2019-02-03 13:56:58|   true|2019-03-11 20:30:00|Centre  Sainte-Ur...|RXZlbnQ6MzBiZWNmN...|43.4962456|                null|-1.4716916|2019-03-11 18:30:00|Fiscalité et dépe...|2019-02-03 13:56:57|https://granddeba...|        64|
|VXNlcjo2MjExNTU1Y...|Élu / élue et Ins...|        61300|<p><em>L’exactitu...|2019-02-01 15:46:42|   true|2019-02-22 21:00:00|Salle des Fêtes A...|RXZlbnQ6MzBjMTMwN...|48.9275357|                null| 0.1990102|2019-02-22 19:00:00|réunion d'échange...|2019-02-01 15:46:41|https://granddeba...|        61|
|VXNlcjpmNGViMWIwY...| Citoyen / Citoyenne|        33127|“<i>L’exactitude ...|2019-02-09 13:57:00|   true|2019-02-20 22:00:00|Salle C. Monet, 2...|RXZlbnQ6MzBjMTcwY...|44.8403818|                null|-0.7759731|2019-02-20 19:00:00|Transition énergé...|2019-02-09 13:57:00|https://granddeba...|        33|
|VXNlcjo5YjZiYmQ2Z...|                null|        89290|“<i>L’exactitude ...|2019-02-20 12:18:04|   true|2019-02-26 21:00:00|salle des joinche...|RXZlbnQ6MzBjNzRiN...|47.8137129|                null| 3.6293265|2019-02-26 19:00:00|Grand débat national|2019-02-20 12:18:04|https://granddeba...|        89|
|VXNlcjo2MjExNTU1Y...|Élu / élue et Ins...|        61300|<p><em>L’exactitu...|2019-02-01 15:18:05|   true|2019-02-18 21:00:00|Salle Michaux Ave...|RXZlbnQ6MzBkMGUzY...| 48.763015|                null| 0.6155253|2019-02-18 19:00:00|Réunion pour un m...|2019-02-01 15:18:03|https://granddeba...|        61|
|VXNlcjo4YzU5MGE1Y...| Citoyen / Citoyenne|        42000|“<i>L’exactitude ...|2019-02-20 12:18:04|   true|2019-03-05 20:00:00|EHPAD LA SARRAZIN...|RXZlbnQ6MzBkNTYxO...|45.4596811|                null| 4.4061507|2019-03-05 18:00:00|EHPAD.... mais pa...|2019-02-20 12:18:04|https://granddeba...|        42|
|VXNlcjozYzczODQwM...|Élu / élue et Ins...|        92120|<p><em>L’exactitu...|2019-01-30 18:26:34|   true|2019-02-16 12:00:00|Beffroi - place E...|RXZlbnQ6MzBkNmY5O...|48.8192054|http://www.ville-...| 2.3196415|2019-02-16 10:00:00|Débat national : ...|2019-01-30 18:26:33|https://granddeba...|        92|
|VXNlcjpiNGZlYjM4O...|Élu / élue et Ins...|         6380|<p><em>L’exactitu...|2019-01-19 16:37:55|   true|2019-01-28 10:30:00|8, avenue Jean Mé...|RXZlbnQ6MzBkNzY2Y...|43.8781089|                null| 7.4461031|2019-01-28 09:30:00|          Café-Débat|2019-01-19 16:37:55|https://granddeba...|         6|
|VXNlcjowNWM5NjYyN...|                null|        83400|“<i>L’exactitude ...|2019-02-20 12:18:04|   true|2019-03-02 11:30:00|Forum du Casino, ...|RXZlbnQ6MzBlMmNhY...|43.1181735|                null| 6.1317726|2019-03-02 09:30:00|Urgence sociale e...|2019-02-20 12:18:04|https://granddeba...|        83|
|VXNlcjo3ODJjYzg0Z...|Élu / élue et Ins...|        78270|<p><em>L’exactitu...|2019-01-25 14:14:48|   true|2019-02-04 21:30:00|Salle des Fêtes -...|RXZlbnQ6MzBlZDBjM...|49.0405384|                null| 1.5679587|2019-02-04 19:00:00|        Débat public|2019-01-25 14:14:47|https://granddeba...|        78|
|VXNlcjoxMDUwZGNhZ...|Élu / élue et Ins...|        44770|“<i>L’exactitude ...|2019-02-05 10:48:29|   true|2019-02-09 12:00:00|Espace culturel, ...|RXZlbnQ6MzBlZjNmN...|47.1301779|                null|-2.2204287|2019-02-09 10:00:00|Grand Débat National|2019-02-05 10:48:28|https://granddeba...|        44|
|VXNlcjo2NjBkZjBkN...| Citoyen / Citoyenne|        33170|“<i>L’exactitude ...|2019-02-20 12:18:04|   true|2019-02-23 20:00:00|     33170 Gradignan|RXZlbnQ6MzBmMWJlM...|44.7693026|                null|-0.6156794|2019-02-23 15:00:00|Le grand débat na...|2019-02-20 12:18:04|https://granddeba...|        33|
|VXNlcjoxYjYzODVhM...|Élu / élue et Ins...|        91730|<p><em>L’exactitu...|2019-01-26 15:18:02|   true|2019-02-16 12:30:00|salle polyvalente...|RXZlbnQ6MzBmODljY...|48.4544446|                null| 2.2606563|2019-02-16 09:30:00|GRAND DEBAT NATIONAL|2019-01-26 15:18:01|https://granddeba...|        91|
|VXNlcjoyM2ViOTVhM...|Organisation à bu...|         6100|“<i>L’exactitude ...|2019-02-20 12:18:04|   true|2019-03-12 17:00:00|Délégation APF Fr...|RXZlbnQ6MzBmZjk2Z...|43.1347105|https://docs.goog...| 6.0207527|2019-03-12 14:00:00|Handicap : à quan...|2019-02-20 12:18:04|https://granddeba...|         6|
+--------------------+--------------------+-------------+--------------------+-------------------+-------+-------------------+--------------------+--------------------+----------+--------------------+----------+-------------------+--------------------+-------------------+--------------------+----------+
only showing top 15 rows

root
 |-- authorId: string (nullable = true)
 |-- authorType: string (nullable = true)
 |-- authorZipCode: integer (nullable = true)
 |-- body: string (nullable = true)
 |-- createdAt: timestamp (nullable = true)
 |-- enabled: boolean (nullable = true)
 |-- endAt: timestamp (nullable = true)
 |-- fullAddress: string (nullable = true)
 |-- id: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- link: string (nullable = true)
 |-- lng: double (nullable = true)
 |-- startAt: timestamp (nullable = true)
 |-- title: string (nullable = true)
 |-- updatedAt: timestamp (nullable = true)
 |-- url: string (nullable = true)
 |-- authorDept: integer (nullable = true)

9501 rows updated.
 */