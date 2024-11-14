package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Connects Spark and Elasticsearch, ingests data from Elasticsearch in Spark.
 * <p>
 * Input data
 * https://github.com/elastic/examples/tree/master/Exploring%20Public%20Datasets/nyc_restaurants
 * http://download.elasticsearch.org/demos/nyc_restaurants/nyc_restaurants-5-4-3.tar.gz
 * <p>
 * install ES and start (D:\program_files\es_elasticsearch-6.8.23\bin)
 * $ curl -H "Content-Type: application/json" -XPUT 'http://localhost:9200/_snapshot/restaurants_backup' -d '{    "type": "fs",    "settings": {        "location": "D:/program_files/es_elasticsearch-6.8.23/nyc_restaurants/",        "compress": true,        "max_snapshot_bytes_per_sec": "1000mb",        "max_restore_bytes_per_sec": "1000mb"    }}'
 * $ curl -XPOST "localhost:9200/_snapshot/restaurants_backup/snapshot_1/_restore"
 * $ curl -H "Content-Type: application/json" -XGET localhost:9200/nyc_restaurants/_count -d '{	"query": {		"match_all": {}        }}'
 * <p>
 * состояние сервера:
 * $ curl -H "Content-Type: application/json" http://localhost:9200
 * <p>
 * вывод структуры индекса:
 * $ curl -H "Content-Type: application/json" http://localhost:9200/nyc_restaurants | python -m json.tool
 * <p>
 * <p>
 * Концепция в реляционной БД | Elasticsearch
 * База данных (Database)     | Индекс (Index)
 * Раздел (Partition)         | Сегмент (Shard)
 * Таблица (Table)            | Тип (Type)
 * Строка (Row)               | Документ (Document)
 * Столбец (Column)           | Поле (Field)
 * Схема (Schema)             | Отображение (Mapping)
 * SQL                        | Query DSL
 * Представление (View)       | Отфильтрованный псевдоним (Filtered alias)
 * Триггер (Trigger)          | Watch API (через X-Pack)
 * <p>
 * index; constraint;         | нету
 * primary keys; foreign keys | нету
 *
 * @author jgp
 */
public class Lab8_51ElasticsearchToDatasetApp {

    /**
     * "hits": { -
     * "total": 473039,
     * "max_score": 1,
     * "hits": [ -
     * { -
     * "_index": "nyc_restaurants",
     * "_type": "inspection",
     * "_id": "113450",
     * "_score": 1,
     * "_source": { -
     * "Dba": "SHUN FOO CHINESE RESTAURANT",
     * "Inspection_Type": "Cycle Inspection / Re-inspection",
     * "Inspection_Date": [ -
     * "2014-08-07T00:00:00"
     * ],
     * "Action": "Violations were cited in the following area(s).",
     * "Violation_Code": "08A",
     * "Score": null,
     * "Building": "13507",
     * "Grade_Date": "2014-08-07T00:00:00",
     * "Critical_Flag": "Not Critical",
     * "Camis": 41030732,
     * "Zipcode": 11420,
     * "Violation_Description": "Facility not vermin proof. Harborage or conditions conducive to attracting vermin to the premises and/or allowing vermin to exist.",
     * "Phone": "7188437074",
     * "Cuisine_Description": "Chinese",
     * "Grade": "",
     * "Street": "LEFFERTS BOULEVARD",
     * "Coord": [ -
     * -73.8206948,
     * 40.670218
     * ],
     * "Record_Date": "2016-03-21T00:00:00",
     * "Address": "13507 LEFFERTS BOULEVARD QUEENS,NY",
     * "Boro": "QUEENS"
     * }
     * },
     */

    public static void main(String[] args) {
        var app = new Lab8_51ElasticsearchToDatasetApp();
        app.start();
    }

    private void start() {
        long t0 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Elasticsearch to Dataframe")
                .master("local")
                .getOrCreate();

        long t1 = System.currentTimeMillis();
        System.out.println("Getting a session took: " + (t1 - t0) + " ms");

        Dataset<Row> df = spark.read().format("org.elasticsearch.spark.sql")
                .option("es.nodes", "localhost")
                .option("es.port", "9200")
                .option("es.query", "?q=*")
                .option("es.read.field.as.array.include", "Inspection_Date")
                .load("nyc_restaurants");
        // 22/04/27 17:06:11 WARN ScalaRowValueReader: Field 'Inspection_Date' is backed by an array but the associated Spark Schema does not reflect this;
        // (use es.read.field.as.array.include/exclude)

        long t2 = System.currentTimeMillis();
        System.out.println("Init communication and starting to get some results took: "
                + (t2 - t1) + " ms");

        // Shows only a few records as they are pretty long
        df.show(10);

        long t3 = System.currentTimeMillis();
        System.out.println("Showing a few records took: " + (t3 - t2) + " ms");

        df.printSchema();
        long t4 = System.currentTimeMillis();
        System.out.println("Displaying the schema took: " + (t4 - t3) + " ms");

        System.out.println("The dataframe contains " + df.count() + " record(s).");
        long t5 = System.currentTimeMillis();
        System.out.println("Counting the number of records took: " + (t5 - t4) + " ms");

        System.out.println("The dataframe is split over " +
                df.rdd().getPartitions().length + " partition(s).");
        long t6 = System.currentTimeMillis();
        System.out.println("Counting the # of partitions took: " + (t6 - t5) + " ms");
    }
/**

 Getting a session took: 5688 ms
 Init communication and starting to get some results took: 3604 ms
 +--------------------+--------------------+---------+----------+--------+--------------------+-------------+-------------------+--------+-----+-------------------+--------------------+--------------------+----------+-------------------+-----+--------------------+--------------+---------------------+-------+
 |              Action|             Address|     Boro|  Building|   Camis|               Coord|Critical_Flag|Cuisine_Description|     Dba|Grade|         Grade_Date|     Inspection_Date|     Inspection_Type|     Phone|        Record_Date|Score|              Street|Violation_Code|Violation_Description|Zipcode|
 +--------------------+--------------------+---------+----------+--------+--------------------+-------------+-------------------+--------+-----+-------------------+--------------------+--------------------+----------+-------------------+-----+--------------------+--------------+---------------------+-------+
 |Violations were c...|34 COOPER SQUARE ...|MANHATTAN|34        |41234821|[-73.9914601, 40....|     Critical|           Japanese|GYU-KAKU| null|               null|[2014-09-02 00:00...|Cycle Inspection ...|2124752989|2016-03-21 00:00:00| null|COOPER SQUARE    ...|           06F| Wiping cloths soi...|  10003|
 |Violations were c...|34 COOPER SQUARE ...|MANHATTAN|34        |41234821|[-73.9914601, 40....| Not Critical|           Japanese|GYU-KAKU| null|               null|[2014-09-02 00:00...|Cycle Inspection ...|2124752989|2016-03-21 00:00:00| null|COOPER SQUARE    ...|           10F| Non-food contact ...|  10003|
 |Violations were c...|34 COOPER SQUARE ...|MANHATTAN|34        |41234821|[-73.9914601, 40....|     Critical|           Japanese|GYU-KAKU|    B|2014-01-15 00:00:00|[2014-01-15 00:00...|Cycle Inspection ...|2124752989|2016-03-21 00:00:00| 27.0|COOPER SQUARE    ...|           02B| Hot food item not...|  10003|
 |Violations were c...|34 COOPER SQUARE ...|MANHATTAN|34        |41234821|[-73.9914601, 40....|     Critical|           Japanese|GYU-KAKU| null|               null|[2013-11-18 00:00...|Cycle Inspection ...|2124752989|2016-03-21 00:00:00| null|COOPER SQUARE    ...|           02G| Cold food item he...|  10003|
 |Violations were c...|34 COOPER SQUARE ...|MANHATTAN|34        |41234821|[-73.9914601, 40....| Not Critical|           Japanese|GYU-KAKU| null|               null|[2013-06-04 00:00...|Administrative Mi...|2124752989|2016-03-21 00:00:00| null|COOPER SQUARE    ...|           22C| Bulb not shielded...|  10003|
 |Violations were c...|505 MYRTLE AVENUE...| BROOKLYN|       505|41234910|[-73.9646906, 40....| Not Critical|          American |  PILLOW| null|2016-01-12 00:00:00|[2016-01-12 00:00...|Cycle Inspection ...|7182462711|2016-03-21 00:00:00| null|       MYRTLE AVENUE|           10F| Non-food contact ...|  11205|
 |Violations were c...|505 MYRTLE AVENUE...| BROOKLYN|       505|41234910|[-73.9646906, 40....|     Critical|          American |  PILLOW|    A|2015-08-05 00:00:00|[2015-08-05 00:00...|Cycle Inspection ...|7182462711|2016-03-21 00:00:00| 12.0|       MYRTLE AVENUE|           04N| Filth flies or fo...|  11205|
 |Violations were c...|505 MYRTLE AVENUE...| BROOKLYN|       505|41234910|[-73.9646906, 40....| Not Critical|          American |  PILLOW| null|2015-08-05 00:00:00|[2015-08-05 00:00...|Cycle Inspection ...|7182462711|2016-03-21 00:00:00| null|       MYRTLE AVENUE|           10F| Non-food contact ...|  11205|
 |Violations were c...|505 MYRTLE AVENUE...| BROOKLYN|       505|41234910|[-73.9646906, 40....|     Critical|          American |  PILLOW| null|               null|[2014-06-07 00:00...|Cycle Inspection ...|7182462711|2016-03-21 00:00:00| 24.0|       MYRTLE AVENUE|           02G| Cold food item he...|  11205|
 |Violations were c...|505 MYRTLE AVENUE...| BROOKLYN|       505|41234910|[-73.9646906, 40....| Not Critical|          American |  PILLOW| null|2013-05-14 00:00:00|[2013-05-14 00:00...|Cycle Inspection ...|7182462711|2016-03-21 00:00:00| null|       MYRTLE AVENUE|           08A| Facility not verm...|  11205|
 +--------------------+--------------------+---------+----------+--------+--------------------+-------------+-------------------+--------+-----+-------------------+--------------------+--------------------+----------+-------------------+-----+--------------------+--------------+---------------------+-------+
 only showing top 10 rows

 Showing a few records took: 5257 ms
 root
 |-- Action: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- Boro: string (nullable = true)
 |-- Building: string (nullable = true)
 |-- Camis: long (nullable = true)
 |-- Coord: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- Critical_Flag: string (nullable = true)
 |-- Cuisine_Description: string (nullable = true)
 |-- Dba: string (nullable = true)
 |-- Grade: string (nullable = true)
 |-- Grade_Date: timestamp (nullable = true)
 |-- Inspection_Date: array (nullable = true)
 |    |-- element: timestamp (containsNull = true)
 |-- Inspection_Type: string (nullable = true)
 |-- Phone: string (nullable = true)
 |-- Record_Date: timestamp (nullable = true)
 |-- Score: double (nullable = true)
 |-- Street: string (nullable = true)
 |-- Violation_Code: string (nullable = true)
 |-- Violation_Description: string (nullable = true)
 |-- Zipcode: long (nullable = true)

 Displaying the schema took: 2 ms

 The dataframe contains 473039 record(s).
 Counting the number of records took: 53763 ms

 The dataframe is split over 5 partition(s).
 Counting the # of partitions took: 464 ms
 */
}
