package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;

import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Measuring performance without cache, with cache, and with checkpoint.
 *
 * @author jgp
 */
public class Lab16_11CacheCheckpointApp {

    enum Mode {
        NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER
    }

    private SparkSession spark;

    public static void main(String[] args) {
        // Specify the number of records to generate
        int recordCount = 1000;

        Lab16_11CacheCheckpointApp app = new Lab16_11CacheCheckpointApp();
        app.start(recordCount, "local[*]");
    }

    void start(int recordCount, String master) {
        System.out.printf("-> start(%d, %s)\n", recordCount, master);

        // Creates a session on a local master
        this.spark = SparkSession.builder()
                .appName("Example of cache and checkpoint")
                .master(master)
                .config("spark.executor.memory", "70g")
                .config("spark.driver.memory", "50g") // might be too late ;)
                .config("spark.memory.offHeap.enabled", true)
                .config("spark.memory.offHeap.size", "16g")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setCheckpointDir("/tmp");


        System.out.println("\n[1] Create and process the records without cache or checkpoint");
        long t0 = processDataframe(recordCount, Mode.NO_CACHE_NO_CHECKPOINT);

        System.out.println("\n[2] Create and process the records with cache");
        long t1 = processDataframe(recordCount, Mode.CACHE);

        System.out.println("\n[3] Create and process the records with a checkpoint");
        long t2 = processDataframe(recordCount, Mode.CHECKPOINT);

        System.out.println("\n[4] Create and process the records with a checkpoint");
        long t3 = processDataframe(recordCount, Mode.CHECKPOINT_NON_EAGER);
        spark.stop();

        System.out.println("\nProcessing times");
        System.out.println("Without cache ............... " + t0 + " ms");
        System.out.println("With cache .................. " + t1 + " ms");
        System.out.println("With checkpoint ............. " + t2 + " ms");
        System.out.println("With non-eager checkpoint ... " + t3 + " ms");
    }

    private long processDataframe(int recordCount, Mode mode) {
        Dataset<Row> df = Lab16_11RecordGeneratorUtils.createDataframe(this.spark, recordCount);

        long t0 = System.currentTimeMillis();
        Dataset<Row> topDf = df
                .filter(col("rating").equalTo(5));
        switch (mode) {
            case CACHE:
                topDf = topDf.cache();
                break;
            case CHECKPOINT:
                topDf = topDf.checkpoint();
                break;
            case CHECKPOINT_NON_EAGER:
                topDf = topDf.checkpoint(false);
                break;
        }

        List<Row> langDf =
                topDf
                        .groupBy("lang").count()
                        .orderBy("lang")
                        .collectAsList();
        List<Row> yearDf =
                topDf
                        .groupBy("year").count()
                        .orderBy(col("year").desc())
                        .collectAsList();
        long t1 = System.currentTimeMillis();

        System.out.println("Processing took " + (t1 - t0) + " ms.");

        System.out.println("Five-star publications per language");
        for (Row r : langDf) {
            System.out.println(r.getString(0) + " ... " + r.getLong(1));
        }

        System.out.println("\nFive-star publications per year");
        for (Row r : yearDf) {
            System.out.println(r.getInt(0) + " ... " + r.getLong(1));
        }

        return t1 - t0;
    }

}
/*
[1] Create and process the records without cache or checkpoint
-> createDataframe()
1000 records created
+------------+----------------------+------+----+----+
|name        |title                 |rating|year|lang|
+------------+----------------------+------+----+----+
|Sam Kumar   |Their Beach           |3     |2017|fr  |
|Oliver Haque|Their Worse Experience|4     |2010|fr  |
|Rob Tutt    |My Terrible Job       |5     |2005|it  |
+------------+----------------------+------+----+----+
only showing top 3 rows

<- createDataframe()
Processing took 7532 ms.
Five-star publications per language
de ... 43
en ... 47
es ... 44
fr ... 63
it ... 64
pt ... 47

Five-star publications per year
2022 ... 9
2021 ... 9
2020 ... 12
2019 ... 17
2018 ... 4
2017 ... 16
2016 ... 9
2015 ... 12
2014 ... 17
2013 ... 14
2012 ... 11
2011 ... 10
2010 ... 19
2009 ... 19
2008 ... 12
2007 ... 9
2006 ... 6
2005 ... 7
2004 ... 11
2003 ... 13
2002 ... 13
2001 ... 15
2000 ... 14
1999 ... 15
1998 ... 15

[2] Create and process the records with cache
-> createDataframe()
1000 records created
+------------+--------------------+------+----+----+
|name        |title               |rating|year|lang|
+------------+--------------------+------+----+----+
|Kevin Kahn  |The Worse Experience|3     |2013|es  |
|Murthy Haque|The Worse Experience|5     |2010|pt  |
|Noemie Main |Their Beach         |3     |2002|es  |
+------------+--------------------+------+----+----+
only showing top 3 rows

<- createDataframe()
Processing took 3621 ms.
Five-star publications per language
de ... 51
en ... 63
es ... 46
fr ... 68
it ... 66
pt ... 59

Five-star publications per year
2022 ... 10
2021 ... 13
2020 ... 16
2019 ... 16
2018 ... 14
2017 ... 14
2016 ... 15
2015 ... 10
2014 ... 17
2013 ... 8
2012 ... 16
2011 ... 16
2010 ... 11
2009 ... 16
2008 ... 13
2007 ... 9
2006 ... 14
2005 ... 16
2004 ... 18
2003 ... 16
2002 ... 15
2001 ... 12
2000 ... 19
1999 ... 17
1998 ... 12

[3] Create and process the records with a checkpoint
-> createDataframe()
1000 records created
+-------------+-----------------+------+----+----+
|name         |title            |rating|year|lang|
+-------------+-----------------+------+----+----+
|Jean Kumar   |A Gorgeous Life  |5     |2015|pt  |
|Jane Jones   |Your Terrific Job|4     |2012|de  |
|Jonathan Hahn|The Terrible Trip|4     |2003|fr  |
+-------------+-----------------+------+----+----+
only showing top 3 rows

<- createDataframe()
Processing took 3705 ms.
Five-star publications per language
de ... 54
en ... 53
es ... 61
fr ... 54
it ... 56
pt ... 49

Five-star publications per year
2022 ... 9
2021 ... 15
2020 ... 12
2019 ... 10
2018 ... 13
2017 ... 15
2016 ... 15
2015 ... 12
2014 ... 11
2013 ... 13
2012 ... 18
2011 ... 10
2010 ... 14
2009 ... 15
2008 ... 13
2007 ... 14
2006 ... 13
2005 ... 13
2004 ... 12
2003 ... 3
2002 ... 21
2001 ... 15
2000 ... 8
1999 ... 19
1998 ... 14

[4] Create and process the records with a checkpoint
-> createDataframe()
1000 records created
+--------------+---------------------+------+----+----+
|name          |title                |rating|year|lang|
+--------------+---------------------+------+----+----+
|Sam Sanders   |My Gorgeous Beach    |5     |2015|pt  |
|Stephanie Main|The Terrible Life    |3     |2016|fr  |
|Liz Main      |A Terrible Experience|4     |2020|en  |
+--------------+---------------------+------+----+----+
only showing top 3 rows

<- createDataframe()
Processing took 2554 ms.
Five-star publications per language
de ... 60
en ... 52
es ... 53
fr ... 59
it ... 57
pt ... 63

Five-star publications per year
2022 ... 8
2021 ... 16
2020 ... 17
2019 ... 8
2018 ... 15
2017 ... 13
2016 ... 21
2015 ... 15
2014 ... 10
2013 ... 14
2012 ... 12
2011 ... 11
2010 ... 22
2009 ... 18
2008 ... 7
2007 ... 18
2006 ... 13
2005 ... 10
2004 ... 13
2003 ... 8
2002 ... 16
2001 ... 19
2000 ... 15
1999 ... 10
1998 ... 15

Processing times
Without cache ............... 7532 ms
With cache .................. 3621 ms
With checkpoint ............. 3705 ms
With non-eager checkpoint ... 2554 ms
 */