package learning_spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * every Spark program and shell session will work as follows:
 * 1. Create some input RDDs from external data.
 * 2. Transform them to define new RDDs using transformations like filter().
 * 3. Ask Spark to persist() any intermediate RDDs that will need to be reused.
 * 4. Launch actions such as count() and first() to kick off a parallel computation,
 * which is then optimized and executed by Spark.
 * <p>
 * [cache() is the same as calling persist() with the default storage level.]
 */
public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .config("spark.master", "local")
                .config("spark.executor.memory", "1g")
                .getOrCreate();

        // Spark provides two ways to create RDDs: loading an external dataset and parallelizing a
        // collection in your driver program.
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        if (false) {
            lines = spark.read().textFile(args[0]).javaRDD();
        } else {
            List<String> data = Arrays.asList("1 1", "22", "3 3 3 ");

            if (true) {
                JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
                lines = sc.parallelize(data);
            } else {
                RDD<String> numRDD = spark.
                        sparkContext()
                        .parallelize(JavaConverters.asScalaIteratorConverter(data.iterator()).asScala()
                                .toSeq(), 2, scala.reflect.ClassTag$.MODULE$.apply(String.class));

                lines = JavaRDD.fromRDD(numRDD, scala.reflect.ClassTag$.MODULE$.apply(String.class));
            }
        }

        // RDDs offer two types of operations: transformations and actions
        // [transformations]:
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        // Spark’s RDDs are by default recomputed each time you run an action on them. If
        // you would like to reuse an RDD in multiple actions, you can ask Spark to persist it
        words.persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        // [actions]:
        if (false) {
            // Save the word count back out to a text file, causing evaluation.
            counts.saveAsTextFile("outputFile.txt");
        } else {
            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2<String, Integer> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }

        System.out.println(words.first());

        spark.stop();
    }

}
