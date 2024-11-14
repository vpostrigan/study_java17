package learning_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkPairRDD {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .appName(SparkTransformations.class.getSimpleName())
                .config("spark.master", "local")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> data1 = Arrays.asList("coffee 1", "coffee", "coffee", "panda", "monkey", "tea");
        JavaRDD<String> lines1 = sc.parallelize(data1);

        JavaPairRDD<String, String> pairs = lines1.mapToPair(s -> new Tuple2<>(s.split(" ")[0], s));
        System.out.println("lines10: " + pairs.collect());

        JavaPairRDD<String, String> pairs0 = pairs.filter(v1 -> v1._2.length() <= 5);
        System.out.println("pairs0: " + pairs0.collect()); // pairs0: [(panda,panda), (tea,tea)]

        // //

        List<Tuple2<Integer, Integer>> data2 = Arrays.asList(new Tuple2(1, 2), new Tuple2(3, 4), new Tuple2(3, 6));
        JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(data2);

        JavaPairRDD<Integer, Integer> rddSecond = sc.parallelizePairs(
                Arrays.asList(new Tuple2(3, 9))
        );

        // //

        basicOp(rdd);

        transformationsOnTwoPairRDDs(rdd, rddSecond);

        partitions(rdd, rddSecond, sc);

        spark.stop();
    }

    private static void basicOp(JavaPairRDD<Integer, Integer> rdd) {
        JavaPairRDD<Integer, Integer> rdd1 = rdd.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println("rdd1: " + rdd1.collect()); // {(1,2),(3,10)}

        JavaPairRDD<Integer, Iterable<Integer>> rdd2 = rdd.groupByKey();
        System.out.println("rdd2: " + rdd2.collect()); // {(1,[2]),(3,[4,6])}

        // (In any case, using one of the specialized aggregation functions in Spark can be much faster
        // than the naive approach of grouping our data and then reducing it.)
        JavaPairRDD<Integer, SparkActions.AvgCount> rdd3 = rdd.combineByKey(a ->
                { // createCombiner (createAcc)
                    SparkActions.AvgCount r = new SparkActions.AvgCount(a);
                    r.num = 1;
                    return r;
                },
                (a, b) -> { // mergeValue (addAndCount)
                    a.total += b;
                    a.num += 1;
                    return a;
                },
                (a, b) -> { // mergeCombiners (combine)
                    a.total += b.total;
                    a.num += a.num;
                    return a;
                });
        // rdd3: 1 = 2.0
        // rdd3: 3 = 5.0
        Map<Integer, SparkActions.AvgCount> countMap = rdd3.collectAsMap();
        for (Map.Entry<Integer, SparkActions.AvgCount> entry : countMap.entrySet()) {
            System.out.println("rdd3: " + entry.getKey() + " = " + entry.getValue().avg());
        }

        JavaPairRDD<Integer, Integer> rdd4 = rdd.mapValues(v1 -> v1 + 1);
        System.out.println("rdd4: " + rdd4.collect()); // [(1,3), (3,5), (3,7)]

        JavaPairRDD<Integer, Integer> rdd5 = rdd.flatMapValues(v1 -> Arrays.asList(v1, v1 + 1).iterator());
        System.out.println("rdd5: " + rdd5.collect()); // [(1,2), (1,3), (3,4), (3,5), (3,6), (3,7)]

        JavaRDD<Integer> rdd6 = rdd.keys();
        System.out.println("rdd6: " + rdd6.collect()); // rdd6: [1, 3, 3]

        JavaRDD<Integer> rdd7 = rdd.values();
        System.out.println("rdd7: " + rdd7.collect()); // rdd7: [2, 4, 6]

        JavaPairRDD<Integer, Integer> rdd8 = rdd.sortByKey(false);
        System.out.println("rdd8: " + rdd8.collect()); // rdd8: [(3,4), (3,6), (1,2)]

        // rdd80: [(1,2), (3,10)]
        JavaPairRDD<Integer, Integer> rdd9 = rdd.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println("rdd9: " + rdd9.collect());

        // rdd91: [(1,(2,1)), (3,(10,2))]
        JavaPairRDD<Integer, Tuple2> rdd91 = rdd.mapValues(v1 -> new Tuple2(v1, 1))
                .reduceByKey((v1, v2) ->
                        new Tuple2((int) v1._1 + (int) v2._1, (int) v1._2 + (int) v2._2));
        System.out.println("rdd91: " + rdd91.collect());

        // rdd92: [((1,2),1), ((3,4),1), ((3,6),1)]
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd92 = rdd.mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((v1, v2) -> v1 + v2);
        System.out.println("rdd92: " + rdd92.collect());
    }

    private static void transformationsOnTwoPairRDDs(JavaPairRDD<Integer, Integer> rdd,
                                                     JavaPairRDD<Integer, Integer> rddSecond) {
        JavaPairRDD<Integer, Integer> rdd10 = rdd.subtractByKey(rddSecond);
        System.out.println("rdd10: " + rdd10.collect()); // rdd10: [(1,2)]

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rdd11 = rdd.join(rddSecond);
        System.out.println("rdd11: " + rdd11.collect()); // rdd11: [(3,(4,9)), (3,(6,9))]

        // Perform a join between two RDDs where the key must be present in the first RDD
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rdd12 = rdd.rightOuterJoin(rddSecond);
        System.out.println("rdd12: " + rdd12.collect()); // rdd12: [(3,(Optional[4],9)), (3,(Optional[6],9))]

        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> rdd13 = rdd.leftOuterJoin(rddSecond);
        System.out.println("rdd13: " + rdd13.collect()); // rdd13: [(1,(2,Optional.empty)), (3,(4,Optional[9])), (3,(6,Optional[9]))]

        // Group data from both RDDs sharing the same key
        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> rdd14 =
                rdd.cogroup(rddSecond);
        System.out.println("rdd14: " + rdd14.collect()); // rdd14: [(1,([2],[])), (3,([4, 6],[9]))]
    }

    private static void partitions(JavaPairRDD<Integer, Integer> rdd,
                                   JavaPairRDD<Integer, Integer> rddSecond,
                                   JavaSparkContext sc) {

        Optional<Partitioner> partitioner = rdd.partitioner();
        System.out.println("rdd partitioner: " + partitioner.orNull());

        JavaPairRDD<Integer, Integer> rdd0 =
                // if rdd is large then join() doesn't know how keys are partitioned in the datasets
                // so it will hash all the keys of both datasets,
                // sending elements with the same key hash across the network
                // to the same machine, and then join together the elements
                // with the same key on that machine
                rdd.partitionBy(new HashPartitioner(100))
                        // Failure to persist an RDD after it has been transformed with partitionBy() will cause
                        // subsequent uses of the RDD to repeat the partitioning of the data.
                        .persist(StorageLevel.MEMORY_AND_DISK_2());
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rdd15 = rdd0.join(rddSecond);
        System.out.println("rdd15: " + rdd15.collect() +
                " \n  rdd15 partitioner: " + rdd15.partitioner().orNull() +
                " \n  rdd0 partitioner: " + rdd0.partitioner().orNull()); // rdd15: [(3,(4,9)), (3,(6,9))]
        // sortByKey() and groupByKey()  will take advantage from 'HashPartitioner'
        // map() cause the new RDD to forget the parent’s partitioning information (no advantage)

        // operations that benefit from partitioning are
        // cogroup(), groupWith(), join(), leftOuterJoin(), rightOuterJoin(),
        // groupByKey(), reduceByKey(), combineByKey(), and lookup()

        // mapValues() and flatMapValues(), which guarantee that each tuple’s key remains the same.
        // and partitioner will be used

        // //

        // Custom Partitioner

        List<String> data1 = Arrays.asList("http://www.cnn.com/WORLD", "http://www.cnn.com/US"
                , "http://www.cnn.com/Canada");
        JavaRDD<String> lines1 = sc.parallelize(data1);
        JavaPairRDD<String, Integer> pairs = lines1.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> rdd16 = pairs.partitionBy(new DomainPartitioner())
                .persist(StorageLevel.MEMORY_AND_DISK_2());
        System.out.println("rdd16: " + rdd16.collect() +
                " \n  rdd16 partitioner: " + rdd16.partitioner().orNull());
    }

    /**
     * All with 'www.cnn.com' will be in single partition
     */
    private static class DomainPartitioner extends Partitioner {
        @Override
        public int numPartitions() {
            return 10;
        }

        @Override
        public int getPartition(Object key) {
            String domain = null;
            try {
                domain = new URL(key.toString()).getHost();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            int code = (domain.hashCode() % numPartitions());
            if (code < 0) {
                return code + numPartitions(); // Make it non-negative
            } else {
                return code;
            }
        }
    }

}
