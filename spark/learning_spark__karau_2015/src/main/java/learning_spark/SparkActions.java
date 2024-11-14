package learning_spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SparkActions {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName(SparkTransformations.class.getSimpleName())
                .config("spark.master", "local")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // //

        List<AvgCount> data1 = Arrays.asList(new AvgCount(105), new AvgCount(110));
        JavaRDD<AvgCount> rdd = sc.parallelize(data1);

        Function2<AvgCount, AvgCount, AvgCount> addAndCount = (a, b) -> {
            a.total += b.total;
            a.num += 1;
            return a;
        };
        Function2<AvgCount, AvgCount, AvgCount> combine = (a, b) -> {
            a.total += b.total;
            a.num += b.num;
            return a;
        };
        AvgCount initial = new AvgCount(0);
        AvgCount result = rdd.aggregate(initial, addAndCount, combine);
        System.out.println(result.avg()); // 107.5

        // //

        List<Integer> data2 = Arrays.asList(1, 2, 3, 3);
        JavaRDD<Integer> rdd2 = sc.parallelize(data2);
        System.out.println("collect: " + rdd2.collect()); // Return all elements from the RDD // {1, 2, 3, 3}

        System.out.println("count: " + rdd2.count()); // Number of elements in the RDD // 4

        // count: {1=1, 3=2, 2=1}
        System.out.println("countByValue: " + rdd2.countByValue()); // Number of times each element occurs in the RDD

        // do not return the elements in the order you might expect
        System.out.println("take: " + rdd2.take(2)); // Return num elements from the RDD

        System.out.println("takeOrdered: " + rdd2.takeOrdered(2)); // Return num elements based on provided ordering.

        System.out.println("top: " + rdd2.top(2)); // Return the top num elements the RDD.

        // reduce: 9
        System.out.println("reduce: " + rdd2.reduce((v1, v2) -> v1 + v2)); // Combine the elements of the RDD together in parallel (e.g., sum)

        System.out.println(rdd2.aggregate(0, (x, y) -> x + y, (x, y) -> x + y).toString());

        rdd2.foreach(integer -> System.out.println(integer));

        spark.stop();
    }

    static class AvgCount implements Serializable {
        public AvgCount(int total) {
            this.total = total;
        }

        public int total;
        public int num;

        public double avg() {
            return total / (double) num;
        }
    }

}
