package learning_spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SparkTransformations {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName(SparkTransformations.class.getSimpleName())
                .config("spark.master", "local")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> data1 = Arrays.asList("coffee 1", "coffee", "coffee", "panda", "monkey", "tea");
        JavaRDD<String> lines1 = sc.parallelize(data1);

        List<String> data2 = Arrays.asList("coffee", "coffee", "monkey", "kitty");
        JavaRDD<String> lines2 = sc.parallelize(data2);

        // //

        JavaRDD<String> lines10 = lines1.sample(false, 0.5);
        System.out.println("lines10: " + lines10.collect());

        // //

        JavaRDD<String> lines11 = lines1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println("lines11: " + lines11.first());

        // //

        JavaRDD<String> lines12 = lines1.distinct(); // expensive (read all data) (has 'shuffle')
        System.out.println("lines12: " + lines12.collect()); // [monkey, coffee, panda, coffee 1, tea]

        // //

        JavaRDD<String> lines13 = lines1.union(lines2); // with duplicates (fast, no 'shuffle')
        System.out.println("lines13: " + lines13.collect()); // [coffee 1, coffee, coffee, panda, monkey, tea, coffee, coffee, monkey, kitty]

        // //

        JavaRDD<String> lines14 = lines1.intersection(lines2); // removes duplicates (has 'shuffle')
        System.out.println("lines14: " + lines14.collect()); // [monkey, coffee]

        // //

        JavaRDD<String> lines15 = lines1.subtract(lines2); // (has 'shuffle')
        System.out.println("lines15: " + lines15.collect()); // [tea, panda, coffee 1]

        // //

        JavaPairRDD<String, String> lines16 = lines1.cartesian(lines2); // very expensive
        System.out.println("lines16: " + lines16.collect()); // [(coffee 1,coffee), (coffee 1,coffee), (coffee 1,monkey), (coffee 1,kitty), (coffee,coffee), (coffee,coffee), (coffee,monkey), (coffee,kitty),


        spark.stop();
    }

}
