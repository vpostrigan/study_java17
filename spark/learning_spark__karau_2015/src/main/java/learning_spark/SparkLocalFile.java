package learning_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkLocalFile {

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName(SparkLocalFile.class.getSimpleName())
                .config("spark.master", "local")
                .getOrCreate();

        simpleFile(spark);

        simpleFile_Json(spark);

        spark.stop();
    }

    private static void simpleFile(SparkSession spark) {

        long t = System.currentTimeMillis();
        JavaRDD<String> textFile = spark.read().textFile("D:\\INSTALL2\\SoapUI-x64-5.6.0.exe").toJavaRDD();

        textFile.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println(textFile.count());
        System.out.println(textFile.first());

        JavaRDD<String> textFile2 = textFile.filter(s -> s.contains("8"));

        System.out.println("textFile2.count(): " + textFile2.count());
        System.out.println(textFile2.first());
        System.out.println(textFile2.count());

        System.out.println(textFile.count());
        System.out.println(textFile.first());

        System.out.println((System.currentTimeMillis() - t));

        // [2] union
        JavaRDD<String> textFile3 = textFile.filter(s -> s.contains("9"));
        System.out.println("textFile3.count(): " + textFile3.count());

        JavaRDD<String> textFile23 = textFile2.union(textFile3);

        System.out.println("textFile23.count(): " + textFile23.count());
        System.out.println("textFile23 examples: ");
        textFile23.take(10).forEach(t0 -> System.out.println(t0));
    }

    private static void simpleFile_Json(SparkSession spark) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        final String fileName = SparkLocalFile.class.getSimpleName() + "_test.json";
        if (new File(fileName).isDirectory()) {
            for (File f : new File(fileName).listFiles()) {
                f.delete();
            }
        }
        new File(fileName).delete();

        Person p1 = new Person("fname1", "lname1");
        Person p2 = new Person("fname2", "lname2");
        Person p3 = new Person("fname3", "lname3");
        Person p4 = new Person("fname4", "lname4");
        Person p5 = new Person("fname5", "lname5");

        List<Person> data1 = Arrays.asList(p1, p2, p3, p4, p5);
        JavaRDD<Person> lines1 = sc.parallelize(data1);

        JavaRDD<String> formatted = lines1.mapPartitions(people -> {
            ObjectMapper mapper = new ObjectMapper();

            ArrayList<String> text = new ArrayList<>();
            while (people.hasNext()) {
                Person person = people.next();
                text.add(mapper.writeValueAsString(person));
            }
            return text.iterator();
        }).filter(v1 -> true);
        formatted.saveAsTextFile(fileName);

        // //

        JavaRDD<String> input = sc.textFile(fileName);
        JavaRDD<Person> result = input.mapPartitions(lines -> {
            ObjectMapper mapper = new ObjectMapper();

            List<Person> people = new ArrayList<>();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    people.add(mapper.readValue(line, Person.class));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return people.iterator();
        });

        result.foreach(x -> System.out.println(x));

        // //

        if (new File(fileName).isDirectory()) {
            for (File f : new File(fileName).listFiles()) {
                f.delete();
            }
        }
        new File(fileName).delete();

    }

    private static class Person implements Serializable {
        String fname;
        String lname;

        public Person() {
        }

        public Person(String fname, String lname) {
            this.fname = fname;
            this.lname = lname;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "fname='" + fname + '\'' +
                    ", lname='" + lname + '\'' +
                    '}';
        }

        public String getFname() {
            return fname;
        }

        public void setFname(String fname) {
            this.fname = fname;
        }

        public String getLname() {
            return lname;
        }

        public void setLname(String lname) {
            this.lname = lname;
        }
    }

}
