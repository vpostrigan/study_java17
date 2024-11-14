package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Dropping data using SQL
 *
 * @author jgp
 */
public class Lab11_31DeleteApp {
    private static final transient Logger log = LoggerFactory.getLogger(Lab11_31DeleteApp.class);

    public static void main(String[] args) {
        Lab11_31DeleteApp app = new Lab11_31DeleteApp();
        app.start();
    }

    private void start() {
        log.debug("-> start()");

        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Simple SQL")
                .master("local")
                .getOrCreate();

        // Create the schema for the whole dataset
        List<StructField> list = new ArrayList<>();
        list.add(DataTypes.createStructField("geo", DataTypes.StringType, true));
        for (int i = 1980; i <= 2010; i++) {
            list.add(DataTypes.createStructField("yr" + i, DataTypes.DoubleType, false));
        }
        StructType schema = DataTypes.createStructType(list.toArray((new StructField[0])));

        // Reads a CSV file with header (as specified in the schema), called
        // populationbycountry19802010millions.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");

        // Remove the columns we do not want
        for (int i = 1981; i < 2010; i++) {
            df = df.drop(df.col("yr" + i));
        }

        // Creates a new column with the evolution of the population between 1980 and 2010
        df = df.withColumn("evolution", functions.expr("round((yr2010 - yr1980) * 1000000)"));
        df.createOrReplaceTempView("geodata");

        // remove data with SQL like filter
        // (remove World and continent names)
        log.debug("Territories in original dataset: {}", df.count());
        Dataset<Row> cleanedDf = spark.sql(
                "SELECT * FROM geodata WHERE geo is not null "
                        + "AND geo != 'Africa' "
                        + "AND geo != 'North America' "
                        + "AND geo != 'World' "
                        + "AND geo != 'Asia & Oceania' "
                        + "AND geo != 'Central & South America' "
                        + "AND geo != 'Europe' "
                        + "AND geo != 'Eurasia' "
                        + "AND geo != 'Middle East' "
                        + "ORDER BY yr2010 DESC");
        log.debug("Territories in cleaned dataset: {}", cleanedDf.count());
        cleanedDf.show(20, false);
    }
/**
 2022-07-25 14:13:23.397 -DEBUG --- [           main] leteApp.start(Lab11_31DeleteApp.java:64): Territories in original dataset: 232
 2022-07-25 14:13:23.888 -DEBUG --- [           main] leteApp.start(Lab11_31DeleteApp.java:76): Territories in cleaned dataset: 224
 +----------------+---------+----------+-----------+
 |geo             |yr1980   |yr2010    |evolution  |
 +----------------+---------+----------+-----------+
 |China           |984.73646|1330.14129|3.4540483E8|
 |India           |684.8877 |1173.10802|4.8822032E8|
 |United States   |227.22468|310.23286 |8.300818E7 |
 |Indonesia       |151.0244 |242.96834 |9.194394E7 |
 |Brazil          |123.01963|201.10333 |7.80837E7  |
 |Pakistan        |85.21912 |184.40479 |9.918567E7 |
 |Bangladesh      |87.93733 |156.11846 |6.818113E7 |
 |Nigeria         |74.82127 |152.21734 |7.739607E7 |
 |Russia          |null     |139.39021 |null       |
 |Japan           |116.80731|126.80443 |9997120.0  |
 |Mexico          |68.34748 |112.46886 |4.412138E7 |
 |Philippines     |50.94018 |99.90018  |4.896E7    |
 |Vietnam         |53.7152  |89.57113  |3.585593E7 |
 |Ethiopia        |38.6052  |88.01349  |4.940829E7 |
 |Germany         |null     |82.28299  |null       |
 |Egypt           |42.63422 |80.47187  |3.783765E7 |
 |Turkey          |45.04797 |77.80412  |3.275615E7 |
 |Iran            |39.70873 |76.9233   |3.721457E7 |
 |Congo (Kinshasa)|29.01255 |70.91644  |4.190389E7 |
 |Thailand        |47.02576 |67.0895   |2.006374E7 |
 +----------------+---------+----------+-----------+
 only showing top 20 rows
 */
}
