package spark_in_action2021.part4;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Appends content of a dataframe to a PostgreSQL database.
 * <p>
 * Check for additional information in the README.md file in the same repository.
 *
 * @author jgp
 */
public class Lab17_41AppendDataJdbcPrimaryKeyApp {

    public static void main(String[] args) {
        Lab17_41AppendDataJdbcPrimaryKeyApp app = new Lab17_41AppendDataJdbcPrimaryKeyApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Addition")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("fname", DataTypes.StringType, false),
                DataTypes.createStructField("lname", DataTypes.StringType, false),
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("score", DataTypes.IntegerType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("Matei", "Zaharia", 34, 456));
        rows.add(RowFactory.create("Jean-Georges", "Perrin", 23, 3));
        rows.add(RowFactory.create("Jacek", "Laskowski", 12, 758));
        rows.add(RowFactory.create("Holden", "Karau", 31, 369));

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show(false);

        // Write in a table called ch17_lab900_pkey
        df.write().format("jdbc")
                .mode(SaveMode.Append)
                .option("url", "jdbc:postgresql://localhost:5432/postgres")
                .option("dbtable", "ch17_lab900_pkey")
                .option("driver", "org.postgresql.Driver")
                .option("user", "postgres")
                .option("password", "somePassword")
                .save();
    }

}
/*
+------------+---------+---+-----+
|fname       |lname    |id |score|
+------------+---------+---+-----+
|Matei       |Zaharia  |34 |456  |
|Jean-Georges|Perrin   |23 |3    |
|Jacek       |Laskowski|12 |758  |
|Holden      |Karau    |31 |369  |
+------------+---------+---+-----+
 */