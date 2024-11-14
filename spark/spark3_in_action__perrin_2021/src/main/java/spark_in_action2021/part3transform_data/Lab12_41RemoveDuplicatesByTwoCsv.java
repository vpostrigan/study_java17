package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark_in_action2021.Logs;

public class Lab12_41RemoveDuplicatesByTwoCsv {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab12_41RemoveDuplicatesByTwoCsv app = new Lab12_41RemoveDuplicatesByTwoCsv();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        SparkSession spark = SparkSession.builder()
                .appName("All joins!")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("identifier_id", DataTypes.StringType, false),
                DataTypes.createStructField("enum_id1", DataTypes.StringType, false),
                DataTypes.createStructField("enum_id2", DataTypes.StringType, false),
                DataTypes.createStructField("context", DataTypes.StringType, false),
                DataTypes.createStructField("identifier_name", DataTypes.StringType, false)}
        );

        Dataset<Row> df1 = spark.read().format("csv")
                .option("header", "false")
                .schema(schema)
                .load("D:\\workspace_selerity_cascade_git\\cascade_RefDataProcessor\\remove_202209.txt");

        Dataset<Row> df2 = spark.read().format("csv")
                .option("header", "false")
                .schema(schema)
                .load("D:\\workspace_selerity_cascade_git\\cascade_RefDataProcessor\\remove_202210.txt");

        Dataset<Row> df = df2.join(df1,
                df2.col("identifier_id").equalTo(df1.col("identifier_id")),
                "left_anti");
        allLogs.showAndSaveToCsv("df", df, 5, false, false);

        df.write()
                .format("csv")
                .save("D:\\workspace_selerity_cascade_git\\cascade_RefDataProcessor\\remove_202210_2.txt");
    }

}
