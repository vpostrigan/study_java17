package spark_in_action2021.part2consume_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark_in_action2021.SchemaInspector;

/**
 * CSV ingestion in a dataframe with a Schema.
 */
public class Lab7_21ComplexCsvToDataframeWithSchemaApp {

    public static final DecimalType$ DecimalType = DecimalType$.MODULE$;

    public static void main(String[] args) {
        Lab7_21ComplexCsvToDataframeWithSchemaApp app =
                new Lab7_21ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local")
                .getOrCreate();

        // Creates the schema
//        Header: id, authorId, title, releaseDate, link
//        Schema: id, authorId, bookTitle, releaseDate, url
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("authordId", DataTypes.IntegerType, true),
                DataTypes.createStructField("bookTitle", DataTypes.StringType, false),
                DataTypes.createStructField("releaseDate", DataTypes.DateType, true), // nullable, but this will be ignore
                DataTypes.createStructField("url", DataTypes.StringType, false)});

        // GitHub version only: dumps the schema
        SchemaInspector.print(schema);
//        {"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"authordId","type":"integer","nullable":true,"metadata":{}},{"name":"bookTitle","type":"string","nullable":false,"metadata":{}},{"name":"releaseDate","type":"date","nullable":true,"metadata":{}},{"name":"url","type":"string","nullable":false,"metadata":{}}]}

        // Reads a CSV file with header, called books.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "M/d/yyyy")
                .option("quote", "*")
                .option("comment", "#")
                .schema(schema)
                .load("data/chapter7/books.csv");

        // GitHub version only: dumps the schema
        SchemaInspector.print("Schema ...... ", schema);
        SchemaInspector.print("Dataframe ... ", df);
//        Schema ...... {"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"authordId","type":"integer","nullable":true,"metadata":{}},{"name":"bookTitle","type":"string","nullable":false,"metadata":{}},{"name":"releaseDate","type":"date","nullable":true,"metadata":{}},{"name":"url","type":"string","nullable":false,"metadata":{}}]}
//        Dataframe ... {"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"authordId","type":"integer","nullable":true,"metadata":{}},{"name":"bookTitle","type":"string","nullable":true,"metadata":{}},{"name":"releaseDate","type":"date","nullable":true,"metadata":{}},{"name":"url","type":"string","nullable":true,"metadata":{}}]}

        // Shows at most 20 rows from the dataframe
        df.show(30, 25, false);
//+---+---------+-------------------------+-----------+----------------------+
//| id|authordId|                bookTitle|releaseDate|                   url|
//+---+---------+-------------------------+-----------+----------------------+
//|  1|        1|Fantastic Beasts and W...| 2016-11-18|http://amzn.to/2kup94P|
//|  2|        1|Harry Potter and the S...| 2015-10-06|http://amzn.to/2l2lSwP|
//|  3|        1|The Tales of Beedle th...| 2008-12-04|http://amzn.to/2kYezqr|
//|  4|        1|Harry Potter and the C...| 2016-10-04|http://amzn.to/2kYhL5n|
//|  5|        2|Informix 12.10 on Mac ...| 2017-04-23|http://amzn.to/2i3mthT|
//|  6|        2|Development Tools in 2...| 2016-12-28|http://amzn.to/2vBxOe1|
//|  7|        3|Adventures of Hucklebe...| 1994-05-26|http://amzn.to/2wOeOav|
//|  8|        3|A Connecticut Yankee i...| 2017-06-17|http://amzn.to/2x1NuoD|
//| 10|        4|     Jacques le Fataliste| 2000-03-01|http://amzn.to/2uZj2KA|
//| 11|        4|Diderot Encyclopedia: ...|       null|http://amzn.to/2i2zo3I|
//| 12|     null|        A Woman in Berlin| 2006-07-11|http://amzn.to/2i472WZ|
//| 13|        6|    Spring Boot in Action| 1916-01-03|http://amzn.to/2hCPktW|
//| 14|        6|Spring in Action: Cove...| 2014-11-28|http://amzn.to/2yJLyCk|
//| 15|        7|Soft Skills: The softw...| 2014-12-29|http://amzn.to/2zNnSyn|
//| 16|        8|          Of Mice and Men|       null|http://amzn.to/2zJjXoc|
//| 17|        9|Java 8 in Action: Lamb...| 2014-08-28|http://amzn.to/2isdqoL|
//| 18|       12|                   Hamlet| 2012-06-08|http://amzn.to/2yRbewY|
//| 19|       13|                  Pensées| 1670-12-31|http://amzn.to/2jweHOG|
//| 20|       14|Fables choisies; mises...| 1999-09-01|http://amzn.to/2yRH10W|
//| 21|       15|Discourse on Method an...| 1999-06-15|http://amzn.to/2hwB8zc|
//| 22|       12|            Twelfth Night| 2004-07-01|http://amzn.to/2zPYnwo|
//| 23|       12|                  Macbeth| 2003-07-01|http://amzn.to/2zPYnwo|
//+---+---------+-------------------------+-----------+----------------------+

        df.printSchema();
//        root
//                |-- id: integer (nullable = true)
//                |-- authordId: integer (nullable = true)
//                |-- bookTitle: string (nullable = true)
//                |-- releaseDate: date (nullable = true)
//                |-- url: string (nullable = true)
    }

}