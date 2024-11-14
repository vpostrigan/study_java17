package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author jgp
 */
public class Lab17_31FeedFrancePopDeltaLakeApp {

    public static void main(String[] args) {
        Lab17_31FeedFrancePopDeltaLakeApp app = new Lab17_31FeedFrancePopDeltaLakeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Load France's population dataset and store it in Delta")
                .master("local[*]")
                .getOrCreate();

        // Reads a CSV file, called population_dept.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("data/chapter17/france_population_dept/population_dept.csv");

        df = df
                .withColumn("Code département",
                        when(col("Code département").$eq$eq$eq("2A"), "20")
                                .otherwise(col("Code département")))
                .withColumn("Code département",
                        when(col("Code département").$eq$eq$eq("2B"), "20")
                                .otherwise(col("Code département")))
                .withColumn("Code département",
                        col("Code département").cast(DataTypes.IntegerType))

                .withColumn("Population municipale",
                        regexp_replace(col("Population municipale"), ",", ""))
                .withColumn("Population municipale",
                        col("Population municipale").cast(DataTypes.IntegerType))

                .withColumn("Population totale",
                        regexp_replace(col("Population totale"), ",", ""))
                .withColumn("Population totale",
                        col("Population totale").cast(DataTypes.IntegerType))
                .drop("_c9")

                .withColumnRenamed("Code région", "Code_region")
                .withColumnRenamed("Nom de la région", "Nom_de_la_region")
                .withColumnRenamed("Code département", "Code_departement")
                .withColumnRenamed("Nom du département", "Nom_du_departement")
                .withColumnRenamed("Population totale", "Population_totale")
                .withColumnRenamed("Population municipale", "Population_municipale")
                .withColumnRenamed("Nombre de communes", "Nombre_de_communes")
                .withColumnRenamed("Nombre de cantons", "Nombre_de_cantons")
                .withColumnRenamed("Nombre d'arrondissements", "Nombre_d'arrondissements");
        df.show(25);
        df.printSchema();

        df.write().format("delta")
                .mode("overwrite")
                .save("/tmp/chapter17_delta_france_population");

        System.out.println(df.count() + " rows updated.");
    }

}
/*
+-----------+--------------------+----------------+--------------------+------------------------+-----------------+------------------+---------------------+-----------------+
|Code_region|    Nom_de_la_region|Code_departement|  Nom_du_departement|Nombre_d'arrondissements|Nombre_de_cantons|Nombre_de_communes|Population_municipale|Population_totale|
+-----------+--------------------+----------------+--------------------+------------------------+-----------------+------------------+---------------------+-----------------+
|         84|Auvergne-Rhône-Alpes|               1|                 Ain|                       4|               23|               407|               638425|           655171|
|         32|     Hauts-de-France|               2|               Aisne|                       5|               21|               804|               536136|           549587|
|         84|Auvergne-Rhône-Alpes|               3|              Allier|                       3|               19|               317|               339384|           349336|
|         93|Provence-Alpes-Cô...|               4|Alpes-de-Haute-Pr...|                       4|               15|               198|               162565|           167331|
|         93|Provence-Alpes-Cô...|               5|        Hautes-Alpes|                       2|               15|               163|               141107|           146148|
|         93|Provence-Alpes-Cô...|               6|     Alpes-Maritimes|                       2|               27|               163|              1083704|          1098539|
|         84|Auvergne-Rhône-Alpes|               7|             Ardèche|                       3|               17|               339|               325157|           334591|
|         44|           Grand Est|               8|            Ardennes|                       4|               19|               452|               275371|           283004|
|         76|           Occitanie|               9|              Ariège|                       3|               13|               331|               153067|           158205|
|         44|           Grand Est|              10|                Aube|                       3|               17|               431|               308910|           316639|
|         76|           Occitanie|              11|                Aude|                       3|               19|               436|               368025|           377580|
|         76|           Occitanie|              12|             Aveyron|                       3|               23|               285|               278697|           289481|
|         93|Provence-Alpes-Cô...|              13|    Bouches-du-Rhône|                       4|               29|               119|              2019717|          2047433|
|         28|           Normandie|              14|            Calvados|                       4|               25|               537|               693679|           709715|
|         84|Auvergne-Rhône-Alpes|              15|              Cantal|                       3|               15|               247|               145969|           151615|
|         75|  Nouvelle-Aquitaine|              16|            Charente|                       3|               19|               381|               353288|           365697|
|         75|  Nouvelle-Aquitaine|              17|   Charente-Maritime|                       5|               27|               466|               642191|           660458|
|         24| Centre-Val de Loire|              18|                Cher|                       3|               19|               290|               307110|           315100|
|         75|  Nouvelle-Aquitaine|              19|             Corrèze|                       3|               19|               283|               241535|           249707|
|         94|               Corse|              20|        Corse-du-Sud|                       2|               11|               124|               154303|           156958|
|         94|               Corse|              20|         Haute-Corse|                       3|               15|               236|               176152|           179037|
|         27|Bourgogne-Franche...|              21|           Côte-d'Or|                       3|               23|               704|               533213|           546466|
|         53|            Bretagne|              22|       Côtes-d'Armor|                       4|               27|               355|               598953|           618478|
|         75|  Nouvelle-Aquitaine|              23|              Creuse|                       2|               15|               258|               119502|           123500|
|         75|  Nouvelle-Aquitaine|              24|            Dordogne|                       4|               25|               520|               414789|           426667|
+-----------+--------------------+----------------+--------------------+------------------------+-----------------+------------------+---------------------+-----------------+
only showing top 25 rows

root
 |-- Code_region: integer (nullable = true)
 |-- Nom_de_la_region: string (nullable = true)
 |-- Code_departement: integer (nullable = true)
 |-- Nom_du_departement: string (nullable = true)
 |-- Nombre_d'arrondissements: integer (nullable = true)
 |-- Nombre_de_cantons: integer (nullable = true)
 |-- Nombre_de_communes: integer (nullable = true)
 |-- Population_municipale: integer (nullable = true)
 |-- Population_totale: integer (nullable = true)
 */