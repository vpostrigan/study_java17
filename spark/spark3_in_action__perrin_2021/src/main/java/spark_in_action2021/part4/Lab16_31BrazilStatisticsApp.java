package spark_in_action2021.part4;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analytics on Brazil's economy.
 *
 * @author jgp
 */
public class Lab16_31BrazilStatisticsApp {
    private static Logger log = LoggerFactory.getLogger(Lab16_31BrazilStatisticsApp.class);

    enum Mode {
        NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER
    }

    public static void main(String[] args) {
        Lab16_31BrazilStatisticsApp app = new Lab16_31BrazilStatisticsApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Brazil economy")
                .master("local[*]")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setCheckpointDir("/tmp");

        // Reads a CSV file with header, called BRAZIL_CITIES.csv, stores it in
        // a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("sep", ";")
                .option("enforceSchema", true)
                .option("nullValue", "null")
                .option("inferSchema", true)
                .load("data/chapter16/brazil/BRAZIL_CITIES.csv.gz");
        System.out.println("***** Raw dataset and schema");
        df.show(15);
        df.printSchema();

        // Create and process the records without cache or checkpoint
        long t0 = process(df, Mode.NO_CACHE_NO_CHECKPOINT);

        // Create and process the records with cache
        long t1 = process(df, Mode.CACHE);

        // Create and process the records with a checkpoint
        long t2 = process(df, Mode.CHECKPOINT);

        // Create and process the records with a checkpoint
        long t3 = process(df, Mode.CHECKPOINT_NON_EAGER);

        System.out.println("\n***** Processing times (excluding purification)");
        System.out.println("Without cache ............... " + t0 + " ms");
        System.out.println("With cache .................. " + t1 + " ms");
        System.out.println("With checkpoint ............. " + t2 + " ms");
        System.out.println("With non-eager checkpoint ... " + t3 + " ms");
    }

    long process(Dataset<Row> df, Mode mode) {
        long t0 = System.currentTimeMillis();

        df = df
                .orderBy(col("CAPITAL").desc())
                .withColumn("WAL-MART", when(col("WAL-MART").isNull(), 0).otherwise(col("WAL-MART")))
                .withColumn("MAC", when(col("MAC").isNull(), 0).otherwise(col("MAC")))
                .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
                .withColumn("GDP", col("GDP").cast("float"))
                .withColumn("area", regexp_replace(col("area"), ",", ""))
                .withColumn("area", col("area").cast("float"))
                .groupBy("STATE")
                .agg(
                        first("CITY").alias("capital"),
                        sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
                        sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
                        sum("POP_GDP").alias("pop_2016"),
                        sum("GDP").alias("gdp_2016"),
                        sum("POST_OFFICES").alias("post_offices_ct"),
                        sum("WAL-MART").alias("wal_mart_ct"),
                        sum("MAC").alias("mc_donalds_ct"),
                        sum("Cars").alias("cars_ct"),
                        sum("Motorcycles").alias("moto_ct"),
                        sum("AREA").alias("area"),
                        sum("IBGE_PLANTED_AREA").alias("agr_area"),
                        sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
                        sum("HOTELS").alias("hotels_ct"),
                        sum("BEDS").alias("beds_ct"))
                .withColumn("agr_area", expr("agr_area / 100")) // converts hectares
                // to km2
                .orderBy(col("STATE"))
                .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"));
        switch (mode) {
            case CACHE:
                df = df.cache();
                break;
            case CHECKPOINT:
                df = df.checkpoint();
                break;
            case CHECKPOINT_NON_EAGER:
                df = df.checkpoint(false);
                break;
        }
        System.out.println("\n***** Pure data");
        df.show(5);

        long t1 = System.currentTimeMillis();
        System.out.println("Aggregation (ms) .................. " + (t1 - t0));

        // Regions per population
        System.out.println("\n***** Population");
        Dataset<Row> popDf = df
                .drop(
                        "area", "pop_brazil", "pop_foreign", "post_offices_ct",
                        "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
                        "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
                        "gdp_2016")
                .orderBy(col("pop_2016").desc());
        popDf.show(15);
        long t2 = System.currentTimeMillis();
        System.out.println("Population (ms) ................... " + (t2 - t1));

        // Regions per size in km2
        System.out.println("\n***** Area (squared kilometers)");
        Dataset<Row> areaDf = df
                .withColumn("area", round(col("area"), 2))
                .drop(
                        "pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct",
                        "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
                        "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
                        "gdp_2016")
                .orderBy(col("area").desc());
        areaDf.show(15);
        long t3 = System.currentTimeMillis();
        System.out.println("Area (ms) ......................... " + (t3 - t2));

        // McDonald's per 1m inhabitants
        System.out.println("\n***** McDonald's restaurants per 1m inhabitants");
        Dataset<Row> mcDonaldsPopDf = df
                .withColumn("mcd_1m_inh",
                        expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100"))
                .drop(
                        "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
                        "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct",
                        "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
                .orderBy(col("mcd_1m_inh").desc());
        mcDonaldsPopDf.show(5);
        long t4 = System.currentTimeMillis();
        System.out.println("Mc Donald's (ms) .................. " + (t4 - t3));

        // Walmart per 1m inhabitants
        System.out.println("\n***** Walmart supermarket per 1m inhabitants");
        Dataset<Row> walmartPopDf = df
                .withColumn("walmart_1m_inh",
                        expr("int(wal_mart_ct / pop_2016 * 100000000) / 100"))
                .drop(
                        "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
                        "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
                .orderBy(col("walmart_1m_inh").desc());
        walmartPopDf.show(5);
        long t5 = System.currentTimeMillis();
        System.out.println("Walmart (ms) ...................... " + (t5 - t4));

        // GDP per capita
        System.out.println("\n***** GDP per capita");
        Dataset<Row> gdpPerCapitaDf = df
                .drop(
                        "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
                        "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area")
                .withColumn("gdp_capita", expr("int(gdp_capita)"))
                .orderBy(col("gdp_capita").desc());
        gdpPerCapitaDf.show(5);
        long t6 = System.currentTimeMillis();
        System.out.println("GDP per capita (ms) ............... " + (t6 - t5));

        // Post offices
        System.out.println("\n***** Post offices");
        Dataset<Row> postOfficeDf = df
                .withColumn("post_office_1m_inh",
                        expr("int(post_offices_ct / pop_2016 * 100000000) / 100"))
                .withColumn("post_office_100k_km2",
                        expr("int(post_offices_ct / area * 10000000) / 100"))
                .drop(
                        "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
                        "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")
                .orderBy(col("post_office_1m_inh").desc());
        switch (mode) {
            case CACHE:
                postOfficeDf = postOfficeDf.cache();
                break;
            case CHECKPOINT:
                postOfficeDf = postOfficeDf.checkpoint();
                break;
            case CHECKPOINT_NON_EAGER:
                postOfficeDf = postOfficeDf.checkpoint(false);
                break;
        }
        System.out.println("\n****  Per 1 million inhabitants");
        Dataset<Row> postOfficePopDf = postOfficeDf
                .drop("post_office_100k_km2", "area")
                .orderBy(col("post_office_1m_inh").desc());
        postOfficePopDf.show(5);

        System.out.println("\n****  per 100000 km2");
        Dataset<Row> postOfficeArea = postOfficeDf
                .drop("post_office_1m_inh", "pop_2016")
                .orderBy(col("post_office_100k_km2").desc());
        postOfficeArea.show(5);
        long t7 = System.currentTimeMillis();
        System.out.println("Post offices (ms) ................. " + (t7 - t6) + " / Mode: " + mode);

        // Cars and motorcycles per 1k habitants
        System.out.println("\n***** Vehicles");
        Dataset<Row> vehiclesDf = df
                .withColumn("veh_1k_inh",
                        expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100"))
                .drop(
                        "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
                        "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area",
                        "pop_brazil")
                .orderBy(col("veh_1k_inh").desc());
        vehiclesDf.show(5);
        long t8 = System.currentTimeMillis();
        System.out.println("Vehicles (ms) ..................... " + (t8 - t7));

        // Cars and motorcycles per 1k habitants
        System.out.println("\n***** Agriculture - usage of land for agriculture");
        Dataset<Row> agricultureDf = df
                .withColumn("agr_area_pct",
                        expr("int(agr_area / area * 1000) / 10"))
                .withColumn("area",
                        expr("int(area)"))
                .drop(
                        "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
                        "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod",
                        "pop_2016")
                .orderBy(col("agr_area_pct").desc());
        agricultureDf.show(5);
        long t9 = System.currentTimeMillis();
        System.out.println("Agriculture revenue (ms) .......... " + (t9 - t8));

        long t_ = System.currentTimeMillis();
        System.out.println("Total with purification (ms) ...... " + (t_ - t0));
        System.out.println("Total without purification (ms) ... " + (t_ - t0));

        return t_ - t1;
    }

}
/*
***** Raw dataset and schema
+-------------------+-----+-------+------------+-----------------+-----------------+-------+-------------+-------------+--------+------+--------+--------+----------+----------+--------+-----------------+----------------------+-----------------+-----+----------+----------------+-------------+------------+------------+-------+------+------------+--------+--------------------+-------------+-------------+--------------------+-----------+------------+------------+----------+-----------+---------+----------+-------+----------+--------------------+------------+--------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+----+-----------+-----------+-------+-------+---------+---------+-----+-----------+---------------+----+----+--------+------------+
|               CITY|STATE|CAPITAL|IBGE_RES_POP|IBGE_RES_POP_BRAS|IBGE_RES_POP_ESTR|IBGE_DU|IBGE_DU_URBAN|IBGE_DU_RURAL|IBGE_POP|IBGE_1|IBGE_1-4|IBGE_5-9|IBGE_10-14|IBGE_15-59|IBGE_60+|IBGE_PLANTED_AREA|IBGE_CROP_PRODUCTION_$|IDHM Ranking 2010| IDHM|IDHM_Renda|IDHM_Longevidade|IDHM_Educacao|        LONG|         LAT|    ALT|PAY_TV|FIXED_PHONES|    AREA|          REGIAO_TUR|CATEGORIA_TUR|ESTIMATED_POP|         RURAL_URBAN|GVA_AGROPEC|GVA_INDUSTRY|GVA_SERVICES|GVA_PUBLIC| GVA_TOTAL |    TAXES|       GDP|POP_GDP|GDP_CAPITA|            GVA_MAIN|MUN_EXPENDIT|COMP_TOT|COMP_A|COMP_B|COMP_C|COMP_D|COMP_E|COMP_F|COMP_G|COMP_H|COMP_I|COMP_J|COMP_K|COMP_L|COMP_M|COMP_N|COMP_O|COMP_P|COMP_Q|COMP_R|COMP_S|COMP_T|COMP_U|HOTELS|BEDS|Pr_Agencies|Pu_Agencies|Pr_Bank|Pu_Bank|Pr_Assets|Pu_Assets| Cars|Motorcycles|Wheeled_tractor|UBER| MAC|WAL-MART|POST_OFFICES|
+-------------------+-----+-------+------------+-----------------+-----------------+-------+-------------+-------------+--------+------+--------+--------+----------+----------+--------+-----------------+----------------------+-----------------+-----+----------+----------------+-------------+------------+------------+-------+------+------------+--------+--------------------+-------------+-------------+--------------------+-----------+------------+------------+----------+-----------+---------+----------+-------+----------+--------------------+------------+--------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+----+-----------+-----------+-------+-------+---------+---------+-----+-----------+---------------+----+----+--------+------------+
|    Abadia De Goiás|   GO|      0|        6876|             6876|                0|   2137|         1546|          591|    5300|    69|     318|     438|       517|      3542|     416|              319|                  1843|             1689|0.708|     0.687|            0.83|        0.622|-49.44054783|-16.75881189|  893.6|   360|         842|  147.26|                null|         null|         8583|              Urbano|        6.2|    27991.25|    74750.32|  36915.04|   145857.6|  20554.2|    166.41|   8053|  20664.57|     Demais serviços|    28227691|     284|     5|     1|    56|     0|     2|    29|   110|    26|     4|     5|     0|     2|    10|    12|     4|     6|     6|     1|     5|     0|     0|  null|null|       null|       null|   null|   null|     null|     null| 2158|       1246|              0|null|null|    null|           1|
|Abadia Dos Dourados|   MG|      0|        6704|             6704|                0|   2328|         1481|          847|    4154|    38|     207|     260|       351|      2709|     589|             4479|                 18017|             2207| 0.69|     0.693|           0.839|        0.563|-47.39683244|-18.48756496| 753.12|    77|         296|  881.06| Caminhos Do Cerrado|            D|         6972|     Rural Adjacente|   50524.57|     25917.7|    62689.23|  28083.79|  167215.28|  12873.5|    180.09|   7037|   25591.7|     Demais serviços|    17909274|     476|     6|     6|    30|     1|     2|    34|   190|    70|    28|    11|     0|     4|    15|    29|     2|     9|    14|     6|    19|     0|     0|  null|null|       null|       null|   null|   null|     null|     null| 2227|       1142|              0|null|null|    null|           1|
|          Abadiânia|   GO|      0|       15757|            15609|              148|   4655|         3233|         1422|   10656|   139|     650|     894|      1087|      6896|     990|            10307|                 33085|             2202| 0.69|     0.671|           0.841|        0.579|-48.71881214|-16.18267186|1017.55|   227|         720|1,045.13|Região Turística ...|            C|        19614|     Rural Adjacente|      42.84|     16728.3|   138198.58|   63396.2|  261161.91| 26822.58| 287984.49|  18427|   15628.4|     Demais serviços|    37513019|     288|     5|     9|    26|     0|     2|     7|   117|    12|    57|     2|     1|     0|     7|    15|     3|    11|     5|     1|     8|     0|     0|     1|  34|          1|          1|      1|      1| 33724584| 67091904| 2838|       1426|              0|null|null|    null|           3|
|             Abaeté|   MG|      0|       22690|            22690|                0|   7694|         6667|         1027|   18464|   176|     856|    1233|      1539|     11979|    2681|             1862|                  7502|             1994|0.698|      0.72|           0.848|        0.556|-45.44619142|-19.15584769| 644.74|  1230|        1716|1,817.07| Lago De Três Marias|            D|        23223|              Urbano|   113824.6|    31002.62|      172.33|  86081.41|  403241.27| 26994.09| 430235.36|  23574|  18250.42|     Demais serviços|        null|     621|    18|     1|    40|     0|     1|    20|   303|    62|    30|     9|     6|     4|    28|    27|     2|    15|    19|     9|    27|     0|     0|  null|null|          2|          2|      2|      2| 44974716|371922572| 6928|       2953|              0|null|null|    null|           4|
|         Abaetetuba|   PA|      0|      141100|           141040|               60|  31061|        19057|        12004|   82956|  1354|    5567|    7618|      8905|     53516|    5996|            25200|                700872|             3530|0.628|     0.579|           0.798|        0.537|-48.88440382|-1.723469863|  10.12|  3389|        1218|1,610.65|  Araguaia-Tocantins|            D|       156292|              Urbano|  140463.72|     58610.0|   468128.69|  486872.4| 1154074.81| 95180.48|1249255.29| 151934|   8222.36|Administração, de...|        null|     931|     4|     2|    43|     0|     1|    27|   500|    16|    31|     6|     1|     1|    22|    16|     2|   155|    33|    15|    56|     0|     0|  null|null|          2|          4|      2|      4| 76181384|800078483| 5277|      25661|              0|null|null|    null|           2|
|            Abaiara|   CE|      0|       10496|            10496|                0|   2791|         1251|         1540|    4538|    98|     323|     421|       483|      2631|     582|             2598|                  5234|             3522|0.628|      0.54|           0.748|        0.612|-39.04754664|-7.356976596| 403.11|    29|          34|  180.08|                null|         null|        11663|     Rural Adjacente|    4435.16|        5.88|       22.81|  35989.96|   69108.67|  4042.79|  73151.46|  11483|   6370.41|Administração, de...|        null|      86|     1|     0|     4|     0|     0|     6|    48|     2|    10|     2|     0|     0|     2|     3|     2|     0|     2|     0|     4|     0|     0|  null|null|       null|       null|   null|   null|     null|     null|  553|       1674|              0|null|null|    null|           1|
|             Abaíra|   BA|      0|        8316|             8316|                0|   2572|         1193|         1379|    3725|    37|     156|     263|       277|      2319|     673|              895|                  3999|             4086|0.603|     0.577|           0.746|         0.51|-41.66160848|-13.25353189| 674.22|   952|         335|  538.68|  Chapada Diamantina|            D|         8767|        Rural Remoto|      12.41|     3437.43|    17990.74|  28463.74|   62304.83|  2019.77|  64324.59|   9212|    6982.7|Administração, de...|        null|     191|     6|     0|     8|     0|     1|     4|    97|     5|     5|     3|     1|     0|     5|     5|     2|     8|     1|     2|    38|     0|     0|     1|  24|       null|       null|   null|   null|     null|     null|  896|        696|              0|null|null|    null|           1|
|              Abaré|   BA|      0|       17064|            17064|                0|   4332|         2379|         1953|    8994|   167|     733|     978|       927|      5386|     803|             2058|                 22761|             4756|0.575|     0.533|           0.776|         0.46|-39.11658794|-8.723418246| 316.38|    51|         222|1,604.92|                null|         null|        19814|        Rural Remoto|     9176.4|         6.7|    36921.84|     65.75|     118.55|     6.21| 124754.26|  19939|    6256.8|Administração, de...|        null|      87|     2|     0|     3|     0|     0|     0|    71|     0|     1|     1|     0|     0|     0|     1|     2|     0|     2|     0|     4|     0|     0|  null|null|          1|          0|      1|      0| 21823314|        0|  613|       1532|              0|null|null|    null|           1|
|             Abatiá|   PR|      0|        7764|             7764|                0|   2499|         1877|          622|    5685|    69|     302|     370|       483|      3650|     811|             1197|                  9943|             2258|0.687|     0.676|           0.804|        0.596|-50.31252658|-23.30049404|  579.3|    55|         392|  228.72|                null|         null|         7507|     Rural Adjacente|   73340.52|     8839.71|    42999.37|  34103.49|  159283.08|     5.77| 165048.21|   7795|   21173.6|Agricultura, incl...|        null|     285|     5|     0|    20|     0|     1|    10|   133|    18|    14|     8|     0|     4|    11|    26|     2|     8|     9|     4|    12|     0|     0|  null|null|          0|          1|      0|      1|        0| 45976288| 2168|        912|              0|null|null|    null|           1|
|      Abdon Batista|   SC|      0|        2653|             2653|                0|    848|          234|          614|     724|    12|      32|      49|        63|       479|      89|             5502|                 26195|             2092| 0.69|      0.66|           0.812|        0.625|-51.02527221|-27.60898712| 720.98|   109|         260|  237.16|  Vale Do Contestado|            D|         2577|     Rural Adjacente|   24996.75|     3578.87|     16011.1|  17842.64|   62429.36|  2312.65|  64742.01|   2617|  24739.02|Administração, de...|    19506956|      69|     2|     0|     4|     0|     0|     2|    35|     8|     3|     1|     1|     0|     4|     0|     2|     1|     3|     0|     3|     0|     0|  null|null|          0|          1|      0|      1|        0| 42909056|  976|        345|              2|null|null|    null|           1|
|    Abel Figueiredo|   PA|      0|        6780|             6780|                0|   1880|         1650|          230|    5998|   102|     471|     609|       691|      3657|     468|             1184|                  4168|             3657|0.622|     0.625|             0.8|        0.481|-48.39676213|-4.951390896| 192.76|   248|          43|  614.13|  Amazônia Atlântica|            E|         7382|Intermediário Adj...|   21381.59|        5.31|    20515.48|  27736.46|   74947.67|  4709.92|  79657.59|   7179|  11095.92|Administração, de...|        null|      72|     0|     0|     9|     0|     1|     0|    41|     1|     0|     1|     0|     0|     2|     1|     6|     6|     0|     0|     4|     0|     0|  null|null|          0|          1|      0|      1|        0| 11709575|  279|        807|              0|null|null|    null|           1|
|       Abelardo Luz|   SC|      0|       17100|            17084|               16|   4739|         2694|         2045|    9544|   147|     616|     808|       963|      6068|     942|            57186|                254540|             2032|0.696|     0.684|           0.852|        0.578|-52.33648238|-26.56303102| 724.99|   484|         910|  953.99|        Grande Oeste|            D|        17847|     Rural Adjacente|  212531.45|    78176.47|    197092.1|   78972.1|  566772.13| 37508.23| 604280.35|  17782|   33982.7|Agricultura, incl...|        null|     464|     8|     2|    40|     1|     1|    25|   177|    67|    25|     9|     4|     7|    26|    16|     2|    14|    13|     4|    23|     0|     0|  null|null|          1|          2|      1|      2| 29730462|382942698| 5823|       2001|              1|null|null|    null|           1|
|         Abre Campo|   MG|      0|       13311|            13294|               17|   3930|         2202|         1728|    7101|    85|     362|     529|       620|      4512|     993|             4543|                 24155|             3046|0.654|     0.646|           0.823|        0.525|-42.48098999|-20.30144528| 589.83|   625|         519|  470.55|      Montanhas E Fé|            D|        13465|     Rural Adjacente|   43828.42|    10448.76|    72852.23|     50.66|  177793.23|  10110.5| 187903.74|  13726|  13689.62|     Demais serviços|        null|     369|     3|     2|    33|     0|     1|    12|   148|    31|    17|     5|     3|     4|    17|    12|     4|    11|    22|     6|    38|     0|     0|  null|null|          0|          1|      0|      1|        0| 81340685| 2838|       1851|              0|null|null|    null|           2|
|       Abreu E Lima|   PE|      0|       94429|            94407|               22|  28182|        25944|         2238|   81482|  1050|    4405|    6255|      7019|     54749|    8004|              387|                  2595|             2477|0.679|     0.625|           0.791|        0.632|-34.89913058| -7.90444899|  27.06|  1418|        4661|  126.19|Costa Náutica Cor...|            D|        99622|              Urbano|        7.8|   384262.09|      526.04| 336141.88|  1254241.3|170264.52|1424505.83|  98990|   14390.4|     Demais serviços|   119645700|     841|     1|     0|   130|     0|     2|    26|   434|    27|    36|    14|     3|     4|    18|    30|     2|    47|    20|     6|    41|     0|     0|  null|null|          2|          3|      2|      3|155632735|460626103|14579|      10122|              0|null|null|    null|           1|
|        Abreulândia|   TO|      0|        2391|             2391|                0|    760|          445|          315|    1476|    21|     107|     172|       182|       872|     122|            13102|                 31281|             2786|0.665|       0.7|           0.835|        0.502|-49.16243756|-9.621790712| 238.86|    14|          29|1,895.21|                null|         null|         2564|     Rural Adjacente|   20312.55|     2108.24|     6802.38|  16520.91|   45744.07|  1634.18|  47378.25|   2555|  18543.35|Administração, de...|     9712530|      27|     4|     0|     0|     0|     0|     3|    11|     0|     1|     1|     0|     0|     1|     1|     4|     0|     0|     0|     1|     0|     0|  null|null|       null|       null|   null|   null|     null|     null|  190|        318|              0|null|null|    null|           1|
+-------------------+-----+-------+------------+-----------------+-----------------+-------+-------------+-------------+--------+------+--------+--------+----------+----------+--------+-----------------+----------------------+-----------------+-----+----------+----------------+-------------+------------+------------+-------+------+------------+--------+--------------------+-------------+-------------+--------------------+-----------+------------+------------+----------+-----------+---------+----------+-------+----------+--------------------+------------+--------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+------+----+-----------+-----------+-------+-------+---------+---------+-----+-----------+---------------+----+----+--------+------------+
only showing top 15 rows

root
 |-- CITY: string (nullable = true)
 |-- STATE: string (nullable = true)
 |-- CAPITAL: integer (nullable = true)
 |-- IBGE_RES_POP: integer (nullable = true)
 |-- IBGE_RES_POP_BRAS: integer (nullable = true)
 |-- IBGE_RES_POP_ESTR: integer (nullable = true)
 |-- IBGE_DU: integer (nullable = true)
 |-- IBGE_DU_URBAN: integer (nullable = true)
 |-- IBGE_DU_RURAL: integer (nullable = true)
 |-- IBGE_POP: integer (nullable = true)
 |-- IBGE_1: integer (nullable = true)
 |-- IBGE_1-4: integer (nullable = true)
 |-- IBGE_5-9: integer (nullable = true)
 |-- IBGE_10-14: integer (nullable = true)
 |-- IBGE_15-59: integer (nullable = true)
 |-- IBGE_60+: integer (nullable = true)
 |-- IBGE_PLANTED_AREA: integer (nullable = true)
 |-- IBGE_CROP_PRODUCTION_$: integer (nullable = true)
 |-- IDHM Ranking 2010: integer (nullable = true)
 |-- IDHM: double (nullable = true)
 |-- IDHM_Renda: double (nullable = true)
 |-- IDHM_Longevidade: double (nullable = true)
 |-- IDHM_Educacao: double (nullable = true)
 |-- LONG: double (nullable = true)
 |-- LAT: double (nullable = true)
 |-- ALT: double (nullable = true)
 |-- PAY_TV: integer (nullable = true)
 |-- FIXED_PHONES: integer (nullable = true)
 |-- AREA: string (nullable = true)
 |-- REGIAO_TUR: string (nullable = true)
 |-- CATEGORIA_TUR: string (nullable = true)
 |-- ESTIMATED_POP: integer (nullable = true)
 |-- RURAL_URBAN: string (nullable = true)
 |-- GVA_AGROPEC: double (nullable = true)
 |-- GVA_INDUSTRY: double (nullable = true)
 |-- GVA_SERVICES: double (nullable = true)
 |-- GVA_PUBLIC: double (nullable = true)
 |--  GVA_TOTAL : double (nullable = true)
 |-- TAXES: double (nullable = true)
 |-- GDP: double (nullable = true)
 |-- POP_GDP: integer (nullable = true)
 |-- GDP_CAPITA: double (nullable = true)
 |-- GVA_MAIN: string (nullable = true)
 |-- MUN_EXPENDIT: long (nullable = true)
 |-- COMP_TOT: integer (nullable = true)
 |-- COMP_A: integer (nullable = true)
 |-- COMP_B: integer (nullable = true)
 |-- COMP_C: integer (nullable = true)
 |-- COMP_D: integer (nullable = true)
 |-- COMP_E: integer (nullable = true)
 |-- COMP_F: integer (nullable = true)
 |-- COMP_G: integer (nullable = true)
 |-- COMP_H: integer (nullable = true)
 |-- COMP_I: integer (nullable = true)
 |-- COMP_J: integer (nullable = true)
 |-- COMP_K: integer (nullable = true)
 |-- COMP_L: integer (nullable = true)
 |-- COMP_M: integer (nullable = true)
 |-- COMP_N: integer (nullable = true)
 |-- COMP_O: integer (nullable = true)
 |-- COMP_P: integer (nullable = true)
 |-- COMP_Q: integer (nullable = true)
 |-- COMP_R: integer (nullable = true)
 |-- COMP_S: integer (nullable = true)
 |-- COMP_T: integer (nullable = true)
 |-- COMP_U: integer (nullable = true)
 |-- HOTELS: integer (nullable = true)
 |-- BEDS: integer (nullable = true)
 |-- Pr_Agencies: integer (nullable = true)
 |-- Pu_Agencies: integer (nullable = true)
 |-- Pr_Bank: integer (nullable = true)
 |-- Pu_Bank: integer (nullable = true)
 |-- Pr_Assets: long (nullable = true)
 |-- Pu_Assets: long (nullable = true)
 |-- Cars: integer (nullable = true)
 |-- Motorcycles: integer (nullable = true)
 |-- Wheeled_tractor: integer (nullable = true)
 |-- UBER: integer (nullable = true)
 |-- MAC: integer (nullable = true)
 |-- WAL-MART: integer (nullable = true)
 |-- POST_OFFICES: integer (nullable = true)


***** Pure data
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|STATE|   capital|pop_brazil|pop_foreign|pop_2016|            gdp_2016|post_offices_ct|wal_mart_ct|mc_donalds_ct|cars_ct|moto_ct|              area|agr_area|agr_prod|hotels_ct|beds_ct|        gdp_capita|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|   AC|Rio Branco|    732629|        930|  816687|   4757012.914863586|             28|          0|            0|  87400| 144634|164123.75061035156|  998.43|  506950|       10|    411| 5824.768748447797|
|   AL|    Maceió|   3119722|        772| 3358963|4.5452747180238724E7|            136|          6|            6| 357183| 336964|27843.360107421875| 5215.81| 2192556|       21|   1225|13531.779653493868|
|   AM|    Manaus|   3476932|       7053| 4001667|  8.43052467495079E7|            103|          0|            3| 394443| 318069|1503340.9572753906|  1309.7| 1660381|       91|   6208|21067.531793502036|
|   AP|    Macapá|    668977|        549|  782295| 1.402544351076889E7|             36|          0|            0|  80686|  78547|142470.76989746094|  421.65|  209480|        7|    470|17928.586416593345|
|   BA|  Salvador|  14006865|      10041|15276566| 2.414818135993576E8|            777|         32|           21|1835913|1509451| 564722.7909622192|40716.24|15433518|      209|  15232|15807.336125105445|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
only showing top 5 rows

Aggregation (ms) .................. 4283

***** Population
+-----+--------------+--------+
|STATE|       capital|pop_2016|
+-----+--------------+--------+
|   SP|     São Paulo|44749699|
|   MG|Belo Horizonte|20997560|
|   RJ|Rio De Janeiro|16635996|
|   BA|      Salvador|15276566|
|   RS|  Porto Alegre|11286500|
|   PR|      Curitiba|11242720|
|   PE|        Recife| 9410336|
|   CE|     Fortaleza| 8963663|
|   PA|         Belém| 8272724|
|   MA|      São Luís| 6954036|
|   SC| Florianópolis| 6910553|
|   GO|       Goiânia| 6695855|
|   AM|        Manaus| 4001667|
|   PB|   João Pessoa| 3999415|
|   ES|       Vitória| 3973697|
+-----+--------------+--------+
only showing top 15 rows

Population (ms) ................... 1779

***** Area (squared kilometers)
+-----+--------------+----------+
|STATE|       capital|      area|
+-----+--------------+----------+
|   AM|        Manaus|1503340.96|
|   PA|         Belém|1245759.35|
|   MT|        Cuiabá| 903207.13|
|   MG|Belo Horizonte| 586521.48|
|   BA|      Salvador| 564722.79|
|   MS|  Campo Grande| 357145.57|
|   GO|       Goiânia| 340125.92|
|   MA|      São Luís| 329642.19|
|   RS|  Porto Alegre| 278848.33|
|   TO|        Palmas| 277720.44|
|   PI|      Teresina| 251616.93|
|   SP|     São Paulo| 248219.94|
|   RO|   Porto Velho| 237765.27|
|   RR|     Boa Vista| 224273.84|
|   PR|      Curitiba| 199305.38|
+-----+--------------+----------+
only showing top 15 rows

Area (ms) ......................... 1445

***** McDonald's restaurants per 1m inhabitants
+-----+--------------+--------+-------------+----------+
|STATE|       capital|pop_2016|mc_donalds_ct|mcd_1m_inh|
+-----+--------------+--------+-------------+----------+
|   DF|      Brasília| 2977216|           28|       9.4|
|   SP|     São Paulo|44749699|          315|      7.03|
|   RJ|Rio De Janeiro|16635996|          103|      6.19|
|   SC| Florianópolis| 6910553|           26|      3.76|
|   RS|  Porto Alegre|11286500|           40|      3.54|
+-----+--------------+--------+-------------+----------+
only showing top 5 rows

Mc Donald's (ms) .................. 1215

***** Walmart supermarket per 1m inhabitants
+-----+------------+--------+-----------+--------------+
|STATE|     capital|pop_2016|wal_mart_ct|walmart_1m_inh|
+-----+------------+--------+-----------+--------------+
|   RS|Porto Alegre|11286500|         52|           4.6|
|   PE|      Recife| 9410336|         22|          2.33|
|   SE|     Aracaju| 2265779|          5|           2.2|
|   BA|    Salvador|15276566|         32|          2.09|
|   AL|      Maceió| 3358963|          6|          1.78|
+-----+------------+--------+-----------+--------------+
only showing top 5 rows

Walmart (ms) ...................... 1150

***** GDP per capita
+-----+--------------+--------+--------------------+----------+
|STATE|       capital|pop_2016|            gdp_2016|gdp_capita|
+-----+--------------+--------+--------------------+----------+
|   DF|      Brasília| 2977216|        2.35497104E8|     79099|
|   SP|     São Paulo|44749699|1.7657257060075645E9|     39457|
|   RJ|Rio De Janeiro|16635996| 6.148317895841064E8|     36957|
|   SC| Florianópolis| 6910553|2.0441083797054672E8|     29579|
|   MS|  Campo Grande| 2682386|  7.90870977463913E7|     29483|
+-----+--------------+--------+--------------------+----------+
only showing top 5 rows

GDP per capita (ms) ............... 1233

***** Post offices

****  Per 1 million inhabitants
+-----+--------------+--------+---------------+------------------+
|STATE|       capital|pop_2016|post_offices_ct|post_office_1m_inh|
+-----+--------------+--------+---------------+------------------+
|   TO|        Palmas| 1532902|            151|              98.5|
|   MG|Belo Horizonte|20997560|           1925|             91.67|
|   RS|  Porto Alegre|11286500|            972|             86.12|
|   CE|     Fortaleza| 8963663|            745|             83.11|
|   MT|        Cuiabá| 3305531|            274|             82.89|
+-----+--------------+--------+---------------+------------------+
only showing top 5 rows


****  per 100000 km2
+-----+--------------+---------------+------------------+--------------------+
|STATE|       capital|post_offices_ct|              area|post_office_100k_km2|
+-----+--------------+---------------+------------------+--------------------+
|   RJ|Rio De Janeiro|            544| 43750.46017074585|             1243.41|
|   DF|      Brasília|             60|  5760.77978515625|             1041.52|
|   ES|       Vitória|            308| 46074.50023651123|              668.48|
|   SP|     São Paulo|           1447|248219.94022870064|              582.95|
|   SC| Florianópolis|            512| 95731.03971099854|              534.83|
+-----+--------------+---------------+------------------+--------------------+
only showing top 5 rows

Post offices (ms) ................. 2022 / Mode: NO_CACHE_NO_CHECKPOINT

***** Vehicles
+-----+-------------+--------+--------+-------+----------+
|STATE|      capital|pop_2016| cars_ct|moto_ct|veh_1k_inh|
+-----+-------------+--------+--------+-------+----------+
|   SC|Florianópolis| 6910553| 2942198|1151969|    592.45|
|   SP|    São Paulo|44749699|18274046|5617982|     533.9|
|   PR|     Curitiba|11242720| 4435871|1471749|    525.46|
|   DF|     Brasília| 2977216| 1288107| 211392|    503.65|
|   RS| Porto Alegre|11286500| 4319444|1281229|    496.22|
+-----+-------------+--------+--------+-------+----------+
only showing top 5 rows

Vehicles (ms) ..................... 1176

***** Agriculture - usage of land for agriculture
+-----+------------+------+---------+------------+
|STATE|     capital|  area| agr_area|agr_area_pct|
+-----+------------+------+---------+------------+
|   PR|    Curitiba|199305|105806.85|        53.0|
|   SP|   São Paulo|248219| 88242.08|        35.5|
|   RS|Porto Alegre|278848| 90721.48|        32.5|
|   DF|    Brasília|  5760|  1628.94|        28.2|
|   GO|     Goiânia|340125| 64331.29|        18.9|
+-----+------------+------+---------+------------+
only showing top 5 rows

Agriculture revenue (ms) .......... 932
Total with purification (ms) ...... 15235
Total without purification (ms) ... 15235

***** Pure data
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|STATE|   capital|pop_brazil|pop_foreign|pop_2016|            gdp_2016|post_offices_ct|wal_mart_ct|mc_donalds_ct|cars_ct|moto_ct|              area|agr_area|agr_prod|hotels_ct|beds_ct|        gdp_capita|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|   AC|Rio Branco|    732629|        930|  816687|   4757012.914863586|             28|          0|            0|  87400| 144634|164123.75061035156|  998.43|  506950|       10|    411| 5824.768748447797|
|   AL|    Maceió|   3119722|        772| 3358963|4.5452747180238724E7|            136|          6|            6| 357183| 336964|27843.360107421875| 5215.81| 2192556|       21|   1225|13531.779653493868|
|   AM|    Manaus|   3476932|       7053| 4001667|  8.43052467495079E7|            103|          0|            3| 394443| 318069|1503340.9572753906|  1309.7| 1660381|       91|   6208|21067.531793502036|
|   AP|    Macapá|    668977|        549|  782295| 1.402544351076889E7|             36|          0|            0|  80686|  78547|142470.76989746094|  421.65|  209480|        7|    470|17928.586416593345|
|   BA|  Salvador|  14006865|      10041|15276566| 2.414818135993576E8|            777|         32|           21|1835913|1509451| 564722.7909622192|40716.24|15433518|      209|  15232|15807.336125105445|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
only showing top 5 rows

Aggregation (ms) .................. 2600

***** Population
+-----+--------------+--------+
|STATE|       capital|pop_2016|
+-----+--------------+--------+
|   SP|     São Paulo|44749699|
|   MG|Belo Horizonte|20997560|
|   RJ|Rio De Janeiro|16635996|
|   BA|      Salvador|15276566|
|   RS|  Porto Alegre|11286500|
|   PR|      Curitiba|11242720|
|   PE|        Recife| 9410336|
|   CE|     Fortaleza| 8963663|
|   PA|         Belém| 8272724|
|   MA|      São Luís| 6954036|
|   SC| Florianópolis| 6910553|
|   GO|       Goiânia| 6695855|
|   AM|        Manaus| 4001667|
|   PB|   João Pessoa| 3999415|
|   ES|       Vitória| 3973697|
+-----+--------------+--------+
only showing top 15 rows

Population (ms) ................... 243

***** Area (squared kilometers)
+-----+--------------+----------+
|STATE|       capital|      area|
+-----+--------------+----------+
|   AM|        Manaus|1503340.96|
|   PA|         Belém|1245759.35|
|   MT|        Cuiabá| 903207.13|
|   MG|Belo Horizonte| 586521.48|
|   BA|      Salvador| 564722.79|
|   MS|  Campo Grande| 357145.57|
|   GO|       Goiânia| 340125.92|
|   MA|      São Luís| 329642.19|
|   RS|  Porto Alegre| 278848.33|
|   TO|        Palmas| 277720.44|
|   PI|      Teresina| 251616.93|
|   SP|     São Paulo| 248219.94|
|   RO|   Porto Velho| 237765.27|
|   RR|     Boa Vista| 224273.84|
|   PR|      Curitiba| 199305.38|
+-----+--------------+----------+
only showing top 15 rows

Area (ms) ......................... 181

***** McDonald's restaurants per 1m inhabitants
+-----+--------------+--------+-------------+----------+
|STATE|       capital|pop_2016|mc_donalds_ct|mcd_1m_inh|
+-----+--------------+--------+-------------+----------+
|   DF|      Brasília| 2977216|           28|       9.4|
|   SP|     São Paulo|44749699|          315|      7.03|
|   RJ|Rio De Janeiro|16635996|          103|      6.19|
|   SC| Florianópolis| 6910553|           26|      3.76|
|   RS|  Porto Alegre|11286500|           40|      3.54|
+-----+--------------+--------+-------------+----------+
only showing top 5 rows

Mc Donald's (ms) .................. 161

***** Walmart supermarket per 1m inhabitants
+-----+------------+--------+-----------+--------------+
|STATE|     capital|pop_2016|wal_mart_ct|walmart_1m_inh|
+-----+------------+--------+-----------+--------------+
|   RS|Porto Alegre|11286500|         52|           4.6|
|   PE|      Recife| 9410336|         22|          2.33|
|   SE|     Aracaju| 2265779|          5|           2.2|
|   BA|    Salvador|15276566|         32|          2.09|
|   AL|      Maceió| 3358963|          6|          1.78|
+-----+------------+--------+-----------+--------------+
only showing top 5 rows

Walmart (ms) ...................... 263

***** GDP per capita
+-----+--------------+--------+--------------------+----------+
|STATE|       capital|pop_2016|            gdp_2016|gdp_capita|
+-----+--------------+--------+--------------------+----------+
|   DF|      Brasília| 2977216|        2.35497104E8|     79099|
|   SP|     São Paulo|44749699|1.7657257060075645E9|     39457|
|   RJ|Rio De Janeiro|16635996| 6.148317895841064E8|     36957|
|   SC| Florianópolis| 6910553|2.0441083797054672E8|     29579|
|   MS|  Campo Grande| 2682386|  7.90870977463913E7|     29483|
+-----+--------------+--------+--------------------+----------+
only showing top 5 rows

GDP per capita (ms) ............... 202

***** Post offices

****  Per 1 million inhabitants
+-----+--------------+--------+---------------+------------------+
|STATE|       capital|pop_2016|post_offices_ct|post_office_1m_inh|
+-----+--------------+--------+---------------+------------------+
|   TO|        Palmas| 1532902|            151|              98.5|
|   MG|Belo Horizonte|20997560|           1925|             91.67|
|   RS|  Porto Alegre|11286500|            972|             86.12|
|   CE|     Fortaleza| 8963663|            745|             83.11|
|   MT|        Cuiabá| 3305531|            274|             82.89|
+-----+--------------+--------+---------------+------------------+
only showing top 5 rows


****  per 100000 km2
+-----+--------------+---------------+------------------+--------------------+
|STATE|       capital|post_offices_ct|              area|post_office_100k_km2|
+-----+--------------+---------------+------------------+--------------------+
|   RJ|Rio De Janeiro|            544| 43750.46017074585|             1243.41|
|   DF|      Brasília|             60|  5760.77978515625|             1041.52|
|   ES|       Vitória|            308| 46074.50023651123|              668.48|
|   SP|     São Paulo|           1447|248219.94022870064|              582.95|
|   SC| Florianópolis|            512| 95731.03971099854|              534.83|
+-----+--------------+---------------+------------------+--------------------+
only showing top 5 rows

Post offices (ms) ................. 770 / Mode: CACHE

***** Vehicles
+-----+-------------+--------+--------+-------+----------+
|STATE|      capital|pop_2016| cars_ct|moto_ct|veh_1k_inh|
+-----+-------------+--------+--------+-------+----------+
|   SC|Florianópolis| 6910553| 2942198|1151969|    592.45|
|   SP|    São Paulo|44749699|18274046|5617982|     533.9|
|   PR|     Curitiba|11242720| 4435871|1471749|    525.46|
|   DF|     Brasília| 2977216| 1288107| 211392|    503.65|
|   RS| Porto Alegre|11286500| 4319444|1281229|    496.22|
+-----+-------------+--------+--------+-------+----------+
only showing top 5 rows

Vehicles (ms) ..................... 164

***** Agriculture - usage of land for agriculture
+-----+------------+------+---------+------------+
|STATE|     capital|  area| agr_area|agr_area_pct|
+-----+------------+------+---------+------------+
|   PR|    Curitiba|199305|105806.85|        53.0|
|   SP|   São Paulo|248219| 88242.08|        35.5|
|   RS|Porto Alegre|278848| 90721.48|        32.5|
|   DF|    Brasília|  5760|  1628.94|        28.2|
|   GO|     Goiânia|340125| 64331.29|        18.9|
+-----+------------+------+---------+------------+
only showing top 5 rows

Agriculture revenue (ms) .......... 164
Total with purification (ms) ...... 4748
Total without purification (ms) ... 4748

***** Pure data
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|STATE|   capital|pop_brazil|pop_foreign|pop_2016|            gdp_2016|post_offices_ct|wal_mart_ct|mc_donalds_ct|cars_ct|moto_ct|              area|agr_area|agr_prod|hotels_ct|beds_ct|        gdp_capita|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|   AC|Rio Branco|    732629|        930|  816687|   4757012.914863586|             28|          0|            0|  87400| 144634|164123.75061035156|  998.43|  506950|       10|    411| 5824.768748447797|
|   AL|    Maceió|   3119722|        772| 3358963|4.5452747180238724E7|            136|          6|            6| 357183| 336964|27843.360107421875| 5215.81| 2192556|       21|   1225|13531.779653493868|
|   AM|    Manaus|   3476932|       7053| 4001667|  8.43052467495079E7|            103|          0|            3| 394443| 318069|1503340.9572753906|  1309.7| 1660381|       91|   6208|21067.531793502036|
|   AP|    Macapá|    668977|        549|  782295| 1.402544351076889E7|             36|          0|            0|  80686|  78547|142470.76989746094|  421.65|  209480|        7|    470|17928.586416593345|
|   BA|  Salvador|  14006865|      10041|15276566| 2.414818135993576E8|            777|         32|           21|1835913|1509451| 564722.7909622192|40716.24|15433518|      209|  15232|15807.336125105445|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
only showing top 5 rows

Aggregation (ms) .................. 588

***** Population
+-----+--------------+--------+
|STATE|       capital|pop_2016|
+-----+--------------+--------+
|   SP|     São Paulo|44749699|
|   MG|Belo Horizonte|20997560|
|   RJ|Rio De Janeiro|16635996|
|   BA|      Salvador|15276566|
|   RS|  Porto Alegre|11286500|
|   PR|      Curitiba|11242720|
|   PE|        Recife| 9410336|
|   CE|     Fortaleza| 8963663|
|   PA|         Belém| 8272724|
|   MA|      São Luís| 6954036|
|   SC| Florianópolis| 6910553|
|   GO|       Goiânia| 6695855|
|   AM|        Manaus| 4001667|
|   PB|   João Pessoa| 3999415|
|   ES|       Vitória| 3973697|
+-----+--------------+--------+
only showing top 15 rows

Population (ms) ................... 135

***** Area (squared kilometers)
+-----+--------------+----------+
|STATE|       capital|      area|
+-----+--------------+----------+
|   AM|        Manaus|1503340.96|
|   PA|         Belém|1245759.35|
|   MT|        Cuiabá| 903207.13|
|   MG|Belo Horizonte| 586521.48|
|   BA|      Salvador| 564722.79|
|   MS|  Campo Grande| 357145.57|
|   GO|       Goiânia| 340125.92|
|   MA|      São Luís| 329642.19|
|   RS|  Porto Alegre| 278848.33|
|   TO|        Palmas| 277720.44|
|   PI|      Teresina| 251616.93|
|   SP|     São Paulo| 248219.94|
|   RO|   Porto Velho| 237765.27|
|   RR|     Boa Vista| 224273.84|
|   PR|      Curitiba| 199305.38|
+-----+--------------+----------+
only showing top 15 rows

Area (ms) ......................... 118

***** McDonald's restaurants per 1m inhabitants
+-----+--------------+--------+-------------+----------+
|STATE|       capital|pop_2016|mc_donalds_ct|mcd_1m_inh|
+-----+--------------+--------+-------------+----------+
|   DF|      Brasília| 2977216|           28|       9.4|
|   SP|     São Paulo|44749699|          315|      7.03|
|   RJ|Rio De Janeiro|16635996|          103|      6.19|
|   SC| Florianópolis| 6910553|           26|      3.76|
|   RS|  Porto Alegre|11286500|           40|      3.54|
+-----+--------------+--------+-------------+----------+
only showing top 5 rows

Mc Donald's (ms) .................. 138

***** Walmart supermarket per 1m inhabitants
+-----+------------+--------+-----------+--------------+
|STATE|     capital|pop_2016|wal_mart_ct|walmart_1m_inh|
+-----+------------+--------+-----------+--------------+
|   RS|Porto Alegre|11286500|         52|           4.6|
|   PE|      Recife| 9410336|         22|          2.33|
|   SE|     Aracaju| 2265779|          5|           2.2|
|   BA|    Salvador|15276566|         32|          2.09|
|   AL|      Maceió| 3358963|          6|          1.78|
+-----+------------+--------+-----------+--------------+
only showing top 5 rows

Walmart (ms) ...................... 121

***** GDP per capita
+-----+--------------+--------+--------------------+----------+
|STATE|       capital|pop_2016|            gdp_2016|gdp_capita|
+-----+--------------+--------+--------------------+----------+
|   DF|      Brasília| 2977216|        2.35497104E8|     79099|
|   SP|     São Paulo|44749699|1.7657257060075645E9|     39457|
|   RJ|Rio De Janeiro|16635996| 6.148317895841064E8|     36957|
|   SC| Florianópolis| 6910553|2.0441083797054672E8|     29579|
|   MS|  Campo Grande| 2682386|  7.90870977463913E7|     29483|
+-----+--------------+--------+--------------------+----------+
only showing top 5 rows

GDP per capita (ms) ............... 81

***** Post offices

****  Per 1 million inhabitants
+-----+--------------+--------+---------------+------------------+
|STATE|       capital|pop_2016|post_offices_ct|post_office_1m_inh|
+-----+--------------+--------+---------------+------------------+
|   TO|        Palmas| 1532902|            151|              98.5|
|   MG|Belo Horizonte|20997560|           1925|             91.67|
|   RS|  Porto Alegre|11286500|            972|             86.12|
|   CE|     Fortaleza| 8963663|            745|             83.11|
|   MT|        Cuiabá| 3305531|            274|             82.89|
+-----+--------------+--------+---------------+------------------+
only showing top 5 rows


****  per 100000 km2
+-----+--------------+---------------+------------------+--------------------+
|STATE|       capital|post_offices_ct|              area|post_office_100k_km2|
+-----+--------------+---------------+------------------+--------------------+
|   RJ|Rio De Janeiro|            544| 43750.46017074585|             1243.41|
|   DF|      Brasília|             60|  5760.77978515625|             1041.52|
|   ES|       Vitória|            308| 46074.50023651123|              668.48|
|   SP|     São Paulo|           1447|248219.94022870064|              582.95|
|   SC| Florianópolis|            512| 95731.03971099854|              534.83|
+-----+--------------+---------------+------------------+--------------------+
only showing top 5 rows

Post offices (ms) ................. 621 / Mode: CHECKPOINT

***** Vehicles
+-----+-------------+--------+--------+-------+----------+
|STATE|      capital|pop_2016| cars_ct|moto_ct|veh_1k_inh|
+-----+-------------+--------+--------+-------+----------+
|   SC|Florianópolis| 6910553| 2942198|1151969|    592.45|
|   SP|    São Paulo|44749699|18274046|5617982|     533.9|
|   PR|     Curitiba|11242720| 4435871|1471749|    525.46|
|   DF|     Brasília| 2977216| 1288107| 211392|    503.65|
|   RS| Porto Alegre|11286500| 4319444|1281229|    496.22|
+-----+-------------+--------+--------+-------+----------+
only showing top 5 rows

Vehicles (ms) ..................... 152

***** Agriculture - usage of land for agriculture
+-----+------------+------+---------+------------+
|STATE|     capital|  area| agr_area|agr_area_pct|
+-----+------------+------+---------+------------+
|   PR|    Curitiba|199305|105806.85|        53.0|
|   SP|   São Paulo|248219| 88242.08|        35.5|
|   RS|Porto Alegre|278848| 90721.48|        32.5|
|   DF|    Brasília|  5760|  1628.94|        28.2|
|   GO|     Goiânia|340125| 64331.29|        18.9|
+-----+------------+------+---------+------------+
only showing top 5 rows

Agriculture revenue (ms) .......... 110
Total with purification (ms) ...... 2064
Total without purification (ms) ... 2064

***** Pure data
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|STATE|   capital|pop_brazil|pop_foreign|pop_2016|            gdp_2016|post_offices_ct|wal_mart_ct|mc_donalds_ct|cars_ct|moto_ct|              area|agr_area|agr_prod|hotels_ct|beds_ct|        gdp_capita|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
|   AC|Rio Branco|    732629|        930|  816687|   4757012.914863586|             28|          0|            0|  87400| 144634|164123.75061035156|  998.43|  506950|       10|    411| 5824.768748447797|
|   AL|    Maceió|   3119722|        772| 3358963|4.5452747180238724E7|            136|          6|            6| 357183| 336964|27843.360107421875| 5215.81| 2192556|       21|   1225|13531.779653493868|
|   AM|    Manaus|   3476932|       7053| 4001667|  8.43052467495079E7|            103|          0|            3| 394443| 318069|1503340.9572753906|  1309.7| 1660381|       91|   6208|21067.531793502036|
|   AP|    Macapá|    668977|        549|  782295| 1.402544351076889E7|             36|          0|            0|  80686|  78547|142470.76989746094|  421.65|  209480|        7|    470|17928.586416593345|
|   BA|  Salvador|  14006865|      10041|15276566| 2.414818135993576E8|            777|         32|           21|1835913|1509451| 564722.7909622192|40716.24|15433518|      209|  15232|15807.336125105445|
+-----+----------+----------+-----------+--------+--------------------+---------------+-----------+-------------+-------+-------+------------------+--------+--------+---------+-------+------------------+
only showing top 5 rows

Aggregation (ms) .................. 313

***** Population
+-----+--------------+--------+
|STATE|       capital|pop_2016|
+-----+--------------+--------+
|   SP|     São Paulo|44749699|
|   MG|Belo Horizonte|20997560|
|   RJ|Rio De Janeiro|16635996|
|   BA|      Salvador|15276566|
|   RS|  Porto Alegre|11286500|
|   PR|      Curitiba|11242720|
|   PE|        Recife| 9410336|
|   CE|     Fortaleza| 8963663|
|   PA|         Belém| 8272724|
|   MA|      São Luís| 6954036|
|   SC| Florianópolis| 6910553|
|   GO|       Goiânia| 6695855|
|   AM|        Manaus| 4001667|
|   PB|   João Pessoa| 3999415|
|   ES|       Vitória| 3973697|
+-----+--------------+--------+
only showing top 15 rows

Population (ms) ................... 112

***** Area (squared kilometers)
+-----+--------------+----------+
|STATE|       capital|      area|
+-----+--------------+----------+
|   AM|        Manaus|1503340.96|
|   PA|         Belém|1245759.35|
|   MT|        Cuiabá| 903207.13|
|   MG|Belo Horizonte| 586521.48|
|   BA|      Salvador| 564722.79|
|   MS|  Campo Grande| 357145.57|
|   GO|       Goiânia| 340125.92|
|   MA|      São Luís| 329642.19|
|   RS|  Porto Alegre| 278848.33|
|   TO|        Palmas| 277720.44|
|   PI|      Teresina| 251616.93|
|   SP|     São Paulo| 248219.94|
|   RO|   Porto Velho| 237765.27|
|   RR|     Boa Vista| 224273.84|
|   PR|      Curitiba| 199305.38|
+-----+--------------+----------+
only showing top 15 rows

Area (ms) ......................... 95

***** McDonald's restaurants per 1m inhabitants
+-----+--------------+--------+-------------+----------+
|STATE|       capital|pop_2016|mc_donalds_ct|mcd_1m_inh|
+-----+--------------+--------+-------------+----------+
|   DF|      Brasília| 2977216|           28|       9.4|
|   SP|     São Paulo|44749699|          315|      7.03|
|   RJ|Rio De Janeiro|16635996|          103|      6.19|
|   SC| Florianópolis| 6910553|           26|      3.76|
|   RS|  Porto Alegre|11286500|           40|      3.54|
+-----+--------------+--------+-------------+----------+
only showing top 5 rows

Mc Donald's (ms) .................. 93

***** Walmart supermarket per 1m inhabitants
+-----+------------+--------+-----------+--------------+
|STATE|     capital|pop_2016|wal_mart_ct|walmart_1m_inh|
+-----+------------+--------+-----------+--------------+
|   RS|Porto Alegre|11286500|         52|           4.6|
|   PE|      Recife| 9410336|         22|          2.33|
|   SE|     Aracaju| 2265779|          5|           2.2|
|   BA|    Salvador|15276566|         32|          2.09|
|   AL|      Maceió| 3358963|          6|          1.78|
+-----+------------+--------+-----------+--------------+
only showing top 5 rows

Walmart (ms) ...................... 90

***** GDP per capita
+-----+--------------+--------+--------------------+----------+
|STATE|       capital|pop_2016|            gdp_2016|gdp_capita|
+-----+--------------+--------+--------------------+----------+
|   DF|      Brasília| 2977216|        2.35497104E8|     79099|
|   SP|     São Paulo|44749699|1.7657257060075645E9|     39457|
|   RJ|Rio De Janeiro|16635996| 6.148317895841064E8|     36957|
|   SC| Florianópolis| 6910553|2.0441083797054672E8|     29579|
|   MS|  Campo Grande| 2682386|  7.90870977463913E7|     29483|
+-----+--------------+--------+--------------------+----------+
only showing top 5 rows

GDP per capita (ms) ............... 108

***** Post offices

****  Per 1 million inhabitants
+-----+--------------+--------+---------------+------------------+
|STATE|       capital|pop_2016|post_offices_ct|post_office_1m_inh|
+-----+--------------+--------+---------------+------------------+
|   TO|        Palmas| 1532902|            151|              98.5|
|   MG|Belo Horizonte|20997560|           1925|             91.67|
|   RS|  Porto Alegre|11286500|            972|             86.12|
|   CE|     Fortaleza| 8963663|            745|             83.11|
|   MT|        Cuiabá| 3305531|            274|             82.89|
+-----+--------------+--------+---------------+------------------+
only showing top 5 rows


****  per 100000 km2
+-----+--------------+---------------+------------------+--------------------+
|STATE|       capital|post_offices_ct|              area|post_office_100k_km2|
+-----+--------------+---------------+------------------+--------------------+
|   RJ|Rio De Janeiro|            544| 43750.46017074585|             1243.41|
|   DF|      Brasília|             60|  5760.77978515625|             1041.52|
|   ES|       Vitória|            308| 46074.50023651123|              668.48|
|   SP|     São Paulo|           1447|248219.94022870064|              582.95|
|   SC| Florianópolis|            512| 95731.03971099854|              534.83|
+-----+--------------+---------------+------------------+--------------------+
only showing top 5 rows

Post offices (ms) ................. 513 / Mode: CHECKPOINT_NON_EAGER

***** Vehicles
+-----+-------------+--------+--------+-------+----------+
|STATE|      capital|pop_2016| cars_ct|moto_ct|veh_1k_inh|
+-----+-------------+--------+--------+-------+----------+
|   SC|Florianópolis| 6910553| 2942198|1151969|    592.45|
|   SP|    São Paulo|44749699|18274046|5617982|     533.9|
|   PR|     Curitiba|11242720| 4435871|1471749|    525.46|
|   DF|     Brasília| 2977216| 1288107| 211392|    503.65|
|   RS| Porto Alegre|11286500| 4319444|1281229|    496.22|
+-----+-------------+--------+--------+-------+----------+
only showing top 5 rows

Vehicles (ms) ..................... 80

***** Agriculture - usage of land for agriculture
+-----+------------+------+---------+------------+
|STATE|     capital|  area| agr_area|agr_area_pct|
+-----+------------+------+---------+------------+
|   PR|    Curitiba|199305|105806.85|        53.0|
|   SP|   São Paulo|248219| 88242.08|        35.5|
|   RS|Porto Alegre|278848| 90721.48|        32.5|
|   DF|    Brasília|  5760|  1628.94|        28.2|
|   GO|     Goiânia|340125| 64331.29|        18.9|
+-----+------------+------+---------+------------+
only showing top 5 rows

Agriculture revenue (ms) .......... 97
Total with purification (ms) ...... 1501
Total without purification (ms) ... 1501

***** Processing times (excluding purification)
Without cache ............... 10952 ms
With cache .................. 2148 ms
With checkpoint ............. 1476 ms
With non-eager checkpoint ... 1188 ms
 */