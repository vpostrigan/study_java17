package spark_in_action2021.part3transform_data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark_in_action2021.Logs;

/**
 * All joins in a single app, inspired by
 * https://stackoverflow.com/questions/45990633/what-are-the-various-join-types-in-spark.
 *
 * @author jgp
 */
public class Lab12_34AllJoinsApp {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab12_34AllJoinsApp app = new Lab12_34AllJoinsApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("All joins!")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)}
        );

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, "Value 1"));
        rows.add(RowFactory.create(2, "Value 2"));
        rows.add(RowFactory.create(3, "Value 3"));
        rows.add(RowFactory.create(4, "Value 4"));
        Dataset<Row> dfLeft = spark.createDataFrame(rows, schema);
        allLogs.showAndSaveToCsv("dfLeft", dfLeft, 5, false, false);

        rows = new ArrayList<>();
        rows.add(RowFactory.create(3, "Value 3"));
        rows.add(RowFactory.create(4, "Value 4"));
        rows.add(RowFactory.create(4, "Value 4_1"));
        rows.add(RowFactory.create(5, "Value 5"));
        rows.add(RowFactory.create(6, "Value 6"));
        Dataset<Row> dfRight = spark.createDataFrame(rows, schema);
        allLogs.showAndSaveToCsv("dfRight", dfRight, 5, false, false);

        Map<String, String> joinTypes = new LinkedHashMap<>();
        joinTypes.put("inner", "при внутреннем соединении выбираються все строки " +
                "из левого набора данных и правого набора данных, " +
                "для которых выполняеться условии соединения"); // v2.0.0. default

        joinTypes.put("outer", "выбирают данные из обоих наборов данных " +
                "на основе условия соединения и добавляют значения null, " +
                "если данные отсутствуют в левом или правом наборе данных"); // v2.0.0
        joinTypes.put("full", ""); // v2.1.1
        joinTypes.put("full_outer", ""); // v2.1.1

        joinTypes.put("left", "все строки из левого набора данных; " +
                "все строки из правого набора данных, " +
                "для которых выполняется условие соединения"); // v2.1.1
        joinTypes.put("left_outer", ""); // v2.0.0

        joinTypes.put("right", "все строки из правого набора данных; " +
                "все строки из левого набора данных, " +
                "для которых выполняется условие соединения"); // v2.1.1
        joinTypes.put("right_outer", ""); // v2.0.0

        joinTypes.put("left_semi", "Выбирает строки только из левого набора данных, " +
                "для которых выполняеться условие соединения " +
                "(колонок из правого набора не будет)"); // v2.0.0, was leftsemi before v2.1.1

        joinTypes.put("left_anti", "выбирает строки только из левого набора данных, " +
                "для которых не выполняется условие соединения"); // v2.1.1

        joinTypes.put("cross", "декартово соединение обоих наборов данных"); // with a column, v2.2.0

        for (Map.Entry<String, String> e : joinTypes.entrySet()) {
            String joinType = e.getKey();

            Dataset<Row> df = dfLeft.join(dfRight,
                    dfLeft.col("id").equalTo(dfRight.col("id")),
                    joinType);
            df.orderBy(dfLeft.col("id"));

            allLogs.showAndSaveToCsv(joinType.toUpperCase() + " JOIN (" + e.getValue() + ")", df, 10, false, false);
        }

        System.out.println("CROSS JOIN (without a column)");
        Dataset<Row> df = dfLeft.crossJoin(dfRight);
        df.orderBy(dfLeft.col("id")).show();
    }
/**
 21:57:01.692: dfLeft
 +---+-------+
 |id |value  |
 +---+-------+
 |1  |Value 1|
 |2  |Value 2|
 |3  |Value 3|
 |4  |Value 4|
 +---+-------+

 21:57:04.312: dfLeft has 4. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/1_csv
 // //
 21:57:04.643: dfRight
 +---+---------+
 |id |value    |
 +---+---------+
 |3  |Value 3  |
 |4  |Value 4  |
 |4  |Value 4_1|
 |5  |Value 5  |
 |6  |Value 6  |
 +---+---------+

 21:57:04.913: dfRight has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/2_csv
 // //
 21:57:05.086: INNER JOIN (при внутреннем соединении выбираються все строки из левого набора данных и правого набора данных, для которых выполняеться условии соединения)
 +---+-------+---+---------+
 |id |value  |id |value    |
 +---+-------+---+---------+
 |3  |Value 3|3  |Value 3  |
 |4  |Value 4|4  |Value 4  |
 |4  |Value 4|4  |Value 4_1|
 +---+-------+---+---------+

 // //
 21:57:05.992: OUTER JOIN (выбирают данные из обоих наборов данных на основе условия соединения и добавляют значения null, если данные отсутствуют в левом или правом наборе данных)
 +----+-------+----+---------+
 |id  |value  |id  |value    |
 +----+-------+----+---------+
 |1   |Value 1|null|null     |
 |null|null   |6   |Value 6  |
 |3   |Value 3|3   |Value 3  |
 |null|null   |5   |Value 5  |
 |4   |Value 4|4   |Value 4  |
 |4   |Value 4|4   |Value 4_1|
 |2   |Value 2|null|null     |
 +----+-------+----+---------+
 21:57:09.305: OUTER JOIN (выбирают данные из обоих наборов данных на основе условия соединения и добавляют значения null, если данные отсутствуют в левом или правом наборе данных) has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/4_csv
 // //
 21:57:10.346: FULL JOIN ()
 +----+-------+----+---------+
 |id  |value  |id  |value    |
 +----+-------+----+---------+
 |1   |Value 1|null|null     |
 |null|null   |6   |Value 6  |
 |3   |Value 3|3   |Value 3  |
 |null|null   |5   |Value 5  |
 |4   |Value 4|4   |Value 4  |
 |4   |Value 4|4   |Value 4_1|
 |2   |Value 2|null|null     |
 +----+-------+----+---------+
 21:57:12.073: FULL JOIN () has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/5_csv
 // //
 21:57:12.727: FULL_OUTER JOIN ()
 +----+-------+----+---------+
 |id  |value  |id  |value    |
 +----+-------+----+---------+
 |1   |Value 1|null|null     |
 |null|null   |6   |Value 6  |
 |3   |Value 3|3   |Value 3  |
 |null|null   |5   |Value 5  |
 |4   |Value 4|4   |Value 4  |
 |4   |Value 4|4   |Value 4_1|
 |2   |Value 2|null|null     |
 +----+-------+----+---------+
 21:57:14.131: FULL_OUTER JOIN () has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/6_csv
 // //
 21:57:14.770: LEFT JOIN (все строки из левого набора данных; все строки из правого набора данных, для которых выполняется условие соединения)
 +---+-------+----+---------+
 |id |value  |id  |value    |
 +---+-------+----+---------+
 |1  |Value 1|null|null     |
 |2  |Value 2|null|null     |
 |3  |Value 3|3   |Value 3  |
 |4  |Value 4|4   |Value 4_1|
 |4  |Value 4|4   |Value 4  |
 +---+-------+----+---------+
 21:57:14.906: LEFT JOIN (все строки из левого набора данных; все строки из правого набора данных, для которых выполняется условие соединения) has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/7_csv
 // //
 21:57:15.103: LEFT_OUTER JOIN ()
 +---+-------+----+---------+
 |id |value  |id  |value    |
 +---+-------+----+---------+
 |1  |Value 1|null|null     |
 |2  |Value 2|null|null     |
 |3  |Value 3|3   |Value 3  |
 |4  |Value 4|4   |Value 4_1|
 |4  |Value 4|4   |Value 4  |
 +---+-------+----+---------+
 21:57:15.194: LEFT_OUTER JOIN () has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/8_csv
 // //
 21:57:15.283: RIGHT JOIN (все строки из правого набора данных; все строки из левого набора данных, для которых выполняется условие соединения)
 +----+-------+---+---------+
 |id  |value  |id |value    |
 +----+-------+---+---------+
 |3   |Value 3|3  |Value 3  |
 |4   |Value 4|4  |Value 4  |
 |4   |Value 4|4  |Value 4_1|
 |null|null   |5  |Value 5  |
 |null|null   |6  |Value 6  |
 +----+-------+---+---------+
 21:57:15.456: RIGHT JOIN (все строки из правого набора данных; все строки из левого набора данных, для которых выполняется условие соединения) has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/9_csv
 // //
 21:57:15.592: RIGHT_OUTER JOIN ()
 +----+-------+---+---------+
 |id  |value  |id |value    |
 +----+-------+---+---------+
 |3   |Value 3|3  |Value 3  |
 |4   |Value 4|4  |Value 4  |
 |4   |Value 4|4  |Value 4_1|
 |null|null   |5  |Value 5  |
 |null|null   |6  |Value 6  |
 +----+-------+---+---------+
 21:57:15.737: RIGHT_OUTER JOIN () has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/10_csv
 // //
 21:57:15.815: LEFT_SEMI JOIN (Выбирает строки только из левого набора данных, для которых выполняеться условие соединения (колонок из правого набора не будет))
 +---+-------+
 |id |value  |
 +---+-------+
 |3  |Value 3|
 |4  |Value 4|
 +---+-------+

 21:57:16.127: LEFT_SEMI JOIN (Выбирает строки только из левого набора данных, для которых выполняеться условие соединения (колонок из правого набора не будет)) has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/11_csv
 // //
 21:57:16.223: LEFT_ANTI JOIN (выбирает строки только из левого набора данных, для которых не выполняется условие соединения)
 +---+-------+
 |id |value  |
 +---+-------+
 |1  |Value 1|
 |2  |Value 2|
 +---+-------+

 21:57:16.503: LEFT_ANTI JOIN (выбирает строки только из левого набора данных, для которых не выполняется условие соединения) has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/12_csv
 // //
 21:57:16.586: CROSS JOIN (декартово соединение обоих наборов данных)
 +---+-------+---+---------+
 |id |value  |id |value    |
 +---+-------+---+---------+
 |3  |Value 3|3  |Value 3  |
 |4  |Value 4|4  |Value 4  |
 |4  |Value 4|4  |Value 4_1|
 +---+-------+---+---------+
 21:57:16.664: CROSS JOIN (декартово соединение обоих наборов данных) has 3. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_34AllJoinsApp/13_csv
 // //
 CROSS JOIN (without a column)
 +---+-------+---+---------+
 | id|  value| id|    value|
 +---+-------+---+---------+
 |  1|Value 1|  4|Value 4_1|
 |  1|Value 1|  3|  Value 3|
 |  1|Value 1|  5|  Value 5|
 |  1|Value 1|  4|  Value 4|
 |  1|Value 1|  6|  Value 6|
 |  2|Value 2|  3|  Value 3|
 |  2|Value 2|  4|Value 4_1|
 |  2|Value 2|  6|  Value 6|
 |  2|Value 2|  4|  Value 4|
 |  2|Value 2|  5|  Value 5|
 |  3|Value 3|  4|  Value 4|
 |  3|Value 3|  4|Value 4_1|
 |  3|Value 3|  5|  Value 5|
 |  3|Value 3|  6|  Value 6|
 |  3|Value 3|  3|  Value 3|
 |  4|Value 4|  3|  Value 3|
 |  4|Value 4|  4|  Value 4|
 |  4|Value 4|  4|Value 4_1|
 |  4|Value 4|  6|  Value 6|
 |  4|Value 4|  5|  Value 5|
 +---+-------+---+---------+
 */
}
