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
public class Lab12_35AllJoinsDifferentDataTypesApp {

    public static void main(String[] args) {
        Logs allLogs = new Logs();

        Lab12_35AllJoinsDifferentDataTypesApp app = new Lab12_35AllJoinsDifferentDataTypesApp();
        app.start(allLogs);
    }

    private void start(Logs allLogs) {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Processing of invoices")
                .master("local")
                .getOrCreate();

        StructType schemaLeft = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)});

        StructType schemaRight = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("idx", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)});

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, "Value 1"));
        rows.add(RowFactory.create(2, "Value 2"));
        rows.add(RowFactory.create(3, "Value 3"));
        rows.add(RowFactory.create(4, "Value 4"));
        Dataset<Row> dfLeft = spark.createDataFrame(rows, schemaLeft);
        allLogs.showAndSaveToCsv("dfLeft", dfLeft, 5, false, false);

        rows = new ArrayList<>();
        rows.add(RowFactory.create("3", "Value 3"));
        rows.add(RowFactory.create("4", "Value 4"));
        rows.add(RowFactory.create("4", "Value 4_1"));
        rows.add(RowFactory.create("5", "Value 5"));
        rows.add(RowFactory.create("6", "Value 6"));
        Dataset<Row> dfRight = spark.createDataFrame(rows, schemaRight);
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
                    dfLeft.col("id").equalTo(dfRight.col("idx")),
                    joinType);
            df.orderBy(dfLeft.col("id"));

            allLogs.showAndSaveToCsv(joinType.toUpperCase() + " JOIN (" + e.getValue() + ")", df, 10, false, false);
        }

        System.out.println("CROSS JOIN (without a column)");
        Dataset<Row> df = dfLeft.crossJoin(dfRight);
        df.orderBy(dfLeft.col("id")).show();
    }
/*
22:46:35.839: dfLeft
+---+-------+
|id |value  |
+---+-------+
|1  |Value 1|
|2  |Value 2|
|3  |Value 3|
|4  |Value 4|
+---+-------+
22:46:38.201: dfLeft has 4. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/1_csv
// //
22:46:38.558: dfRight
+---+---------+
|idx|value    |
+---+---------+
|3  |Value 3  |
|4  |Value 4  |
|4  |Value 4_1|
|5  |Value 5  |
|6  |Value 6  |
+---+---------+

22:46:38.815: dfRight has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/2_csv
// //
22:46:38.979: INNER JOIN (при внутреннем соединении выбираються все строки из левого набора данных и правого набора данных, для которых выполняеться условии соединения)
+---+-------+---+---------+
|id |value  |idx|value    |
+---+-------+---+---------+
|3  |Value 3|3  |Value 3  |
|4  |Value 4|4  |Value 4  |
|4  |Value 4|4  |Value 4_1|
+---+-------+---+---------+
22:46:39.351: INNER JOIN (при внутреннем соединении выбираються все строки из левого набора данных и правого набора данных, для которых выполняеться условии соединения) has 3. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/3_csv
// //
22:46:39.856: OUTER JOIN (выбирают данные из обоих наборов данных на основе условия соединения и добавляют значения null, если данные отсутствуют в левом или правом наборе данных)
+----+-------+----+---------+
|id  |value  |idx |value    |
+----+-------+----+---------+
|1   |Value 1|null|null     |
|null|null   |6   |Value 6  |
|3   |Value 3|3   |Value 3  |
|null|null   |5   |Value 5  |
|4   |Value 4|4   |Value 4  |
|4   |Value 4|4   |Value 4_1|
|2   |Value 2|null|null     |
+----+-------+----+---------+
22:46:43.586: OUTER JOIN (выбирают данные из обоих наборов данных на основе условия соединения и добавляют значения null, если данные отсутствуют в левом или правом наборе данных) has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/4_csv
// //
22:46:44.690: FULL JOIN ()
+----+-------+----+---------+
|id  |value  |idx |value    |
+----+-------+----+---------+
|1   |Value 1|null|null     |
|null|null   |6   |Value 6  |
|3   |Value 3|3   |Value 3  |
|null|null   |5   |Value 5  |
|4   |Value 4|4   |Value 4  |
|4   |Value 4|4   |Value 4_1|
|2   |Value 2|null|null     |
+----+-------+----+---------+
22:46:46.392: FULL JOIN () has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/5_csv
// //
22:46:47.221: FULL_OUTER JOIN ()
+----+-------+----+---------+
|id  |value  |idx |value    |
+----+-------+----+---------+
|1   |Value 1|null|null     |
|null|null   |6   |Value 6  |
|3   |Value 3|3   |Value 3  |
|null|null   |5   |Value 5  |
|4   |Value 4|4   |Value 4  |
|4   |Value 4|4   |Value 4_1|
|2   |Value 2|null|null     |
+----+-------+----+---------+
22:46:48.649: FULL_OUTER JOIN () has 7. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/6_csv
// //
22:46:49.373: LEFT JOIN (все строки из левого набора данных; все строки из правого набора данных, для которых выполняется условие соединения)
+---+-------+----+---------+
|id |value  |idx |value    |
+---+-------+----+---------+
|1  |Value 1|null|null     |
|2  |Value 2|null|null     |
|3  |Value 3|3   |Value 3  |
|4  |Value 4|4   |Value 4_1|
|4  |Value 4|4   |Value 4  |
+---+-------+----+---------+
22:46:49.489: LEFT JOIN (все строки из левого набора данных; все строки из правого набора данных, для которых выполняется условие соединения) has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/7_csv
// //
22:46:49.636: LEFT_OUTER JOIN ()
+---+-------+----+---------+
|id |value  |idx |value    |
+---+-------+----+---------+
|1  |Value 1|null|null     |
|2  |Value 2|null|null     |
|3  |Value 3|3   |Value 3  |
|4  |Value 4|4   |Value 4_1|
|4  |Value 4|4   |Value 4  |
+---+-------+----+---------+
22:46:49.752: LEFT_OUTER JOIN () has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/8_csv
// //
22:46:49.848: RIGHT JOIN (все строки из правого набора данных; все строки из левого набора данных, для которых выполняется условие соединения)
+----+-------+---+---------+
|id  |value  |idx|value    |
+----+-------+---+---------+
|3   |Value 3|3  |Value 3  |
|4   |Value 4|4  |Value 4  |
|4   |Value 4|4  |Value 4_1|
|null|null   |5  |Value 5  |
|null|null   |6  |Value 6  |
+----+-------+---+---------+
22:46:50.008: RIGHT JOIN (все строки из правого набора данных; все строки из левого набора данных, для которых выполняется условие соединения) has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/9_csv
// //
22:46:50.165: RIGHT_OUTER JOIN ()
+----+-------+---+---------+
|id  |value  |idx|value    |
+----+-------+---+---------+
|3   |Value 3|3  |Value 3  |
|4   |Value 4|4  |Value 4  |
|4   |Value 4|4  |Value 4_1|
|null|null   |5  |Value 5  |
|null|null   |6  |Value 6  |
+----+-------+---+---------+
22:46:50.286: RIGHT_OUTER JOIN () has 5. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/10_csv
// //
22:46:50.374: LEFT_SEMI JOIN (Выбирает строки только из левого набора данных, для которых выполняеться условие соединения (колонок из правого набора не будет))
+---+-------+
|id |value  |
+---+-------+
|3  |Value 3|
|4  |Value 4|
+---+-------+

22:46:50.692: LEFT_SEMI JOIN (Выбирает строки только из левого набора данных, для которых выполняеться условие соединения (колонок из правого набора не будет)) has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/11_csv
// //
22:46:50.772: LEFT_ANTI JOIN (выбирает строки только из левого набора данных, для которых не выполняется условие соединения)
+---+-------+
|id |value  |
+---+-------+
|1  |Value 1|
|2  |Value 2|
+---+-------+

22:46:51.040: LEFT_ANTI JOIN (выбирает строки только из левого набора данных, для которых не выполняется условие соединения) has 2. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/12_csv
// //
22:46:51.193: CROSS JOIN (декартово соединение обоих наборов данных)
+---+-------+---+---------+
|id |value  |idx|value    |
+---+-------+---+---------+
|3  |Value 3|3  |Value 3  |
|4  |Value 4|4  |Value 4  |
|4  |Value 4|4  |Value 4_1|
+---+-------+---+---------+
22:46:51.266: CROSS JOIN (декартово соединение обоих наборов данных) has 3. Saved to C:\Users\admin\AppData\Local\Temp\spark_in_action2021/Lab12_35AllJoinsDifferentDataTypesApp/13_csv
// //
CROSS JOIN (without a column)
+---+-------+---+---------+
| id|  value|idx|    value|
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
