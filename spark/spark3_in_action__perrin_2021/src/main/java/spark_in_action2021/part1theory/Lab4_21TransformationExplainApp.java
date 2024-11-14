package spark_in_action2021.part1theory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;

public class Lab4_21TransformationExplainApp {

    public static void main(String[] args) {
        Lab4_21TransformationExplainApp app = new Lab4_21TransformationExplainApp();
        app.start();
    }

    private void start() {

        // Step 1 - Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Showing execution plan")
                .master("local")
                .getOrCreate();

        // Step 2 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
        Dataset<Row> df0 = df;

        // Step 3 - Build a bigger dataset
        df = df.union(df0);

        // Step 4 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        // Step 5 - Transformation
        df = df
                .withColumn("avg", expr("(lcl+ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));

        // Step 6 - explain - Spark v3

        System.out.println("explain:");
        df.explain();

        // simple
        System.out.println("simple:");
        df.explain("simple");

        // extended
        System.out.println("extended:");
        df.explain("extended");

        // codegen
        System.out.println("codegen:");
        df.explain("codegen");

        // cost
        System.out.println("cost:");
        df.explain("cost");

        // formatted
        System.out.println("formatted:");
        df.explain("formatted");
    }

}
/*
explain:

== Physical Plan ==
Union
:- *(1) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
:  +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...
+- *(2) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
   +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...


simple:
== Physical Plan ==
Union
:- *(1) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
:  +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...
+- *(2) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
   +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...


extended:
== Parsed Logical Plan ==
Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, ucl#62, avg#72, lcl2#83, ucl#62 AS ucl2#95]
+- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, ucl#62, avg#72, lcl#52 AS lcl2#83]
   +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, ucl#62, ((cast(lcl#52 as double) + cast(ucl#62 as double)) / cast(2 as double)) AS avg#72]
      +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, Upper Confidence Limit#24 AS ucl#62]
         +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24]
            +- Union false, false
               :- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv
               +- Project [Year#16 AS Year#34, State#17 AS State#35, County#18 AS County#36, State FIPS Code#19 AS State FIPS Code#37, County FIPS Code#20 AS County FIPS Code#38, Combined FIPS Code#21 AS Combined FIPS Code#39, Birth Rate#22 AS Birth Rate#40, Lower Confidence Limit#23 AS Lower Confidence Limit#41, Upper Confidence Limit#24 AS Upper Confidence Limit#42]
                  +- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv

== Analyzed Logical Plan ==
Year: string, State: string, County: string, State FIPS Code: string, County FIPS Code: string, Combined FIPS Code: string, Birth Rate: string, lcl: string, ucl: string, avg: double, lcl2: string, ucl2: string
Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, ucl#62, avg#72, lcl2#83, ucl#62 AS ucl2#95]
+- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, ucl#62, avg#72, lcl#52 AS lcl2#83]
   +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, ucl#62, ((cast(lcl#52 as double) + cast(ucl#62 as double)) / cast(2 as double)) AS avg#72]
      +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, lcl#52, Upper Confidence Limit#24 AS ucl#62]
         +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24]
            +- Union false, false
               :- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv
               +- Project [Year#16 AS Year#34, State#17 AS State#35, County#18 AS County#36, State FIPS Code#19 AS State FIPS Code#37, County FIPS Code#20 AS County FIPS Code#38, Combined FIPS Code#21 AS Combined FIPS Code#39, Birth Rate#22 AS Birth Rate#40, Lower Confidence Limit#23 AS Lower Confidence Limit#41, Upper Confidence Limit#24 AS Upper Confidence Limit#42]
                  +- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv

== Optimized Logical Plan ==
Union false, false
:- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
:  +- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv
+- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
   +- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv

== Physical Plan ==
Union
:- *(1) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
:  +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...
+- *(2) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
   +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...

codegen:
23/10/24 15:12:05 INFO CodeGenerator: Code generated in 88.6758 ms
23/10/24 15:12:05 INFO CodeGenerator: Code generated in 33.7421 ms
Found 2 WholeStageCodegen subtrees.
== Subtree 1 / 2 (maxMethodCodeSize:864; maxConstantPoolSize:131(0.20% used); numInnerClasses:0) ==
*(1) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
+- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...

Generated code:
/ 001 / public Object generate(Object[] references) {
    / 002 /   return new GeneratedIteratorForCodegenStage1(references);
    / 003 / }
/ 004 /
/ 005 / // codegenStageId=1
/ 006 / final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
    / 007 /   private Object[] references;
    / 008 /   private scala.collection.Iterator[] inputs;
    / 009 /   private scala.collection.Iterator inputadapter_input_0;
    / 010 /   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] project_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
    / 011 /
    / 012 /   public GeneratedIteratorForCodegenStage1(Object[] references) {
        / 013 /     this.references = references;
        / 014 /   }
    / 015 /
    / 016 /   public void init(int index, scala.collection.Iterator[] inputs) {
        / 017 /     partitionIndex = index;
        / 018 /     this.inputs = inputs;
        / 019 /     inputadapter_input_0 = inputs[0];
        / 020 /     project_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(12, 352);
        / 021 /
        / 022 /   }
    / 023 /
    / 024 /   protected void processNext() throws java.io.IOException {
        / 025 /     while ( inputadapter_input_0.hasNext()) {
            / 026 /       InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();
            / 027 /
            / 028 /       boolean inputadapter_isNull_7 = inputadapter_row_0.isNullAt(7);
            / 029 /       UTF8String inputadapter_value_7 = inputadapter_isNull_7 ?
                    / 030 /       null : (inputadapter_row_0.getUTF8String(7));
            / 031 /       boolean inputadapter_isNull_8 = inputadapter_row_0.isNullAt(8);
            / 032 /       UTF8String inputadapter_value_8 = inputadapter_isNull_8 ?
                    / 033 /       null : (inputadapter_row_0.getUTF8String(8));
...

== Subtree 2 / 2 (maxMethodCodeSize:864; maxConstantPoolSize:131(0.20% used); numInnerClasses:0) ==
        *(2) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
        +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...

        Generated code:
/ 001 / public Object generate(Object[] references) {
        / 002 /   return new GeneratedIteratorForCodegenStage2(references);
        / 003 / }
/ 004 /
/ 005 / // codegenStageId=2
/ 006 / final class GeneratedIteratorForCodegenStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
...


cost:
        == Optimized Logical Plan ==
        Union false, false, Statistics(sizeInBytes=6.3 MiB)
        :- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95], Statistics(sizeInBytes=3.2 MiB)
        :  +- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv, Statistics(sizeInBytes=2.5 MiB)
        +- Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112], Statistics(sizeInBytes=3.2 MiB)
        +- Relation[Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] csv, Statistics(sizeInBytes=2.5 MiB)

        == Physical Plan ==
        Union
        :- *(1) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
        :  +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...
        +- *(2) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
        +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...


        formatted:
        == Physical Plan ==
        Union (5)
        :- * Project (2)
        :  +- Scan csv  (1)
        +- * Project (4)
        +- Scan csv  (3)


        (1) Scan csv
        Output [9]: [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23, Upper Confidence Limit#24]
        Batched: false
        Location: InMemoryFileIndex [file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv]
        ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Combined FIPS Code:string,Birth Rate:string,Lower Confidence Limit:string,Upper Confidence Limit:string>

        (2) Project [codegen id : 1]
        Output [12]: [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]
        Input [9]: [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23, Upper Confidence Limit#24]

        (3) Scan csv
        Output [9]: [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23, Upper Confidence Limit#24]
        Batched: false
        Location: InMemoryFileIndex [file:/D:/workspace_study/study/Java11/spark3_in_action__perrin_2021/data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv]
        ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Combined FIPS Code:string,Birth Rate:string,Lower Confidence Limit:string,Upper Confidence Limit:string>

        (4) Project [codegen id : 2]
        Output [12]: [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#108, Upper Confidence Limit#24 AS ucl#109, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#110, Lower Confidence Limit#23 AS lcl2#111, Upper Confidence Limit#24 AS ucl2#112]
        Input [9]: [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23, Upper Confidence Limit#24]

        (5) Union

 */