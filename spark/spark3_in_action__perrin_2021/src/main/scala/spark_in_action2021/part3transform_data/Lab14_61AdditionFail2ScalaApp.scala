package spark_in_action2021.part3transform_data

import java.util.ArrayList

import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}

/**
 * Additions via UDF.
 *
 * @author rambabu.posa
 */
object Lab14_61AdditionFail2ScalaApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Addition")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("add", new Lab14_61IntegerAdditionScalaUdf, DataTypes.StringType) // Same return type

    spark.udf.register("add", new Lab14_61StringAdditionScalaUdf, DataTypes.StringType)

    var df = createDataframe(spark)
    df.show(false)

    df = df
      .withColumn("concat",
        callUDF("add", col("fname"), col("lname")))

    df.show(false)

    // The next operation will fail with an error:
    // Exception in thread "main" org.apache.spark.SparkException: Failed to
    // execute user defined function($anonfun$261: (int, int) => string)

    df = df
      .withColumn("score",
        callUDF("add", col("score1"), col("score2")))

    df.show(false)

  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("fname", DataTypes.StringType, false),
      DataTypes.createStructField("lname", DataTypes.StringType, false),
      DataTypes.createStructField("score1", DataTypes.IntegerType, false),
      DataTypes.createStructField("score2", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("Jean-Georges", "Perrin", int2Integer(123), int2Integer(456)))
    rows.add(RowFactory.create("Jacek", "Laskowski", int2Integer(147), int2Integer(758)))
    rows.add(RowFactory.create("Holden", "Karau", int2Integer(258), int2Integer(369)))

    spark.createDataFrame(rows, schema)
  }

}
/*
+------------+---------+------+------+
|fname       |lname    |score1|score2|
+------------+---------+------+------+
|Jean-Georges|Perrin   |123   |456   |
|Jacek       |Laskowski|147   |758   |
|Holden      |Karau    |258   |369   |
+------------+---------+------+------+

+------------+---------+------+------+------------------+
|fname       |lname    |score1|score2|concat            |
+------------+---------+------+------+------------------+
|Jean-Georges|Perrin   |123   |456   |Jean-GeorgesPerrin|
|Jacek       |Laskowski|147   |758   |JacekLaskowski    |
|Holden      |Karau    |258   |369   |HoldenKarau       |
+------------+---------+------+------+------------------+

Exception in thread "main" org.apache.spark.SparkException: Failed to execute user defined function(UDFRegistration$$Lambda$825/0x00000008007e2840: (int, int) => string)
	at org.apache.spark.sql.catalyst.expressions.ScalaUDF.eval(ScalaUDF.scala:1193)
	at org.apache.spark.sql.catalyst.expressions.Alias.eval(namedExpressions.scala:160)
	at org.apache.spark.sql.catalyst.expressions.InterpretedMutableProjection.apply(InterpretedMutableProjection.scala:97)
	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$$anonfun$apply$19.$anonfun$applyOrElse$75(Optimizer.scala:1616)
	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:238)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at scala.collection.TraversableLike.map(TraversableLike.scala:238)
	at scala.collection.TraversableLike.map$(TraversableLike.scala:231)
	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$$anonfun$apply$19.applyOrElse(Optimizer.scala:1616)
	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$$anonfun$apply$19.applyOrElse(Optimizer.scala:1611)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$1(TreeNode.scala:318)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:74)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:318)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:173)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:171)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:323)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:408)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:244)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:406)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:359)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:323)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:173)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:171)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:323)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:408)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:244)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:406)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:359)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:323)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:173)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:171)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:323)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:408)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:244)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:406)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:359)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:323)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:173)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:171)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transform(TreeNode.scala:307)
	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$.apply(Optimizer.scala:1611)
	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$.apply(Optimizer.scala:1610)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$2(RuleExecutor.scala:216)
	at scala.collection.IndexedSeqOptimized.foldLeft(IndexedSeqOptimized.scala:60)
	at scala.collection.IndexedSeqOptimized.foldLeft$(IndexedSeqOptimized.scala:68)
	at scala.collection.mutable.WrappedArray.foldLeft(WrappedArray.scala:38)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:213)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1$adapted(RuleExecutor.scala:205)
	at scala.collection.immutable.List.foreach(List.scala:392)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:205)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:183)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:88)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:183)
	at org.apache.spark.sql.execution.QueryExecution.$anonfun$optimizedPlan$1(QueryExecution.scala:87)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
	at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:143)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:775)
	at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:143)
	at org.apache.spark.sql.execution.QueryExecution.optimizedPlan$lzycompute(QueryExecution.scala:84)
	at org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:84)
	at org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:95)
	at org.apache.spark.sql.execution.QueryExecution.executedPlan$lzycompute(QueryExecution.scala:113)
	at org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:110)
	at org.apache.spark.sql.execution.QueryExecution.$anonfun$simpleString$2(QueryExecution.scala:161)
	at org.apache.spark.sql.execution.ExplainUtils$.processPlan(ExplainUtils.scala:115)
	at org.apache.spark.sql.execution.QueryExecution.simpleString(QueryExecution.scala:161)
	at org.apache.spark.sql.execution.QueryExecution.org$apache$spark$sql$execution$QueryExecution$$explainString(QueryExecution.scala:206)
	at org.apache.spark.sql.execution.QueryExecution.explainString(QueryExecution.scala:175)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$5(SQLExecution.scala:98)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:163)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:90)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:775)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:3698)
	at org.apache.spark.sql.Dataset.head(Dataset.scala:2735)
	at org.apache.spark.sql.Dataset.take(Dataset.scala:2942)
	at org.apache.spark.sql.Dataset.getRows(Dataset.scala:302)
	at org.apache.spark.sql.Dataset.showString(Dataset.scala:339)
	at org.apache.spark.sql.Dataset.show(Dataset.scala:828)
	at org.apache.spark.sql.Dataset.show(Dataset.scala:805)
	at spark_in_action2021.part3transform_data.Lab14_61AdditionFail2ScalaApp$.main(Lab14_61AdditionFail2ScalaApp.scala:44)
	at spark_in_action2021.part3transform_data.Lab14_61AdditionFail2ScalaApp.main(Lab14_61AdditionFail2ScalaApp.scala)
Caused by: java.lang.ClassCastException: class java.lang.Integer cannot be cast to class java.lang.String (java.lang.Integer and java.lang.String are in module java.base of loader 'bootstrap')
	at spark_in_action2021.part3transform_data.Lab14_61StringAdditionScalaUdf.call(Lab14_61StringAdditionScalaUdf.scala:11)
	at org.apache.spark.sql.UDFRegistration.$anonfun$register$354(UDFRegistration.scala:793)
	at org.apache.spark.sql.catalyst.expressions.ScalaUDF.$anonfun$f$3(ScalaUDF.scala:217)
	at org.apache.spark.sql.catalyst.expressions.ScalaUDF.eval(ScalaUDF.scala:1190)
	... 96 more
 */