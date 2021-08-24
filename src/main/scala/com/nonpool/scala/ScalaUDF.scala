package com.nonpool.scala

import com.nonpool.java.NormalPOJO
import org.apache.spark.sql.{Encoders, SparkSession}

object ScalaUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("udf scala test")
      .master("local[*]")
      .config("spark.ui.enabled", false)
      .getOrCreate()

    import spark.implicits._
    spark.createDataset(Seq("a", "b")).createTempView("test")

    returnCaseClassInUDF(spark)
//    returnJavaNormalClassInUDF(spark)
		returnCaseClassWithSchemaInUDF(spark)

    spark.stop()
  }

  /**
   * show correctly:
+-------+
|      a|
+-------+
|{a, 19}|
|{b, 19}|
+-------+
   */
  def returnCaseClassInUDF(spark: SparkSession): Unit = {

    spark.udf.register("stu", (str: String) => Student(str, 19))

    spark.sql("select stu(value) as a from test").show()
  }


  /**
   * got exception:
   *
   * Exception in thread "main" org.apache.spark.sql.AnalysisException: Undefined function: 'man'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 7
	at org.apache.spark.sql.catalyst.analysis.Analyzer$LookupFunctions$$anonfun$apply$16.$anonfun$applyOrElse$121(Analyzer.scala:2068)
	at org.apache.spark.sql.catalyst.analysis.package$.withPosition(package.scala:53)
	at org.apache.spark.sql.catalyst.analysis.Analyzer$LookupFunctions$$anonfun$apply$16.applyOrElse(Analyzer.scala:2068)
	at org.apache.spark.sql.catalyst.analysis.Analyzer$LookupFunctions$$anonfun$apply$16.applyOrElse(Analyzer.scala:2059)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$1(TreeNode.scala:317)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:73)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:317)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:322)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:407)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:243)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:405)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:358)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:322)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$transformExpressionsDown$1(QueryPlan.scala:94)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$1(QueryPlan.scala:116)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:73)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:116)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.recursiveTransform$1(QueryPlan.scala:127)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$3(QueryPlan.scala:132)
	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
	at scala.collection.immutable.List.foreach(List.scala:431)
	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
	at scala.collection.immutable.List.map(List.scala:305)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.recursiveTransform$1(QueryPlan.scala:132)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$4(QueryPlan.scala:137)
	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:243)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:137)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsDown(QueryPlan.scala:94)
	at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressions(QueryPlan.scala:85)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$$anonfun$resolveExpressions$1.applyOrElse(AnalysisHelper.scala:151)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$$anonfun$resolveExpressions$1.applyOrElse(AnalysisHelper.scala:150)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.$anonfun$resolveOperatorsDown$2(AnalysisHelper.scala:108)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:73)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.$anonfun$resolveOperatorsDown$1(AnalysisHelper.scala:108)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.allowInvokingTransformsInAnalyzer(AnalysisHelper.scala:221)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.resolveOperatorsDown(AnalysisHelper.scala:106)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.resolveOperatorsDown$(AnalysisHelper.scala:104)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.resolveOperatorsDown(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.resolveOperators(AnalysisHelper.scala:73)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.resolveOperators$(AnalysisHelper.scala:72)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.resolveOperators(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.resolveExpressions(AnalysisHelper.scala:150)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.resolveExpressions$(AnalysisHelper.scala:149)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.resolveExpressions(LogicalPlan.scala:29)
	at org.apache.spark.sql.catalyst.analysis.Analyzer$LookupFunctions$.apply(Analyzer.scala:2059)
	at org.apache.spark.sql.catalyst.analysis.Analyzer$LookupFunctions$.apply(Analyzer.scala:2056)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$2(RuleExecutor.scala:216)
	at scala.collection.IndexedSeqOptimized.foldLeft(IndexedSeqOptimized.scala:60)
	at scala.collection.IndexedSeqOptimized.foldLeft$(IndexedSeqOptimized.scala:68)
	at scala.collection.mutable.WrappedArray.foldLeft(WrappedArray.scala:38)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:213)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1$adapted(RuleExecutor.scala:205)
	at scala.collection.immutable.List.foreach(List.scala:431)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:205)
	at org.apache.spark.sql.catalyst.analysis.Analyzer.org$apache$spark$sql$catalyst$analysis$Analyzer$$executeSameContext(Analyzer.scala:196)
	at org.apache.spark.sql.catalyst.analysis.Analyzer.execute(Analyzer.scala:190)
	at org.apache.spark.sql.catalyst.analysis.Analyzer.execute(Analyzer.scala:155)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:183)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:88)
	at org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:183)
	at org.apache.spark.sql.catalyst.analysis.Analyzer.$anonfun$executeAndCheck$1(Analyzer.scala:174)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.markInAnalyzer(AnalysisHelper.scala:228)
	at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:173)
	at org.apache.spark.sql.execution.QueryExecution.$anonfun$analyzed$1(QueryExecution.scala:73)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
	at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:143)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:772)
	at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:143)
	at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:73)
	at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:71)
	at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:63)
	at org.apache.spark.sql.Dataset$.$anonfun$ofRows$2(Dataset.scala:98)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:772)
	at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:96)
	at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:615)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:772)
	at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:610)
	at com.nonpool.scala.ScalaUDF$.returnJavaNormalClassInUDF(ScalaUDF.scala:42)
	at com.nonpool.scala.ScalaUDF$.main(ScalaUDF.scala:18)
	at com.nonpool.scala.ScalaUDF.main(ScalaUDF.scala)
   */
  def returnJavaNormalClassInUDF(spark: SparkSession): Unit = {
    // if not use `Encoders.bean(classOf[NormalPOJO]).schema`, throw exception:
		// java.lang.UnsupportedOperationException: Schema for type com.nonpool.java.NormalPOJO is not supported
    spark.udf.register("normal", (str: String) => new NormalPOJO(str, 19), Encoders.bean(classOf[NormalPOJO]).schema)

    spark.sql("select man(value) as a from test").show()
  }


	/**
	 * show error result:
+---+
|  a|
+---+
| {}|
| {}|
+---+
	 */
	def returnCaseClassWithSchemaInUDF(spark: SparkSession): Unit = {
		spark.udf.register("stu1", (str: String) => Student(str, 19), Encoders.bean(classOf[Student]).schema)

		spark.sql("select stu1(value) as a from test").show()
	}

}


case class Student(name: String, age: Int)