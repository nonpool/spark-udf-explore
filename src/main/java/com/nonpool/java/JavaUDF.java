package com.nonpool.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

public class JavaUDF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("udf java test")
                .master("local[*]")
                .config("spark.ui.enabled", false)
                .getOrCreate();

        final Dataset<String> dataset = spark.createDataset(
                Arrays.asList("a", "b"),
                Encoders.STRING()
        );

        dataset.toDF().createOrReplaceTempView("test");

        returnNormalPOJOInUDF(spark);

        spark.stop();
    }



    /**
     *  got exception:
     *
     * Exception in thread "main" java.lang.IllegalArgumentException: The value (com.nonpool.java.NormalPOJO@687a0e40) of the type (com.nonpool.java.NormalPOJO) cannot be converted to struct<age:int,name:string>
     * 	at org.apache.spark.sql.catalyst.CatalystTypeConverters$StructConverter.toCatalystImpl(CatalystTypeConverters.scala:268)
     * 	at org.apache.spark.sql.catalyst.CatalystTypeConverters$StructConverter.toCatalystImpl(CatalystTypeConverters.scala:242)
     * 	at org.apache.spark.sql.catalyst.CatalystTypeConverters$CatalystTypeConverter.toCatalyst(CatalystTypeConverters.scala:107)
     * 	at org.apache.spark.sql.catalyst.CatalystTypeConverters$.$anonfun$createToCatalystConverter$2(CatalystTypeConverters.scala:426)
     * 	at org.apache.spark.sql.catalyst.expressions.ScalaUDF.eval(ScalaUDF.scala:1196)
     * 	at org.apache.spark.sql.catalyst.expressions.Alias.eval(namedExpressions.scala:160)
     * 	at org.apache.spark.sql.catalyst.expressions.InterpretedMutableProjection.apply(InterpretedMutableProjection.scala:97)
     * 	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$$anonfun$apply$19.$anonfun$applyOrElse$71(Optimizer.scala:1591)
     * 	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
     * 	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
     * 	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
     * 	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
     * 	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
     * 	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
     * 	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
     * 	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$$anonfun$apply$19.applyOrElse(Optimizer.scala:1591)
     * 	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$$anonfun$apply$19.applyOrElse(Optimizer.scala:1586)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$1(TreeNode.scala:317)
     * 	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:73)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:317)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:171)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:169)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:322)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:407)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:243)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:405)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:358)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:322)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:171)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:169)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:322)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:407)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:243)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:405)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:358)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:322)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:171)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:169)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDown$3(TreeNode.scala:322)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$mapChildren$1(TreeNode.scala:407)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:243)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:405)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:358)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:322)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown(AnalysisHelper.scala:171)
     * 	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDown$(AnalysisHelper.scala:169)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDown(LogicalPlan.scala:29)
     * 	at org.apache.spark.sql.catalyst.trees.TreeNode.transform(TreeNode.scala:306)
     * 	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$.apply(Optimizer.scala:1586)
     * 	at org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation$.apply(Optimizer.scala:1585)
     * 	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$2(RuleExecutor.scala:216)
     * 	at scala.collection.IndexedSeqOptimized.foldLeft(IndexedSeqOptimized.scala:60)
     * 	at scala.collection.IndexedSeqOptimized.foldLeft$(IndexedSeqOptimized.scala:68)
     * 	at scala.collection.mutable.WrappedArray.foldLeft(WrappedArray.scala:38)
     * 	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:213)
     * 	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1$adapted(RuleExecutor.scala:205)
     * 	at scala.collection.immutable.List.foreach(List.scala:431)
     * 	at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:205)
     * 	at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$executeAndTrack$1(RuleExecutor.scala:183)
     * 	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:88)
     * 	at org.apache.spark.sql.catalyst.rules.RuleExecutor.executeAndTrack(RuleExecutor.scala:183)
     * 	at org.apache.spark.sql.execution.QueryExecution.$anonfun$optimizedPlan$1(QueryExecution.scala:87)
     * 	at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
     * 	at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:143)
     * 	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:772)
     * 	at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:143)
     * 	at org.apache.spark.sql.execution.QueryExecution.optimizedPlan$lzycompute(QueryExecution.scala:84)
     * 	at org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:84)
     * 	at org.apache.spark.sql.execution.QueryExecution.assertOptimized(QueryExecution.scala:95)
     * 	at org.apache.spark.sql.execution.QueryExecution.executedPlan$lzycompute(QueryExecution.scala:113)
     * 	at org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:110)
     * 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$5(SQLExecution.scala:101)
     * 	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:163)
     * 	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:90)
     * 	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:772)
     * 	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
     * 	at org.apache.spark.sql.Dataset.withAction(Dataset.scala:3685)
     * 	at org.apache.spark.sql.Dataset.head(Dataset.scala:2722)
     * 	at org.apache.spark.sql.Dataset.take(Dataset.scala:2929)
     * 	at org.apache.spark.sql.Dataset.getRows(Dataset.scala:301)
     * 	at org.apache.spark.sql.Dataset.showString(Dataset.scala:338)
     * 	at org.apache.spark.sql.Dataset.show(Dataset.scala:825)
     * 	at org.apache.spark.sql.Dataset.show(Dataset.scala:784)
     * 	at org.apache.spark.sql.Dataset.show(Dataset.scala:793)
     * 	at com.nonpool.java.JavaUDF.useNormalPOJOInUDF(JavaUDF.java:41)
     * 	at com.nonpool.java.JavaUDF.main(JavaUDF.java:18)
     */
    private static void returnNormalPOJOInUDF(SparkSession spark) {

        spark.udf().register("toNormalClass", new UDF2<String, Integer, NormalPOJO>() {
            @Override
            public NormalPOJO call(String s, Integer integer) throws Exception {
                return new NormalPOJO(s, integer + 1);
            }
        }, Encoders.bean(NormalPOJO.class).schema());


        spark.sql("select toNormalClass(name, 18) from test").show();
        // other: https://forums.databricks.com/questions/13361/how-do-i-create-a-udf-in-java-which-return-a-compl.html
        // https://issues.apache.org/jira/browse/SPARK-29009
    }

}
