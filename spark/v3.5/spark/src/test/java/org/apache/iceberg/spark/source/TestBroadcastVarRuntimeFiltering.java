/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.LongType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.dynamicpruning.PartitionPruning;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.ObjectType;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;

public class TestBroadcastVarRuntimeFiltering extends TestRuntimeFiltering {

  private final Map<String, Expression> runtimeFilterExpressions = new HashMap<>();

  @BeforeEach
  @Override
  void init(TestInfo testInfo) {
    this.testInfo = testInfo;
    spark.conf().set(SQLConf.OPTIMIZER_EXCLUDED_RULES().key(), PartitionPruning.ruleName());
    spark.conf().set(SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN().key(), "true");
    spark.conf().set(SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP().key(), "true");
  }

  @BeforeAll
  @Override
  public void populateFilterMap() {
    runtimeFilterExpressions.put(
        "testIdentityPartitionedTable",
        new BroadcastHRUnboundPredicate<>(
            "date", new DummyBroadcastedJoinKeysWrapper(DateType, new Object[] {1}, 1)));
    runtimeFilterExpressions.put(
        "testBucketedTable",
        new BroadcastHRUnboundPredicate<>(
            "id", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));

    runtimeFilterExpressions.put(
        "testRenamedSourceColumnTable",
        new BroadcastHRUnboundPredicate<>(
            "row_id", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));

    runtimeFilterExpressions.put(
        "testMultipleRuntimeFilters",
        new BroadcastHRUnboundPredicate<>(
            "id", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));
    runtimeFilterExpressions.put(
        "testCaseSensitivityOfRuntimeFilters",
        new BroadcastHRUnboundPredicate<>(
            "id", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));

    runtimeFilterExpressions.put(
        "testBucketedTableWithMultipleSpecs",
        new BroadcastHRUnboundPredicate<>(
            "id", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));
    runtimeFilterExpressions.put(
        "testSourceColumnWithDots",
        new BroadcastHRUnboundPredicate<>(
            "i.d", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));
    runtimeFilterExpressions.put(
        "testSourceColumnWithBackticks",
        new BroadcastHRUnboundPredicate<>(
            "i`d", new DummyBroadcastedJoinKeysWrapper(LongType, new Object[] {1L}, 1)));
  }

  @Override
  @AfterEach
  public void removeTables() {
    super.removeTables();
    spark.conf().unset(SQLConf.OPTIMIZER_EXCLUDED_RULES().key());
    spark.conf().unset(SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN().key());
    spark.conf().unset(SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP().key());
  }

  @Override
  Expression getFileDeletionFilter() {
    return this.runtimeFilterExpressions.get(tesMethodName());
  }

  @Override
  void assertQueryContainsDataFilters(String query, int expectedFilterCount, String errorMessage) {
    SparkPlan sp = spark.sql(query).queryExecution().executedPlan();
    if (sp instanceof AdaptiveSparkPlanExec) {
      sp = ((AdaptiveSparkPlanExec) sp).finalPhysicalPlan();
    }
    String runtimeStrs = getConcatenatedDataFilterString(sp.toString());
    int actualFilterCount = StringUtils.countMatches(runtimeStrs, "RANGE IN (");
    Assert.assertEquals(errorMessage, expectedFilterCount, actualFilterCount);
  }

  @Override
  void assertQueryContainsRuntimeFilters(
      String query, int expectedFilterCount, String errorMessage) {
    SparkPlan sp = spark.sql(query).queryExecution().executedPlan();
    if (sp instanceof AdaptiveSparkPlanExec) {
      sp = ((AdaptiveSparkPlanExec) sp).finalPhysicalPlan();
    }
    String runtimeStrs = getConcatenatedRuntimeFilterString(sp.toString());
    int actualFilterCount = StringUtils.countMatches(runtimeStrs, "RANGE IN (");
    Assert.assertEquals(errorMessage, expectedFilterCount, actualFilterCount);
  }

  private String getConcatenatedDataFilterString(String planStr) {
    String start = "filters=";
    String endStr = "runtimeFilters=";
    List<String> total = new LinkedList<>();

    int startIndex = planStr.indexOf(start);
    while (startIndex != -1) {
      int endIndex = planStr.indexOf(endStr, startIndex);
      total.add(planStr.substring(startIndex, endIndex));
      startIndex = planStr.indexOf(start, endIndex);
    }
    return String.join("", total);
  }

  private String getConcatenatedRuntimeFilterString(String planStr) {
    String start = "runtimeFilters=";
    String endStr = "caseSensitive=";
    List<String> total = new LinkedList<>();

    int startIndex = planStr.indexOf(start);
    while (startIndex != -1) {
      int endIndex = planStr.indexOf(endStr, startIndex);
      total.add(planStr.substring(startIndex, endIndex));
      startIndex = planStr.indexOf(start, endIndex);
    }
    return String.join("", total);
  }

  @Override
  boolean isBroadcastVarPushDownTest() {
    return true;
  }

  @TestTemplate
  public void testBroadcastVarDataFilterSegregation() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP, testCol INT) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, id))",
        tableName);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .withColumn("testCol", expr("CAST(id % 8 as INT) * 100 + CAST(id  as INT)"))
            .select("id", "data", "date", "ts", "testCol");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    // fire a simple query to get handle of SparkBatchQueryScan
    String query = String.format("SELECT * FROM %s ", tableName);
    Dataset<Row> qDf = spark.sql(query);
    SparkPlan sp = qDf.queryExecution().sparkPlan();
    SparkBatchQueryScan sbq =
        (SparkBatchQueryScan) ((BatchScanExec) sp.collectLeaves().head()).scan();

    // Now push a BroadcastFilter on testColumn such that it has min value 303 and max value 380
    // Though it is a non partition column filter , but since it is on broadcast var, it should be
    // able to trim
    // down the tasks created and also it should be part of the data filters when the tasks are
    // serialized to the
    // executors
    // get number of FileScanTasks before pushdown
    int numFileScanTasksBefore = sbq.getNumFileScanTasks();

    Object actualData =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.IntegerType, new Object[] {1, 25, 40}, 1L);
    ObjectType dt = ObjectType.apply(BroadcastedJoinKeysWrapper.class);
    Literal embedAsLiteral = Literal$.MODULE$.create(actualData, dt);
    In filter = In.apply("testCol", new Object[] {embedAsLiteral});

    ((SupportsRuntimeV2Filtering) sbq).filter(new Predicate[] {filter.toV2()});
    // after filter pushdown it should be 1
    int numFileScanTasksAfter = sbq.getNumFileScanTasks();
    assert (numFileScanTasksAfter < numFileScanTasksBefore);
  }
}
