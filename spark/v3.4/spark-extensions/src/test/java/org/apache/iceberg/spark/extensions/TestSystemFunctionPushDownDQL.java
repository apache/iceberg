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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.STRUCT;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.createPartitionedTable;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.createUnpartitionedTable;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToDayOrdinal;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToHourOrdinal;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToMonthOrdinal;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToYearOrdinal;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.PlanUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestSystemFunctionPushDownDQL extends SparkExtensionsTestBase {
  public TestSystemFunctionPushDownDQL(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
      },
    };
  }

  @Before
  public void before() {
    sql("USE %s", catalogName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testYearsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testYearsFunction(false);
  }

  @Test
  public void testYearsFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "years(ts)");
    testYearsFunction(true);
  }

  private void testYearsFunction(boolean partitioned) {
    int targetYears = timestampStrToYearOrdinal("2017-11-22T00:00:00.000000+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.years(ts) = %s ORDER BY id", tableName, targetYears);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "years");
    checkPushedFilters(optimizedPlan, equal(year("ts"), targetYears));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(5);
  }

  @Test
  public void testMonthsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testMonthsFunction(false);
  }

  @Test
  public void testMonthsFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "months(ts)");
    testMonthsFunction(true);
  }

  private void testMonthsFunction(boolean partitioned) {
    int targetMonths = timestampStrToMonthOrdinal("2017-11-22T00:00:00.000000+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.months(ts) > %s ORDER BY id", tableName, targetMonths);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "months");
    checkPushedFilters(optimizedPlan, greaterThan(month("ts"), targetMonths));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(5);
  }

  @Test
  public void testDaysFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testDaysFunction(false);
  }

  @Test
  public void testDaysFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "days(ts)");
    testDaysFunction(true);
  }

  private void testDaysFunction(boolean partitioned) {
    String timestamp = "2018-11-20T00:00:00.000000+00:00";
    int targetDays = timestampStrToDayOrdinal(timestamp);
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.days(ts) < date('%s') ORDER BY id",
            tableName, timestamp);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "days");
    checkPushedFilters(optimizedPlan, lessThan(day("ts"), targetDays));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(5);
  }

  @Test
  public void testHoursFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testHoursFunction(false);
  }

  @Test
  public void testHoursFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "hours(ts)");
    testHoursFunction(true);
  }

  private void testHoursFunction(boolean partitioned) {
    int targetHours = timestampStrToHourOrdinal("2017-11-22T06:02:09.243857+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.hours(ts) >= %s ORDER BY id", tableName, targetHours);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "hours");
    checkPushedFilters(optimizedPlan, greaterThanOrEqual(hour("ts"), targetHours));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(8);
  }

  @Test
  public void testBucketLongFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testBucketLongFunction(false);
  }

  @Test
  public void testBucketLongFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "bucket(5, id)");
    testBucketLongFunction(true);
  }

  private void testBucketLongFunction(boolean partitioned) {
    int target = 2;
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, id) <= %s ORDER BY id", tableName, target);
    checkQueryExecution(query, partitioned, lessThanOrEqual(bucket("id", 5), target));
  }

  @Test
  public void testBucketLongFunctionInClauseOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testBucketLongFunctionInClause(false);
  }

  @Test
  public void testBucketLongFunctionInClauseOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "bucket(5, id)");
    testBucketLongFunctionInClause(true);
  }

  private void testBucketLongFunctionInClause(boolean partitioned) {
    List<Integer> inValues = IntStream.range(0, 3).boxed().collect(Collectors.toList());
    String inValuesAsSql =
        inValues.stream().map(x -> Integer.toString(x)).collect(Collectors.joining(", "));
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, id) IN (%s) ORDER BY id",
            tableName, inValuesAsSql);

    checkQueryExecution(query, partitioned, in(bucket("id", 5), inValues.toArray()));
  }

  private void checkQueryExecution(
      String query, boolean partitioned, org.apache.iceberg.expressions.Expression expression) {
    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "bucket");
    checkPushedFilters(optimizedPlan, expression);

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(5);
  }

  @Test
  public void testBucketLongFunctionIsNotReplacedWhenArgumentsAreNotLiteralsOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "bucket(5, id)");
    testBucketLongFunctionIsNotReplacedWhenArgumentsAreNotLiterals();
  }

  @Test
  public void testBucketLongFunctionIsNotReplacedWhenArgumentsAreNotLiteralsOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testBucketLongFunctionIsNotReplacedWhenArgumentsAreNotLiterals();
  }

  private void testBucketLongFunctionIsNotReplacedWhenArgumentsAreNotLiterals() {
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, id) IN (system.bucket(5, id), 1) ORDER BY id",
            tableName);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressionsNotReplaced(
        optimizedPlan, "org.apache.iceberg.spark.functions.BucketFunction$BucketLong", 2);
    checkNotPushedFilters(optimizedPlan);

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(10);
  }

  @Test
  public void testBucketStringFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testBucketStringFunction(false);
  }

  @Test
  public void testBucketStringFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "bucket(5, data)");
    testBucketStringFunction(true);
  }

  private void testBucketStringFunction(boolean partitioned) {
    int target = 2;
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, data) != %s ORDER BY id", tableName, target);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "bucket");
    checkPushedFilters(optimizedPlan, notEqual(bucket("data", 5), target));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(8);
  }

  @Test
  public void testTruncateFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testTruncateFunction(false);
  }

  @Test
  public void testTruncateFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "truncate(4, data)");
    testTruncateFunction(true);
  }

  private void testTruncateFunction(boolean partitioned) {
    String target = "data";
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.truncate(4, data) = '%s' ORDER BY id",
            tableName, target);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, partitioned, "truncate");
    checkPushedFilters(optimizedPlan, equal(truncate("data", 4), target));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    Assertions.assertThat(actual.size()).isEqualTo(5);
  }

  private void checkExpressions(
      LogicalPlan optimizedPlan, boolean partitioned, String expectedFunctionName) {
    List<Expression> staticInvokes =
        PlanUtils.collectSparkExpressions(
            optimizedPlan, expression -> expression instanceof StaticInvoke);
    Assertions.assertThat(staticInvokes).isEmpty();

    List<Expression> applyExpressions =
        PlanUtils.collectSparkExpressions(
            optimizedPlan, expression -> expression instanceof ApplyFunctionExpression);

    if (partitioned) {
      Assertions.assertThat(applyExpressions).isEmpty();
    } else {
      Assertions.assertThat(applyExpressions.size()).isEqualTo(1);
      ApplyFunctionExpression expression = (ApplyFunctionExpression) applyExpressions.get(0);
      Assertions.assertThat(expression.name()).isEqualTo(expectedFunctionName);
    }
  }

  private void checkPushedFilters(
      LogicalPlan optimizedPlan, org.apache.iceberg.expressions.Expression expected) {
    List<org.apache.iceberg.expressions.Expression> pushedFilters =
        PlanUtils.collectPushDownFilters(optimizedPlan);
    Assertions.assertThat(pushedFilters.size()).isEqualTo(1);
    org.apache.iceberg.expressions.Expression actual = pushedFilters.get(0);
    Assertions.assertThat(ExpressionUtil.equivalent(expected, actual, STRUCT, true))
        .as("Pushed filter should match")
        .isTrue();
  }

  private void checkExpressionsNotReplaced(
      LogicalPlan optimizedPlan, String expectedFunctionName, int expectedFunctionCount) {
    List<StaticInvoke> staticInvokes =
        PlanUtils.collectSparkExpressions(
                optimizedPlan, expression -> expression instanceof StaticInvoke)
            .stream()
            .map(x -> (StaticInvoke) x)
            .collect(Collectors.toList());

    Assertions.assertThat(staticInvokes.size()).isEqualTo(expectedFunctionCount);
    Assertions.assertThat(staticInvokes)
        .allSatisfy(
            e -> Assertions.assertThat(e.staticObject().getName()).isEqualTo(expectedFunctionName));
  }

  private void checkNotPushedFilters(LogicalPlan optimizedPlan) {
    List<org.apache.iceberg.expressions.Expression> pushedFilters =
        PlanUtils.collectPushDownFilters(optimizedPlan);
    Assertions.assertThat(pushedFilters.size()).isEqualTo(0);
  }
}
