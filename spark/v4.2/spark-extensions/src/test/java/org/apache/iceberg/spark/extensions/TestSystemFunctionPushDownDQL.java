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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.PlanUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSystemFunctionPushDownDQL extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
      },
    };
  }

  @BeforeEach
  public void before() {
    super.before();
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testYearsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testYearsFunction();
  }

  @TestTemplate
  public void testYearsFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "years(ts)");
    testYearsFunction();
  }

  private void testYearsFunction() {
    int targetYears = timestampStrToYearOrdinal("2017-11-22T00:00:00.000000+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.years(ts) = %s ORDER BY id", tableName, targetYears);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "years");
    checkPushedFilters(optimizedPlan, equal(year("ts"), targetYears));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(5);
  }

  @TestTemplate
  public void testMonthsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testMonthsFunction();
  }

  @TestTemplate
  public void testMonthsFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "months(ts)");
    testMonthsFunction();
  }

  private void testMonthsFunction() {
    int targetMonths = timestampStrToMonthOrdinal("2017-11-22T00:00:00.000000+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.months(ts) > %s ORDER BY id", tableName, targetMonths);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "months");
    checkPushedFilters(optimizedPlan, greaterThan(month("ts"), targetMonths));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(5);
  }

  @TestTemplate
  public void testDaysFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testDaysFunction();
  }

  @TestTemplate
  public void testDaysFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "days(ts)");
    testDaysFunction();
  }

  private void testDaysFunction() {
    String timestamp = "2018-11-20T00:00:00.000000+00:00";
    int targetDays = timestampStrToDayOrdinal(timestamp);
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.days(ts) < date('%s') ORDER BY id",
            tableName, timestamp);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "days");
    checkPushedFilters(optimizedPlan, lessThan(day("ts"), targetDays));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(5);
  }

  @TestTemplate
  public void testHoursFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testHoursFunction();
  }

  @TestTemplate
  public void testHoursFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "hours(ts)");
    testHoursFunction();
  }

  private void testHoursFunction() {
    int targetHours = timestampStrToHourOrdinal("2017-11-22T06:02:09.243857+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.hours(ts) >= %s ORDER BY id", tableName, targetHours);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "hours");
    checkPushedFilters(optimizedPlan, greaterThanOrEqual(hour("ts"), targetHours));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(8);
  }

  @TestTemplate
  public void testBucketLongFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testBucketLongFunction();
  }

  @TestTemplate
  public void testBucketLongFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "bucket(5, id)");
    testBucketLongFunction();
  }

  private void testBucketLongFunction() {
    int target = 2;
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, id) <= %s ORDER BY id", tableName, target);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "bucket");
    checkPushedFilters(optimizedPlan, lessThanOrEqual(bucket("id", 5), target));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(5);
  }

  @TestTemplate
  public void testBucketStringFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testBucketStringFunction();
  }

  @TestTemplate
  public void testBucketStringFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "bucket(5, data)");
    testBucketStringFunction();
  }

  private void testBucketStringFunction() {
    int target = 2;
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, data) != %s ORDER BY id", tableName, target);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "bucket");
    checkPushedFilters(optimizedPlan, notEqual(bucket("data", 5), target));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(8);
  }

  @TestTemplate
  public void testTruncateFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName);
    testTruncateFunction();
  }

  @TestTemplate
  public void testTruncateFunctionOnPartitionedTable() {
    createPartitionedTable(spark, tableName, "truncate(4, data)");
    testTruncateFunction();
  }

  private void testTruncateFunction() {
    String target = "data";
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.truncate(4, data) = '%s' ORDER BY id",
            tableName, target);

    Dataset<Row> df = spark.sql(query);
    LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

    checkExpressions(optimizedPlan, "truncate");
    checkPushedFilters(optimizedPlan, equal(truncate("data", 4), target));

    List<Object[]> actual = rowsToJava(df.collectAsList());
    assertThat(actual).hasSize(5);
  }

  private void checkExpressions(LogicalPlan optimizedPlan, String expectedFunctionName) {
    List<Expression> staticInvokes =
        PlanUtils.collectSparkExpressions(
            optimizedPlan, expression -> expression instanceof StaticInvoke);
    assertThat(staticInvokes).isEmpty();

    List<Expression> applyExpressions =
        PlanUtils.collectSparkExpressions(
            optimizedPlan, expression -> expression instanceof ApplyFunctionExpression);

    assertThat(applyExpressions).hasSize(1);
    ApplyFunctionExpression expression = (ApplyFunctionExpression) applyExpressions.get(0);
    assertThat(expression.name()).isEqualTo(expectedFunctionName);
  }

  private void checkPushedFilters(
      LogicalPlan optimizedPlan, org.apache.iceberg.expressions.Expression expected) {
    List<org.apache.iceberg.expressions.Expression> pushedFilters =
        PlanUtils.collectPushDownFilters(optimizedPlan);
    assertThat(pushedFilters).hasSize(1);
    org.apache.iceberg.expressions.Expression actual = pushedFilters.get(0);
    assertThat(ExpressionUtil.equivalent(expected, actual, STRUCT, true))
        .as("Pushed filter should match")
        .isTrue();
  }
}
