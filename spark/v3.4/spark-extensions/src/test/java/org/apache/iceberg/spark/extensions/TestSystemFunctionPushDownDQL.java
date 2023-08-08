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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
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
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.days;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.hours;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.months;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.years;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.functions.UnboundFunctionWrapper;
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
  private final String fileFormat;

  public TestSystemFunctionPushDownDQL(
      String catalogName, String implementation, Map<String, String> config, String fileFormat) {
    super(catalogName, implementation, config);
    this.fileFormat = fileFormat;
  }

  @Parameterized.Parameters(
      name = "catalogName = {0}, implementation = {1}, config = {2}, fileFormat = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        "parquet"
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        "orc"
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        "avro"
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
  public void testFoldingConstantExpression() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          String ts = "2017-11-22T09:20:44.294658+00:00";
          Dataset<Row> yearsDF =
              spark.sql(String.format("SELECT system.years(timestamp('%s')) AS value", ts));
          assertExpressionConstantFold(yearsDF);
          Assertions.assertThat(rowsToJava(yearsDF.collectAsList()).get(0)[0]).isEqualTo(47);

          Dataset<Row> monthsDF =
              spark.sql(String.format("SELECT system.months(timestamp('%s')) AS value", ts));
          assertExpressionConstantFold(monthsDF);
          Assertions.assertThat(rowsToJava(monthsDF.collectAsList()).get(0)[0]).isEqualTo(574);

          Dataset<Row> daysDF =
              spark.sql(String.format("SELECT system.days(timestamp('%s')) AS value", ts));
          assertExpressionConstantFold(daysDF);
          Assertions.assertThat(rowsToJava(daysDF.collectAsList()).get(0)[0])
              .isEqualTo(Date.valueOf("2017-11-22"));

          Dataset<Row> hoursDF =
              spark.sql(String.format("SELECT system.hours(timestamp('%s')) AS value", ts));
          assertExpressionConstantFold(hoursDF);
          Assertions.assertThat(rowsToJava(hoursDF.collectAsList()).get(0)[0]).isEqualTo(419817);

          Dataset<Row> bucketDF =
              spark.sql(String.format("SELECT system.bucket(2, '%s') AS value", ts));
          assertExpressionConstantFold(bucketDF);
          Assertions.assertThat(rowsToJava(bucketDF.collectAsList()).get(0)[0]).isEqualTo(0);

          Dataset<Row> truncateDF =
              spark.sql(String.format("SELECT system.truncate(2, '%s') AS value", ts));
          assertExpressionConstantFold(truncateDF);
          Assertions.assertThat(rowsToJava(truncateDF.collectAsList()).get(0)[0]).isEqualTo("20");
        });
  }

  @Test
  public void testYearsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testYearsFunction(false);
  }

  @Test
  public void testYearsFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "years(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testYearsFunction(true);
  }

  private void testYearsFunction(boolean partitioned) {
    int targetYears = years("2017-11-22");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.years(ts) = %s ORDER BY id", tableName, targetYears);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "years");
          checkPushedFilters(optimizedPlan, equal(year("ts"), targetYears));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testMonthsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testMonthsFunction(false);
  }

  @Test
  public void testMonthsFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "months(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testMonthsFunction(true);
  }

  private void testMonthsFunction(boolean partitioned) {
    int targetMonths = months("2017-11-22");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.months(ts) > %s ORDER BY id", tableName, targetMonths);
    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "months");
          checkPushedFilters(optimizedPlan, greaterThan(month("ts"), targetMonths));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testDaysFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testDaysFunction(false);
  }

  @Test
  public void testDaysFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "days(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testDaysFunction(true);
  }

  private void testDaysFunction(boolean partitioned) {
    String date = "2018-11-20";
    int targetDays = days(date);
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.days(ts) < date('%s') ORDER BY id", tableName, date);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "days");
          checkPushedFilters(optimizedPlan, lessThan(day("ts"), targetDays));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testHoursFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testHoursFunction(false);
  }

  @Test
  public void testHoursFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "hours(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testHoursFunction(true);
  }

  private void testHoursFunction(boolean partitioned) {
    int targetHours = hours("2017-11-22T06:02:09.243857+00:00");
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.hours(ts) >= %s ORDER BY id", tableName, targetHours);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "hours");
          checkPushedFilters(optimizedPlan, greaterThanOrEqual(hour("ts"), targetHours));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testBucketLongFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketLongFunction(false);
  }

  @Test
  public void testBucketLongFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "bucket(5, id)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketLongFunction(true);
  }

  private void testBucketLongFunction(boolean partitioned) {
    int target = 2;
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, id) <= %s ORDER BY id", tableName, target);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "bucket");
          checkPushedFilters(optimizedPlan, lessThanOrEqual(bucket("id", 5), target));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testBucketStringFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketStringFunction(false);
  }

  @Test
  public void testBucketStringFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "bucket(5, data)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketStringFunction(true);
  }

  private void testBucketStringFunction(boolean partitioned) {
    int target = 2;
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.bucket(5, data) != %s ORDER BY id", tableName, target);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "bucket");
          checkPushedFilters(optimizedPlan, notEqual(bucket("data", 5), target));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testTruncateFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testTruncateFunction(false);
  }

  @Test
  public void testTruncateFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "truncate(4, data)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testTruncateFunction(true);
  }

  private void testTruncateFunction(boolean partitioned) {
    String target = "data";
    String query =
        String.format(
            "SELECT * FROM %s WHERE system.truncate(4, data) = '%s' ORDER BY id",
            tableName, target);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          LogicalPlan optimizedPlan = df.queryExecution().optimizedPlan();

          checkExpressions(optimizedPlan, partitioned, "truncate");
          checkPushedFilters(optimizedPlan, equal(truncate("data", 4), target));

          List<Object[]> actual = rowsToJava(df.collectAsList());
          assertEquals("Select result should be matched", expected, actual);
        });
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
      Assertions.assertThat(expression.function())
          .isInstanceOf(UnboundFunctionWrapper.SystemFunctionWrapper.class);
      Assertions.assertThat(expression.name()).isEqualTo(expectedFunctionName);
    }
  }

  private void checkPushedFilters(
      LogicalPlan optimizedPlan, org.apache.iceberg.expressions.Expression expected) {
    List<org.apache.iceberg.expressions.Expression> pushedFilters =
        PlanUtils.getScanPushDownFilters(optimizedPlan);
    Assertions.assertThat(pushedFilters.size()).isEqualTo(1);
    org.apache.iceberg.expressions.Expression actual = pushedFilters.get(0);
    Assertions.assertThat(ExpressionUtil.equivalent(expected, actual, STRUCT, true)).isTrue();
  }

  private void assertExpressionConstantFold(Dataset<Row> df) {
    List<Expression> stackInvokes =
        PlanUtils.collectSparkExpressions(
            df.queryExecution().optimizedPlan(), plan -> plan instanceof StaticInvoke);
    Assertions.assertThat(stackInvokes).isEmpty();

    List<Expression> applyFunctions =
        PlanUtils.collectSparkExpressions(
            df.queryExecution().optimizedPlan(), plan -> plan instanceof ApplyFunctionExpression);
    Assertions.assertThat(applyFunctions).isEmpty();
  }
}
