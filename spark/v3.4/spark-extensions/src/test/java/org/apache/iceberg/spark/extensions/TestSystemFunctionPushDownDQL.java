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
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.createPartitionedTable;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.createUnpartitionedTable;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.days;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.hours;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.months;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.years;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
  public void testConstantExpression() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql("SELECT system.bucket(2, 5) AS bucket");
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain("applyfunctionexpression");
        });
  }

  @Test
  public void testYearsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testYearsFunction();
  }

  @Test
  public void testYearsFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "years(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testYearsFunction();
  }

  private void testYearsFunction() {
    String date = "2017-11-22";
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.years(ts) = system.years(date('%s')) ORDER BY id",
            tableName, date);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.YearsFunction$TimestampToYearsFunction");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=ts = " + years(date));

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testMonthsFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testMonthsFunction();
  }

  @Test
  public void testMonthsFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "months(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testMonthsFunction();
  }

  private void testMonthsFunction() {
    String date = "2017-11-22";
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.months(ts) > system.months(date('%s')) ORDER BY id",
            tableName, date);
    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.MonthsFunction$TimestampToMonthsFunction");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=ts > " + months(date));

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testDaysFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testDaysFunction();
  }

  @Test
  public void testDaysFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "days(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testDaysFunction();
  }

  private void testDaysFunction() {
    String date = "2018-11-20";
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.days(ts) < system.days(date('%s')) ORDER BY id",
            tableName, date);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.DaysFunction$TimestampToDaysFunction");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=ts < " + days(date));

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testHoursFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testHoursFunction();
  }

  @Test
  public void testHoursFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "hours(ts)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testHoursFunction();
  }

  private void testHoursFunction() {
    String ts = "2017-11-22T06:02:09.243857+00:00";
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.hours(ts) >= system.hours(timestamp('%s')) ORDER BY id",
            tableName, ts);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.HoursFunction$TimestampToHoursFunction");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=ts >= " + hours(ts));

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testBucketLongFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketLongFunction();
  }

  @Test
  public void testBucketLongFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "bucket(5, id)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketLongFunction();
  }

  private void testBucketLongFunction() {
    int target = 2;
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.bucket(5, id) <= %s ORDER BY id", tableName, target);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketLong");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=id <= " + target);

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testBucketStringFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketStringFunction();
  }

  @Test
  public void testBucketStringFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "bucket(5, data)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testBucketStringFunction();
  }

  private void testBucketStringFunction() {
    int target = 2;
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.bucket(5, data) != %s ORDER BY id", tableName, target);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketString");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=NOT (data = " + target + ")");

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }

  @Test
  public void testTruncateFunctionOnUnpartitionedTable() {
    createUnpartitionedTable(spark, tableName, ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testTruncateFunction();
  }

  @Test
  public void testTruncateFunctionOnPartitionedTable() {
    createPartitionedTable(
        spark, tableName, "truncate(4, data)", ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat));
    testTruncateFunction();
  }

  private void testTruncateFunction() {
    String target = "data";
    String query =
        sqlFormat(
            "SELECT * FROM %s WHERE system.truncate(4, data) = '%s' ORDER BY id",
            tableName, target);

    // system function push down disabled
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED, "true"),
        () -> {
          Dataset<Row> df = spark.sql(query);
          Assertions.assertThat(df.queryExecution().optimizedPlan().toString())
              .doesNotContain(
                  "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateString");
          Assertions.assertThat(df.queryExecution().sparkPlan().toString())
              .contains("filters=data = '" + target + "'");

          List<Object[]> actual = sql(query);
          assertEquals("Select result should be matched", expected, actual);
        });
  }
}
