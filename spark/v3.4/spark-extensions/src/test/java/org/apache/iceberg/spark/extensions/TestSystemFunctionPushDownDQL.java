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

import static org.apache.iceberg.spark.SystemFunPushDownHelper.createPartitionedTable;
import static org.apache.iceberg.spark.SystemFunPushDownHelper.days;
import static org.apache.iceberg.spark.SystemFunPushDownHelper.hours;
import static org.apache.iceberg.spark.SystemFunPushDownHelper.months;
import static org.apache.iceberg.spark.SystemFunPushDownHelper.years;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestSystemFunctionPushDownDQL extends SparkExtensionsTestBase {
  public TestSystemFunctionPushDownDQL(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName);
  }

  @Test
  public void testYearsFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "years(ts)");

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
  public void testMonthsFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "months(ts)");

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
  public void testDaysFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "days(ts)");

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
  public void testHoursFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "hours(ts)");

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
  public void testBucketLongFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "bucket(5, id)");

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
  public void testBucketStringFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "bucket(5, data)");

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
  public void testTruncateFunction() {
    Assume.assumeTrue(!catalogName.equals("spark_catalog"));

    createPartitionedTable(spark, tableName, "truncate(4, data)");

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

  private String sqlFormat(String format, Object... args) {
    return String.format(format, args);
  }
}
