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

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.LocalDate;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.spark.functions.BucketFunction;
import org.apache.iceberg.spark.functions.DaysFunction;
import org.apache.iceberg.spark.functions.MonthsFunction;
import org.apache.iceberg.spark.functions.TruncateFunction;
import org.apache.iceberg.spark.functions.YearsFunction;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.UserDefinedScalarFunc;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilteredWithSystemFunctionScan extends SparkTestBaseWithCatalog {

  @Before
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @After
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName);
  }

  @Test
  public void testUnpartitionedYears() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction function = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(), function.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate =
        new Predicate(
            "=", expressions(udf, LiteralValue.apply(2017 - 1970, DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT Equal
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedYears() throws Exception {
    createPartitionedTable("years(ts)");

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction function = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(), function.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate =
        new Predicate(
            "=", expressions(udf, LiteralValue.apply(2017 - 1970, DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT Equal
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @Test
  public void testUnpartitionedMonths() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    MonthsFunction.TimestampToMonthsFunction function =
        new MonthsFunction.TimestampToMonthsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(), function.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate =
        new Predicate(
            ">", expressions(udf, LiteralValue.apply(months(2017, 11), DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT GT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedMonths() throws Exception {
    createPartitionedTable("months(ts)");

    SparkScanBuilder builder = scanBuilder();

    MonthsFunction.TimestampToMonthsFunction function =
        new MonthsFunction.TimestampToMonthsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(), function.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate =
        new Predicate(
            ">", expressions(udf, LiteralValue.apply(months(2017, 11), DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT GT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @Test
  public void testUnpartitionedDays() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    DaysFunction.TimestampToDaysFunction function = new DaysFunction.TimestampToDaysFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(), function.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate =
        new Predicate(
            "<", expressions(udf, LiteralValue.apply(days(2018, 11, 20), DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT LT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedDays() throws Exception {
    createPartitionedTable("days(ts)");

    SparkScanBuilder builder = scanBuilder();

    DaysFunction.TimestampToDaysFunction function = new DaysFunction.TimestampToDaysFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(), function.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate =
        new Predicate(
            "<", expressions(udf, LiteralValue.apply(days(2018, 11, 20), DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT LT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @Test
  public void testUnpartitionedBucketLong() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketLong function = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("id")));
    Predicate predicate =
        new Predicate(">=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT GTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedBucketLong() throws Exception {
    createPartitionedTable("bucket(5, id)");

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketLong function = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("id")));
    Predicate predicate =
        new Predicate(">=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(6);

    // NOT GTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(4);
  }

  @Test
  public void testUnpartitionedBucketString() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketString function = new BucketFunction.BucketString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate =
        new Predicate("<=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT LTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedBucketString() throws Exception {
    createPartitionedTable("bucket(5, data)");

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketString function = new BucketFunction.BucketString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate =
        new Predicate("<=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(6);

    // NOT LTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(4);
  }

  @Test
  public void testUnpartitionedTruncateString() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate =
        new Predicate("<>", expressions(udf, LiteralValue.apply("data", DataTypes.StringType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT NotEqual
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedTruncateString() throws Exception {
    createPartitionedTable("truncate(4, data)");

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate =
        new Predicate("<>", expressions(udf, LiteralValue.apply("data", DataTypes.StringType)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT NotEqual
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @Test
  public void testUnpartitionedIsNull() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate = new Predicate("IS_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT IsNull
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedIsNull() throws Exception {
    createPartitionedTable("truncate(4, data)");

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate = new Predicate("IS_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(0);

    // NOT IsNULL
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testUnpartitionedIsNotNull() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate = new Predicate("IS_NOT_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT IsNotNull
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedIsNotNull() throws Exception {
    createPartitionedTable("truncate(4, data)");

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            function.name(),
            function.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("data")));
    Predicate predicate = new Predicate("IS_NOT_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT IsNotNULL
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(0);
  }

  @Test
  public void testUnpartitionedAnd() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 =
        new UserDefinedScalarFunc(
            tsToYears.name(), tsToYears.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate1 =
        new Predicate(
            "=", expressions(udf1, LiteralValue.apply(2017 - 1970, DataTypes.IntegerType)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            bucketLong.name(),
            bucketLong.canonicalName(),
            expressions(LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("id")));
    Predicate predicate2 =
        new Predicate(">=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    Predicate predicate = new And(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT (years(ts) = 47 AND bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedAnd() throws Exception {
    createPartitionedTable("years(ts), bucket(5, id)");

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 =
        new UserDefinedScalarFunc(
            tsToYears.name(), tsToYears.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate1 =
        new Predicate(
            "=", expressions(udf1, LiteralValue.apply(2017 - 1970, DataTypes.IntegerType)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            bucketLong.name(),
            bucketLong.canonicalName(),
            expressions(LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("id")));
    Predicate predicate2 =
        new Predicate(">=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    Predicate predicate = new And(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(1);

    // NOT (years(ts) = 47 AND bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(9);
  }

  @Test
  public void testUnpartitionedOr() throws Exception {
    createUnpartitionedTable();

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 =
        new UserDefinedScalarFunc(
            tsToYears.name(), tsToYears.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate1 =
        new Predicate(
            "=", expressions(udf1, LiteralValue.apply(2017 - 1970, DataTypes.IntegerType)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            bucketLong.name(),
            bucketLong.canonicalName(),
            expressions(LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("id")));
    Predicate predicate2 =
        new Predicate(">=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    Predicate predicate = new Or(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT (years(ts) = 47 OR bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @Test
  public void testPartitionedOr() throws Exception {
    createPartitionedTable("years(ts), bucket(5, id)");

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 =
        new UserDefinedScalarFunc(
            tsToYears.name(), tsToYears.canonicalName(), expressions(FieldReference.apply("ts")));
    Predicate predicate1 =
        new Predicate(
            "=", expressions(udf1, LiteralValue.apply(2018 - 1970, DataTypes.IntegerType)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            bucketLong.name(),
            bucketLong.canonicalName(),
            expressions(LiteralValue.apply(5, DataTypes.IntegerType), FieldReference.apply("id")));
    Predicate predicate2 =
        new Predicate(">=", expressions(udf, LiteralValue.apply(2, DataTypes.IntegerType)));
    Predicate predicate = new Or(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(6);

    // NOT (years(ts) = 48 OR bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    Assertions.assertThat(scan.planInputPartitions().length).isEqualTo(4);
  }

  private SparkScanBuilder scanBuilder() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", tableName));

    return new SparkScanBuilder(spark, table, options);
  }

  private void createUnpartitionedTable() {
    sql("CREATE TABLE %s (id BIGINT, ts TIMESTAMP, data STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' %s)", tableName, "read.split.target-size", "10");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(0, CAST('2017-11-22T09:20:44.294658+00:00' AS TIMESTAMP), 'data-0')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(1, CAST('2017-11-22T07:15:34.582910+00:00' AS TIMESTAMP), 'data-1')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(2, CAST('2017-11-22T06:02:09.243857+00:00' AS TIMESTAMP), 'data-2')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(3, CAST('2017-11-22T03:10:11.134509+00:00' AS TIMESTAMP), 'data-3')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(4, CAST('2017-11-22T00:34:00.184671+00:00' AS TIMESTAMP), 'data-4')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(5, CAST('2018-12-21T22:20:08.935889+00:00' AS TIMESTAMP), 'material-5')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(6, CAST('2018-12-21T21:55:30.589712+00:00' AS TIMESTAMP), 'material-6')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(7, CAST('2018-12-21T17:31:14.532797+00:00' AS TIMESTAMP), 'material-7')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(8, CAST('2018-12-21T15:21:51.237521+00:00' AS TIMESTAMP), 'material-8')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(9, CAST('2018-12-21T15:02:15.230570+00:00' AS TIMESTAMP), 'material-9')");
  }

  private void createPartitionedTable(String partitionCol) {
    sql(
        "CREATE TABLE %s (id BIGINT, ts TIMESTAMP, data STRING) USING iceberg PARTITIONED BY (%s)",
        tableName, partitionCol);
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' %s)", tableName, "read.split.target-size", "10");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(0, CAST('2017-11-22T09:20:44.294658+00:00' AS TIMESTAMP), 'data-0')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(1, CAST('2017-11-22T07:15:34.582910+00:00' AS TIMESTAMP), 'data-1')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(2, CAST('2017-11-22T06:02:09.243857+00:00' AS TIMESTAMP), 'data-2')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(3, CAST('2017-11-22T03:10:11.134509+00:00' AS TIMESTAMP), 'data-3')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(4, CAST('2017-11-22T00:34:00.184671+00:00' AS TIMESTAMP), 'data-4')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(5, CAST('2018-12-21T22:20:08.935889+00:00' AS TIMESTAMP), 'material-5')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(6, CAST('2018-12-21T21:55:30.589712+00:00' AS TIMESTAMP), 'material-6')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(7, CAST('2018-12-21T17:31:14.532797+00:00' AS TIMESTAMP), 'material-7')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(8, CAST('2018-12-21T15:21:51.237521+00:00' AS TIMESTAMP), 'material-8')");
    sql(
        "INSERT INTO TABLE %s VALUES %s",
        tableName, "(9, CAST('2018-12-21T15:02:15.230570+00:00' AS TIMESTAMP), 'material-9')");
  }

  private void pushFilters(ScanBuilder scan, Predicate... predicates) {
    Assertions.assertThat(scan).isInstanceOf(SupportsPushDownV2Filters.class);
    SupportsPushDownV2Filters filterable = (SupportsPushDownV2Filters) scan;
    filterable.pushPredicates(predicates);
  }

  private Expression[] expressions(Expression... expressions) {
    return expressions;
  }

  private int years(int year) {
    return year - 1970;
  }

  private int months(int year, int month) {
    return years(year) * 12 + month;
  }

  private int days(int year, int month, int day) {
    return (int) DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(year, month, day));
  }
}
