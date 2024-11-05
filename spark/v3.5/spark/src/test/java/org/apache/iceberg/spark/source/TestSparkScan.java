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

import static org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.createPartitionedTable;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.createUnpartitionedTable;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToDayOrdinal;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToHourOrdinal;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToMonthOrdinal;
import static org.apache.iceberg.spark.SystemFunctionPushDownHelper.timestampStrToYearOrdinal;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.functions.BucketFunction;
import org.apache.iceberg.spark.functions.DaysFunction;
import org.apache.iceberg.spark.functions.HoursFunction;
import org.apache.iceberg.spark.functions.MonthsFunction;
import org.apache.iceberg.spark.functions.TruncateFunction;
import org.apache.iceberg.spark.functions.YearsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.UserDefinedScalarFunc;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkScan extends TestBaseWithCatalog {

  private static final String DUMMY_BLOB_TYPE = "sum-data-size-bytes-v1";

  @Parameter(index = 3)
  private String format;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, format = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        "parquet"
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        "avro"
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        "orc"
      }
    };
  }

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testEstimatedRowCount() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, date DATE) USING iceberg TBLPROPERTIES('%s' = '%s')",
        tableName, TableProperties.DEFAULT_FILE_FORMAT, format);

    Dataset<Row> df =
        spark
            .range(10000)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id AS INT)")))
            .select("id", "date");

    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkScan scan = (SparkScan) scanBuilder.build();
    Statistics stats = scan.estimateStatistics();

    assertThat(stats.numRows().getAsLong()).isEqualTo(10000L);
  }

  @TestTemplate
  public void testTableWithoutColStats() throws NoSuchTableException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);

    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkScan scan = (SparkScan) scanBuilder.build();

    Map<String, String> reportColStatsDisabled =
        ImmutableMap.of(
            SQLConf.CBO_ENABLED().key(), "true", SparkSQLProperties.REPORT_COLUMN_STATS, "false");

    Map<String, String> reportColStatsEnabled =
        ImmutableMap.of(SQLConf.CBO_ENABLED().key(), "true");

    checkColStatisticsNotReported(scan, 4L);
    withSQLConf(reportColStatsDisabled, () -> checkColStatisticsNotReported(scan, 4L));
    // The expected col NDVs are nulls
    withSQLConf(
        reportColStatsEnabled, () -> checkColStatisticsReported(scan, 4L, Maps.newHashMap()));
  }

  @TestTemplate
  public void testTableWithoutApacheDatasketchColStat() throws NoSuchTableException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkScan scan = (SparkScan) scanBuilder.build();

    Map<String, String> reportColStatsDisabled =
        ImmutableMap.of(
            SQLConf.CBO_ENABLED().key(), "true", SparkSQLProperties.REPORT_COLUMN_STATS, "false");

    Map<String, String> reportColStatsEnabled =
        ImmutableMap.of(SQLConf.CBO_ENABLED().key(), "true");

    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/test/statistics/file.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    DUMMY_BLOB_TYPE,
                    snapshotId,
                    1,
                    ImmutableList.of(1),
                    ImmutableMap.of("data_size", "4"))));

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    checkColStatisticsNotReported(scan, 4L);
    withSQLConf(reportColStatsDisabled, () -> checkColStatisticsNotReported(scan, 4L));
    // The expected col NDVs are nulls
    withSQLConf(
        reportColStatsEnabled, () -> checkColStatisticsReported(scan, 4L, Maps.newHashMap()));
  }

  @TestTemplate
  public void testTableWithOneColStats() throws NoSuchTableException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkScan scan = (SparkScan) scanBuilder.build();

    Map<String, String> reportColStatsDisabled =
        ImmutableMap.of(
            SQLConf.CBO_ENABLED().key(), "true", SparkSQLProperties.REPORT_COLUMN_STATS, "false");

    Map<String, String> reportColStatsEnabled =
        ImmutableMap.of(SQLConf.CBO_ENABLED().key(), "true");

    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/test/statistics/file.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    APACHE_DATASKETCHES_THETA_V1,
                    snapshotId,
                    1,
                    ImmutableList.of(1),
                    ImmutableMap.of("ndv", "4"))));

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    checkColStatisticsNotReported(scan, 4L);
    withSQLConf(reportColStatsDisabled, () -> checkColStatisticsNotReported(scan, 4L));

    Map<String, Long> expectedOneNDV = Maps.newHashMap();
    expectedOneNDV.put("id", 4L);
    withSQLConf(reportColStatsEnabled, () -> checkColStatisticsReported(scan, 4L, expectedOneNDV));
  }

  @TestTemplate
  public void testTableWithOneApacheDatasketchColStatAndOneDifferentColStat()
      throws NoSuchTableException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkScan scan = (SparkScan) scanBuilder.build();

    Map<String, String> reportColStatsDisabled =
        ImmutableMap.of(
            SQLConf.CBO_ENABLED().key(), "true", SparkSQLProperties.REPORT_COLUMN_STATS, "false");

    Map<String, String> reportColStatsEnabled =
        ImmutableMap.of(SQLConf.CBO_ENABLED().key(), "true");

    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/test/statistics/file.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    APACHE_DATASKETCHES_THETA_V1,
                    snapshotId,
                    1,
                    ImmutableList.of(1),
                    ImmutableMap.of("ndv", "4")),
                new GenericBlobMetadata(
                    DUMMY_BLOB_TYPE,
                    snapshotId,
                    1,
                    ImmutableList.of(1),
                    ImmutableMap.of("data_size", "2"))));

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    checkColStatisticsNotReported(scan, 4L);
    withSQLConf(reportColStatsDisabled, () -> checkColStatisticsNotReported(scan, 4L));

    Map<String, Long> expectedOneNDV = Maps.newHashMap();
    expectedOneNDV.put("id", 4L);
    withSQLConf(reportColStatsEnabled, () -> checkColStatisticsReported(scan, 4L, expectedOneNDV));
  }

  @TestTemplate
  public void testTableWithTwoColStats() throws NoSuchTableException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkScan scan = (SparkScan) scanBuilder.build();

    Map<String, String> reportColStatsDisabled =
        ImmutableMap.of(
            SQLConf.CBO_ENABLED().key(), "true", SparkSQLProperties.REPORT_COLUMN_STATS, "false");

    Map<String, String> reportColStatsEnabled =
        ImmutableMap.of(SQLConf.CBO_ENABLED().key(), "true");

    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/test/statistics/file.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    APACHE_DATASKETCHES_THETA_V1,
                    snapshotId,
                    1,
                    ImmutableList.of(1),
                    ImmutableMap.of("ndv", "4")),
                new GenericBlobMetadata(
                    APACHE_DATASKETCHES_THETA_V1,
                    snapshotId,
                    1,
                    ImmutableList.of(2),
                    ImmutableMap.of("ndv", "2"))));

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    checkColStatisticsNotReported(scan, 4L);
    withSQLConf(reportColStatsDisabled, () -> checkColStatisticsNotReported(scan, 4L));

    Map<String, Long> expectedTwoNDVs = Maps.newHashMap();
    expectedTwoNDVs.put("id", 4L);
    expectedTwoNDVs.put("data", 2L);
    withSQLConf(reportColStatsEnabled, () -> checkColStatisticsReported(scan, 4L, expectedTwoNDVs));
  }

  @TestTemplate
  public void testUnpartitionedYears() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction function = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            "=",
            expressions(
                udf, intLit(timestampStrToYearOrdinal("2017-11-22T00:00:00.000000+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT Equal
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedYears() throws Exception {
    createPartitionedTable(spark, tableName, "years(ts)");

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction function = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            "=",
            expressions(
                udf, intLit(timestampStrToYearOrdinal("2017-11-22T00:00:00.000000+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT Equal
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @TestTemplate
  public void testUnpartitionedMonths() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    MonthsFunction.TimestampToMonthsFunction function =
        new MonthsFunction.TimestampToMonthsFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            ">",
            expressions(
                udf, intLit(timestampStrToMonthOrdinal("2017-11-22T00:00:00.000000+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT GT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedMonths() throws Exception {
    createPartitionedTable(spark, tableName, "months(ts)");

    SparkScanBuilder builder = scanBuilder();

    MonthsFunction.TimestampToMonthsFunction function =
        new MonthsFunction.TimestampToMonthsFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            ">",
            expressions(
                udf, intLit(timestampStrToMonthOrdinal("2017-11-22T00:00:00.000000+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT GT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @TestTemplate
  public void testUnpartitionedDays() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    DaysFunction.TimestampToDaysFunction function = new DaysFunction.TimestampToDaysFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            "<",
            expressions(
                udf, dateLit(timestampStrToDayOrdinal("2018-11-20T00:00:00.000000+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT LT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedDays() throws Exception {
    createPartitionedTable(spark, tableName, "days(ts)");

    SparkScanBuilder builder = scanBuilder();

    DaysFunction.TimestampToDaysFunction function = new DaysFunction.TimestampToDaysFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            "<",
            expressions(
                udf, dateLit(timestampStrToDayOrdinal("2018-11-20T00:00:00.000000+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT LT
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @TestTemplate
  public void testUnpartitionedHours() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    HoursFunction.TimestampToHoursFunction function = new HoursFunction.TimestampToHoursFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            ">=",
            expressions(
                udf, intLit(timestampStrToHourOrdinal("2017-11-22T06:02:09.243857+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT GTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedHours() throws Exception {
    createPartitionedTable(spark, tableName, "hours(ts)");

    SparkScanBuilder builder = scanBuilder();

    HoursFunction.TimestampToHoursFunction function = new HoursFunction.TimestampToHoursFunction();
    UserDefinedScalarFunc udf = toUDF(function, expressions(fieldRef("ts")));
    Predicate predicate =
        new Predicate(
            ">=",
            expressions(
                udf, intLit(timestampStrToHourOrdinal("2017-11-22T06:02:09.243857+00:00"))));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(8);

    // NOT GTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(2);
  }

  @TestTemplate
  public void testUnpartitionedBucketLong() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketLong function = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(5), fieldRef("id")));
    Predicate predicate = new Predicate(">=", expressions(udf, intLit(2)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT GTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedBucketLong() throws Exception {
    createPartitionedTable(spark, tableName, "bucket(5, id)");

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketLong function = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(5), fieldRef("id")));
    Predicate predicate = new Predicate(">=", expressions(udf, intLit(2)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(6);

    // NOT GTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(4);
  }

  @TestTemplate
  public void testUnpartitionedBucketString() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketString function = new BucketFunction.BucketString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(5), fieldRef("data")));
    Predicate predicate = new Predicate("<=", expressions(udf, intLit(2)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT LTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedBucketString() throws Exception {
    createPartitionedTable(spark, tableName, "bucket(5, data)");

    SparkScanBuilder builder = scanBuilder();

    BucketFunction.BucketString function = new BucketFunction.BucketString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(5), fieldRef("data")));
    Predicate predicate = new Predicate("<=", expressions(udf, intLit(2)));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(6);

    // NOT LTEQ
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(4);
  }

  @TestTemplate
  public void testUnpartitionedTruncateString() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(4), fieldRef("data")));
    Predicate predicate = new Predicate("<>", expressions(udf, stringLit("data")));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT NotEqual
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedTruncateString() throws Exception {
    createPartitionedTable(spark, tableName, "truncate(4, data)");

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(4), fieldRef("data")));
    Predicate predicate = new Predicate("<>", expressions(udf, stringLit("data")));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);

    // NOT NotEqual
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(5);
  }

  @TestTemplate
  public void testUnpartitionedIsNull() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(4), fieldRef("data")));
    Predicate predicate = new Predicate("IS_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT IsNull
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedIsNull() throws Exception {
    createPartitionedTable(spark, tableName, "truncate(4, data)");

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(4), fieldRef("data")));
    Predicate predicate = new Predicate("IS_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(0);

    // NOT IsNULL
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testUnpartitionedIsNotNull() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(4), fieldRef("data")));
    Predicate predicate = new Predicate("IS_NOT_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT IsNotNull
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedIsNotNull() throws Exception {
    createPartitionedTable(spark, tableName, "truncate(4, data)");

    SparkScanBuilder builder = scanBuilder();

    TruncateFunction.TruncateString function = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf = toUDF(function, expressions(intLit(4), fieldRef("data")));
    Predicate predicate = new Predicate("IS_NOT_NULL", expressions(udf));
    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT IsNotNULL
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(0);
  }

  @TestTemplate
  public void testUnpartitionedAnd() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 = toUDF(tsToYears, expressions(fieldRef("ts")));
    Predicate predicate1 = new Predicate("=", expressions(udf1, intLit(2017 - 1970)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf = toUDF(bucketLong, expressions(intLit(5), fieldRef("id")));
    Predicate predicate2 = new Predicate(">=", expressions(udf, intLit(2)));
    Predicate predicate = new And(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT (years(ts) = 47 AND bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedAnd() throws Exception {
    createPartitionedTable(spark, tableName, "years(ts), bucket(5, id)");

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 = toUDF(tsToYears, expressions(fieldRef("ts")));
    Predicate predicate1 = new Predicate("=", expressions(udf1, intLit(2017 - 1970)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf = toUDF(bucketLong, expressions(intLit(5), fieldRef("id")));
    Predicate predicate2 = new Predicate(">=", expressions(udf, intLit(2)));
    Predicate predicate = new And(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(1);

    // NOT (years(ts) = 47 AND bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(9);
  }

  @TestTemplate
  public void testUnpartitionedOr() throws Exception {
    createUnpartitionedTable(spark, tableName);

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 = toUDF(tsToYears, expressions(fieldRef("ts")));
    Predicate predicate1 = new Predicate("=", expressions(udf1, intLit(2017 - 1970)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf = toUDF(bucketLong, expressions(intLit(5), fieldRef("id")));
    Predicate predicate2 = new Predicate(">=", expressions(udf, intLit(2)));
    Predicate predicate = new Or(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);

    // NOT (years(ts) = 47 OR bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(10);
  }

  @TestTemplate
  public void testPartitionedOr() throws Exception {
    createPartitionedTable(spark, tableName, "years(ts), bucket(5, id)");

    SparkScanBuilder builder = scanBuilder();

    YearsFunction.TimestampToYearsFunction tsToYears = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf1 = toUDF(tsToYears, expressions(fieldRef("ts")));
    Predicate predicate1 = new Predicate("=", expressions(udf1, intLit(2018 - 1970)));

    BucketFunction.BucketLong bucketLong = new BucketFunction.BucketLong(DataTypes.LongType);
    UserDefinedScalarFunc udf = toUDF(bucketLong, expressions(intLit(5), fieldRef("id")));
    Predicate predicate2 = new Predicate(">=", expressions(udf, intLit(2)));
    Predicate predicate = new Or(predicate1, predicate2);

    pushFilters(builder, predicate);
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(6);

    // NOT (years(ts) = 48 OR bucket(id, 5) >= 2)
    builder = scanBuilder();

    predicate = new Not(predicate);
    pushFilters(builder, predicate);
    scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions().length).isEqualTo(4);
  }

  private SparkScanBuilder scanBuilder() throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", tableName));

    return new SparkScanBuilder(spark, table, options);
  }

  private void pushFilters(ScanBuilder scan, Predicate... predicates) {
    assertThat(scan).isInstanceOf(SupportsPushDownV2Filters.class);
    SupportsPushDownV2Filters filterable = (SupportsPushDownV2Filters) scan;
    filterable.pushPredicates(predicates);
  }

  private Expression[] expressions(Expression... expressions) {
    return expressions;
  }

  private void checkColStatisticsNotReported(SparkScan scan, long expectedRowCount) {
    Statistics stats = scan.estimateStatistics();
    assertThat(stats.numRows().getAsLong()).isEqualTo(expectedRowCount);

    Map<NamedReference, ColumnStatistics> columnStats = stats.columnStats();
    assertThat(columnStats.isEmpty());
  }

  private void checkColStatisticsReported(
      SparkScan scan, long expectedRowCount, Map<String, Long> expectedNDVs) {
    Statistics stats = scan.estimateStatistics();
    assertThat(stats.numRows().getAsLong()).isEqualTo(expectedRowCount);

    Map<NamedReference, ColumnStatistics> columnStats = stats.columnStats();
    if (expectedNDVs.isEmpty()) {
      assertThat(columnStats.values().stream().allMatch(value -> value.distinctCount().isEmpty()))
          .isTrue();
    } else {
      for (Map.Entry<String, Long> entry : expectedNDVs.entrySet()) {
        assertThat(
                columnStats.get(FieldReference.column(entry.getKey())).distinctCount().getAsLong())
            .isEqualTo(entry.getValue());
      }
    }
  }

  private static LiteralValue<Integer> intLit(int value) {
    return LiteralValue.apply(value, DataTypes.IntegerType);
  }

  private static LiteralValue<Integer> dateLit(int value) {
    return LiteralValue.apply(value, DataTypes.DateType);
  }

  private static LiteralValue<String> stringLit(String value) {
    return LiteralValue.apply(value, DataTypes.StringType);
  }

  private static NamedReference fieldRef(String col) {
    return FieldReference.apply(col);
  }

  private static UserDefinedScalarFunc toUDF(BoundFunction function, Expression[] expressions) {
    return new UserDefinedScalarFunc(function.name(), function.canonicalName(), expressions);
  }
}
