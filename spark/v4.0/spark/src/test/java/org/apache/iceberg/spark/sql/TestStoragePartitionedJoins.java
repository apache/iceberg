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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestStoragePartitionedJoins extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, planningMode = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        LOCAL
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        DISTRIBUTED
      },
    };
  }

  private static final String OTHER_TABLE_NAME = "other_table";

  // open file cost and split size are set as 16 MB to produce a split per file
  private static final Map<String, String> TABLE_PROPERTIES =
      ImmutableMap.of(
          TableProperties.SPLIT_SIZE, "16777216", TableProperties.SPLIT_OPEN_FILE_COST, "16777216");

  // only v2 bucketing and preserve data grouping properties have to be enabled to trigger SPJ
  // other properties are only to simplify testing and validation
  private static final Map<String, String> ENABLED_SPJ_SQL_CONF =
      ImmutableMap.of(
          SQLConf.V2_BUCKETING_ENABLED().key(),
          "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED().key(),
          "true",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
          "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
          "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
          "-1",
          SparkSQLProperties.PRESERVE_DATA_GROUPING,
          "true");

  private static final Map<String, String> DISABLED_SPJ_SQL_CONF =
      ImmutableMap.of(
          SQLConf.V2_BUCKETING_ENABLED().key(),
          "false",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
          "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
          "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
          "-1",
          SparkSQLProperties.PRESERVE_DATA_GROUPING,
          "true");

  @Parameter(index = 3)
  private PlanningMode planningMode;

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName(OTHER_TABLE_NAME));
  }

  // TODO: add tests for truncate transforms once SPARK-40295 is released

  @TestTemplate
  public void testJoinsWithBucketingOnByteColumn() throws NoSuchTableException {
    checkJoin("byte_col", "TINYINT", "bucket(4, byte_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnShortColumn() throws NoSuchTableException {
    checkJoin("short_col", "SMALLINT", "bucket(4, short_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnIntColumn() throws NoSuchTableException {
    checkJoin("int_col", "INT", "bucket(16, int_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnLongColumn() throws NoSuchTableException {
    checkJoin("long_col", "BIGINT", "bucket(16, long_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnTimestampColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP", "bucket(16, timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnTimestampNtzColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP_NTZ", "bucket(16, timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnDateColumn() throws NoSuchTableException {
    checkJoin("date_col", "DATE", "bucket(8, date_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnDecimalColumn() throws NoSuchTableException {
    checkJoin("decimal_col", "DECIMAL(20, 2)", "bucket(8, decimal_col)");
  }

  @TestTemplate
  public void testJoinsWithBucketingOnBinaryColumn() throws NoSuchTableException {
    checkJoin("binary_col", "BINARY", "bucket(8, binary_col)");
  }

  @TestTemplate
  public void testJoinsWithYearsOnTimestampColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP", "years(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithYearsOnTimestampNtzColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP_NTZ", "years(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithYearsOnDateColumn() throws NoSuchTableException {
    checkJoin("date_col", "DATE", "years(date_col)");
  }

  @TestTemplate
  public void testJoinsWithMonthsOnTimestampColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP", "months(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithMonthsOnTimestampNtzColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP_NTZ", "months(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithMonthsOnDateColumn() throws NoSuchTableException {
    checkJoin("date_col", "DATE", "months(date_col)");
  }

  @TestTemplate
  public void testJoinsWithDaysOnTimestampColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP", "days(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithDaysOnTimestampNtzColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP_NTZ", "days(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithDaysOnDateColumn() throws NoSuchTableException {
    checkJoin("date_col", "DATE", "days(date_col)");
  }

  @TestTemplate
  public void testJoinsWithHoursOnTimestampColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP", "hours(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithHoursOnTimestampNtzColumn() throws NoSuchTableException {
    checkJoin("timestamp_col", "TIMESTAMP_NTZ", "hours(timestamp_col)");
  }

  @TestTemplate
  public void testJoinsWithMultipleTransformTypes() throws NoSuchTableException {
    String createTableStmt =
        "CREATE TABLE %s ("
            + "  id BIGINT, int_col INT, date_col1 DATE, date_col2 DATE, date_col3 DATE,"
            + "  timestamp_col TIMESTAMP, string_col STRING, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY ("
            + "  years(date_col1), months(date_col2), days(date_col3), hours(timestamp_col), "
            + "  bucket(8, int_col), dep)"
            + "TBLPROPERTIES (%s)";

    sql(createTableStmt, tableName, tablePropsAsString(TABLE_PROPERTIES));
    sql(createTableStmt, tableName(OTHER_TABLE_NAME), tablePropsAsString(TABLE_PROPERTIES));

    Table table = validationCatalog.loadTable(tableIdent);

    Dataset<Row> dataDF = randomDataDF(table.schema(), 16);

    // write to the first table 1 time to generate 1 file per partition
    append(tableName, dataDF);

    // write to the second table 2 times to generate 2 files per partition
    append(tableName(OTHER_TABLE_NAME), dataDF);
    append(tableName(OTHER_TABLE_NAME), dataDF);

    // Spark SPJ support is limited at the moment and requires all source partitioning columns,
    // which were projected in the query, to be part of the join condition
    // suppose a table is partitioned by `p1`, `bucket(8, pk)`
    // queries covering `p1` and `pk` columns must include equality predicates
    // on both `p1` and `pk` to benefit from SPJ
    // this is a temporary Spark limitation that will be removed in a future release

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT t1.id "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.dep = t2.dep "
            + "ORDER BY t1.id",
        tableName,
        tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT t1.id, t1.int_col, t1.date_col1 "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.int_col = t2.int_col AND t1.date_col1 = t2.date_col1 "
            + "ORDER BY t1.id, t1.int_col, t1.date_col1",
        tableName,
        tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT t1.id, t1.timestamp_col, t1.string_col "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.timestamp_col = t2.timestamp_col AND t1.string_col = t2.string_col "
            + "ORDER BY t1.id, t1.timestamp_col, t1.string_col",
        tableName,
        tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT t1.id, t1.date_col1, t1.date_col2, t1.date_col3 "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.date_col1 = t2.date_col1 AND t1.date_col2 = t2.date_col2 AND t1.date_col3 = t2.date_col3 "
            + "ORDER BY t1.id, t1.date_col1, t1.date_col2, t1.date_col3",
        tableName,
        tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT t1.id, t1.int_col, t1.timestamp_col, t1.dep "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.int_col = t2.int_col AND t1.timestamp_col = t2.timestamp_col AND t1.dep = t2.dep "
            + "ORDER BY t1.id, t1.int_col, t1.timestamp_col, t1.dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testJoinsWithCompatibleSpecEvolution() {
    // create a table with an empty spec
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(TABLE_PROPERTIES));

    Table table = validationCatalog.loadTable(tableIdent);

    // evolve the spec in the first table by adding `dep`
    table.updateSpec().addField("dep").commit();

    // insert data into the first table partitioned by `dep`
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName);

    // evolve the spec in the first table by adding `bucket(int_col, 8)`
    table.updateSpec().addField(Expressions.bucket("int_col", 8)).commit();

    // insert data into the first table partitioned by `dep`, `bucket(8, int_col)`
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO %s VALUES (2L, 200, 'hr')", tableName);

    // create another table partitioned by `other_dep`
    sql(
        "CREATE TABLE %s (other_id BIGINT, other_int_col INT, other_dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (other_dep)"
            + "TBLPROPERTIES (%s)",
        tableName(OTHER_TABLE_NAME), tablePropsAsString(TABLE_PROPERTIES));

    // insert data into the second table partitioned by 'other_dep'
    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (2L, 200, 'hr')", tableName(OTHER_TABLE_NAME));

    // SPJ would apply as the grouping keys are compatible
    // the first table: `dep` (an intersection of all active partition fields across scanned specs)
    // the second table: `other_dep` (the only partition field).

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT * "
            + "FROM %s "
            + "INNER JOIN %s "
            + "ON id = other_id AND int_col = other_int_col AND dep = other_dep "
            + "ORDER BY id, int_col, dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testJoinsWithIncompatibleSpecs() {
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName);
    sql("INSERT INTO %s VALUES (2L, 200, 'software')", tableName);
    sql("INSERT INTO %s VALUES (3L, 300, 'software')", tableName);

    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, int_col))"
            + "TBLPROPERTIES (%s)",
        tableName(OTHER_TABLE_NAME), tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (2L, 200, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (3L, 300, 'software')", tableName(OTHER_TABLE_NAME));

    // queries can't benefit from SPJ as specs are not compatible
    // the first table: `dep`
    // the second table: `bucket(8, int_col)`

    assertPartitioningAwarePlan(
        3, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles with SPJ */
        "SELECT * "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.int_col = t2.int_col AND t1.dep = t2.dep "
            + "ORDER BY t1.id, t1.int_col, t1.dep, t2.id, t2.int_col, t2.dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testJoinsWithUnpartitionedTables() {
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "TBLPROPERTIES ("
            + "  'read.split.target-size' = 16777216,"
            + "  'read.split.open-file-cost' = 16777216)",
        tableName);

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName);
    sql("INSERT INTO %s VALUES (2L, 200, 'software')", tableName);
    sql("INSERT INTO %s VALUES (3L, 300, 'software')", tableName);

    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "TBLPROPERTIES ("
            + "  'read.split.target-size' = 16777216,"
            + "  'read.split.open-file-cost' = 16777216)",
        tableName(OTHER_TABLE_NAME));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (2L, 200, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (3L, 300, 'software')", tableName(OTHER_TABLE_NAME));

    // queries covering unpartitioned tables can't benefit from SPJ but shouldn't fail

    assertPartitioningAwarePlan(
        3, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT * "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.int_col = t2.int_col AND t1.dep = t2.dep "
            + "ORDER BY t1.id, t1.int_col, t1.dep, t2.id, t2.int_col, t2.dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testJoinsWithEmptyTable() {
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(TABLE_PROPERTIES));

    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName(OTHER_TABLE_NAME), tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (2L, 200, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (3L, 300, 'software')", tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        3, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT * "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.int_col = t2.int_col AND t1.dep = t2.dep "
            + "ORDER BY t1.id, t1.int_col, t1.dep, t2.id, t2.int_col, t2.dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testJoinsWithOneSplitTables() {
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName);

    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName(OTHER_TABLE_NAME), tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT * "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.int_col = t2.int_col AND t1.dep = t2.dep "
            + "ORDER BY t1.id, t1.int_col, t1.dep, t2.id, t2.int_col, t2.dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testJoinsWithMismatchingPartitionKeys() {
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName);
    sql("INSERT INTO %s VALUES (2L, 100, 'hr')", tableName);

    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)"
            + "TBLPROPERTIES (%s)",
        tableName(OTHER_TABLE_NAME), tablePropsAsString(TABLE_PROPERTIES));

    sql("INSERT INTO %s VALUES (1L, 100, 'software')", tableName(OTHER_TABLE_NAME));
    sql("INSERT INTO %s VALUES (3L, 300, 'hardware')", tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT * "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.dep = t2.dep "
            + "ORDER BY t1.id, t1.int_col, t1.dep, t2.id, t2.int_col, t2.dep",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testAggregates() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, int_col INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep, bucket(8, int_col))"
            + "TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(TABLE_PROPERTIES));

    // write to the table 3 times to generate 3 files per partition
    Table table = validationCatalog.loadTable(tableIdent);
    Dataset<Row> dataDF = randomDataDF(table.schema(), 100);
    append(tableName, dataDF);

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT COUNT (DISTINCT id) AS count FROM %s GROUP BY dep, int_col ORDER BY count",
        tableName,
        tableName(OTHER_TABLE_NAME));

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT COUNT (DISTINCT id) AS count FROM %s GROUP BY dep ORDER BY count",
        tableName,
        tableName(OTHER_TABLE_NAME));
  }

  private void checkJoin(String sourceColumnName, String sourceColumnType, String transform)
      throws NoSuchTableException {

    String createTableStmt =
        "CREATE TABLE %s (id BIGINT, salary INT, %s %s)"
            + "USING iceberg "
            + "PARTITIONED BY (%s)"
            + "TBLPROPERTIES (%s)";

    sql(
        createTableStmt,
        tableName,
        sourceColumnName,
        sourceColumnType,
        transform,
        tablePropsAsString(TABLE_PROPERTIES));
    configurePlanningMode(tableName, planningMode);

    sql(
        createTableStmt,
        tableName(OTHER_TABLE_NAME),
        sourceColumnName,
        sourceColumnType,
        transform,
        tablePropsAsString(TABLE_PROPERTIES));
    configurePlanningMode(tableName(OTHER_TABLE_NAME), planningMode);

    Table table = validationCatalog.loadTable(tableIdent);
    Dataset<Row> dataDF = randomDataDF(table.schema(), 200);
    append(tableName, dataDF);
    append(tableName(OTHER_TABLE_NAME), dataDF);

    assertPartitioningAwarePlan(
        1, /* expected num of shuffles with SPJ */
        3, /* expected num of shuffles without SPJ */
        "SELECT t1.id, t1.salary, t1.%s "
            + "FROM %s t1 "
            + "INNER JOIN %s t2 "
            + "ON t1.id = t2.id AND t1.%s = t2.%s "
            + "ORDER BY t1.id, t1.%s",
        sourceColumnName,
        tableName,
        tableName(OTHER_TABLE_NAME),
        sourceColumnName,
        sourceColumnName,
        sourceColumnName);
  }

  private void assertPartitioningAwarePlan(
      int expectedNumShufflesWithSPJ,
      int expectedNumShufflesWithoutSPJ,
      String query,
      Object... args) {

    AtomicReference<List<Object[]>> rowsWithSPJ = new AtomicReference<>();
    AtomicReference<List<Object[]>> rowsWithoutSPJ = new AtomicReference<>();

    withSQLConf(
        ENABLED_SPJ_SQL_CONF,
        () -> {
          String plan = executeAndKeepPlan(query, args).toString();
          int actualNumShuffles = StringUtils.countMatches(plan, "Exchange");
          assertThat(actualNumShuffles)
              .as("Number of shuffles with enabled SPJ must match")
              .isEqualTo(expectedNumShufflesWithSPJ);

          rowsWithSPJ.set(sql(query, args));
        });

    withSQLConf(
        DISABLED_SPJ_SQL_CONF,
        () -> {
          String plan = executeAndKeepPlan(query, args).toString();
          int actualNumShuffles = StringUtils.countMatches(plan, "Exchange");
          assertThat(actualNumShuffles)
              .as("Number of shuffles with disabled SPJ must match")
              .isEqualTo(expectedNumShufflesWithoutSPJ);

          rowsWithoutSPJ.set(sql(query, args));
        });

    assertEquals("SPJ should not change query output", rowsWithoutSPJ.get(), rowsWithSPJ.get());
  }

  private Dataset<Row> randomDataDF(Schema schema, int numRows) {
    Iterable<InternalRow> rows = RandomData.generateSpark(schema, numRows, 0);
    JavaRDD<InternalRow> rowRDD = sparkContext.parallelize(Lists.newArrayList(rows));
    StructType rowSparkType = SparkSchemaUtil.convert(schema);
    return spark.internalCreateDataFrame(JavaRDD.toRDD(rowRDD), rowSparkType, false);
  }

  private void append(String table, Dataset<Row> df) throws NoSuchTableException {
    // fanout writes are enabled as write-time clustering is not supported without Spark extensions
    df.coalesce(1).writeTo(table).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();
  }
}
