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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.SystemFunctionPushDownHelper;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesProcedure extends ExtensionsTestBase {

  private static final String QUOTED_SPECIAL_CHARS_TABLE_NAME = "`table:with.special:chars`";

  @BeforeAll
  public static void setupSpark() {
    // disable AQE as tests assume that writes generate a particular number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
  }

  @TestTemplate
  public void testZOrderSortExpression() {
    List<ExtendedParser.RawOrderField> order =
        ExtendedParser.parseSortOrder(spark, "c1, zorder(c2, c3)");
    assertThat(order).as("Should parse 2 order fields").hasSize(2);
    assertThat(((NamedReference<?>) order.get(0).term()).name())
        .as("First field should be a ref")
        .isEqualTo("c1");
    assertThat(order.get(1).term()).as("Second field should be zorder").isInstanceOf(Zorder.class);
  }

  @TestTemplate
  public void testRewriteDataFilesInEmptyTable() {
    createTable();
    List<Object[]> output = sql("CALL %s.system.rewrite_data_files('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0, 0L, 0)), output);
  }

  @TestTemplate
  public void testRewriteDataFilesOnPartitionTable() {
    createPartitionTable();
    // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    List<Object[]> output =
        sql("CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 2 data files (one per partition) ",
        row(10, 2),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesOnNonPartitionTable() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    List<Object[]> output =
        sql("CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithOptions() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // set the min-input-files = 12, instead of default 5 to skip compacting the files.
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files','12'))",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 0 data files and add 0 data files",
        ImmutableList.of(row(0, 0, 0L, 0)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithSortStrategy() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // set sort_order = c1 DESC LAST
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', sort_order => 'c1 DESC NULLS LAST')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithSortStrategyAndMultipleShufflePartitionsPerFile() {
    createTable();
    insertData(10 /* file count */);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files("
                + " table => '%s', "
                + " strategy => 'sort', "
                + " sort_order => 'c1', "
                + " options => map('shuffle-partitions-per-file', '2'))",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));

    // as there is only one small output file, validate the query ordering (it will not change)
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null));
    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  @TestTemplate
  public void testRewriteDataFilesWithZOrder() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);

    // set z_order = c1,c2
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', sort_order => 'zorder(c1,c2)')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    // Due to Z_order, the data written will be in the below order.
    // As there is only one small output file, we can validate the query ordering (as it will not
    // change).
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null));
    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  @TestTemplate
  public void testRewriteDataFilesWithZOrderNullBinaryColumn() {
    sql("CREATE TABLE %s (c1 int, c2 string, c3 binary) USING iceberg", tableName);

    for (int i = 0; i < 5; i++) {
      sql("INSERT INTO %s values (1, 'foo', null), (2, 'bar', null)", tableName);
    }

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', sort_order => 'zorder(c2,c3)')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    assertThat(output.get(0)).hasSize(4);
    assertThat(snapshotSummary())
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, String.valueOf(output.get(0)[2]));
    assertThat(sql("SELECT * FROM %s", tableName))
        .containsExactly(
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null));
  }

  @TestTemplate
  public void testRewriteDataFilesWithZOrderAndMultipleShufflePartitionsPerFile() {
    createTable();
    insertData(10 /* file count */);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files("
                + " table => '%s', "
                + "strategy => 'sort', "
                + " sort_order => 'zorder(c1, c2)', "
                + " options => map('shuffle-partitions-per-file', '2'))",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));

    // due to z-ordering, the data will be written in the below order
    // as there is only one small output file, validate the query ordering (it will not change)
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null));
    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  @TestTemplate
  public void testRewriteDataFilesWithFilter() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // select only 5 files for compaction (files that may have c1 = 1)
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s',"
                + " where => 'c1 = 1 and c2 is not null')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 5 data files (containing c1 = 1) and add 1 data files",
        row(5, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithDeterministicTrueFilter() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();
    // select all 10 files for compaction
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', where => '1=1')",
            catalogName, tableIdent);
    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));
    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithDeterministicFalseFilter() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();
    // select no files for compaction
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', where => '0=1')",
            catalogName, tableIdent);
    assertEquals(
        "Action should rewrite 0 data files and add 0 data files",
        row(0, 0),
        Arrays.copyOf(output.get(0), 2));
    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithFilterOnPartitionTable() {
    createPartitionTable();
    // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // select only 5 files for compaction (files in the partition c2 = 'bar')
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c2 = \"bar\"')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 5 data files from single matching partition"
            + "(containing c2 = bar) and add 1 data files",
        row(5, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithFilterOnOnBucketExpression() {
    // currently spark session catalog only resolve to v1 functions instead of desired v2 functions
    // https://github.com/apache/spark/blob/branch-3.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L2070-L2083
    assumeThat(catalogName).isNotEqualTo(SparkCatalogConfig.SPARK.catalogName());
    createBucketPartitionTable();
    // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // select only 5 files for compaction (files in the partition c2 = 'bar')
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s',"
                + " where => '%s.system.bucket(2, c2) = 0')",
            catalogName, tableIdent, catalogName);

    assertEquals(
        "Action should rewrite 5 data files from single matching partition"
            + "(containing bucket(c2) = 0) and add 1 data files",
        row(5, 1),
        row(output.get(0)[0], output.get(0)[1]));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithInFilterOnPartitionTable() {
    createPartitionTable();
    // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // select only 5 files for compaction (files in the partition c2 in ('bar'))
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c2 in (\"bar\")')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 5 data files from single matching partition"
            + "(containing c2 = bar) and add 1 data files",
        row(5, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteDataFilesWithAllPossibleFilters() {
    createPartitionTable();
    // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
    insertData(10);

    // Pass the literal value which is not present in the data files.
    // So that parsing can be tested on a same dataset without actually compacting the files.

    // EqualTo
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 = 3')",
        catalogName, tableIdent);
    // GreaterThan
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 > 3')",
        catalogName, tableIdent);
    // GreaterThanOrEqual
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 >= 3')",
        catalogName, tableIdent);
    // LessThan
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 < 0')",
        catalogName, tableIdent);
    // LessThanOrEqual
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 <= 0')",
        catalogName, tableIdent);
    // In
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 in (3,4,5)')",
        catalogName, tableIdent);
    // IsNull
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 is null')",
        catalogName, tableIdent);
    // IsNotNull
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c3 is not null')",
        catalogName, tableIdent);
    // And
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 = 3 and c2 = \"bar\"')",
        catalogName, tableIdent);
    // Or
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 = 3 or c1 = 5')",
        catalogName, tableIdent);
    // Not
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c1 not in (1,2)')",
        catalogName, tableIdent);
    // StringStartsWith
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," + " where => 'c2 like \"%s\"')",
        catalogName, tableIdent, "car%");
    // TODO: Enable when org.apache.iceberg.spark.SparkFilters have implementations for
    // StringEndsWith & StringContains
    // StringEndsWith
    // sql("CALL %s.system.rewrite_data_files(table => '%s'," +
    //     " where => 'c2 like \"%s\"')", catalogName, tableIdent, "%car");
    // StringContains
    // sql("CALL %s.system.rewrite_data_files(table => '%s'," +
    //     " where => 'c2 like \"%s\"')", catalogName, tableIdent, "%car%");
  }

  @TestTemplate
  public void testRewriteDataFilesWithPossibleV2Filters() {
    // currently spark session catalog only resolve to v1 functions instead of desired v2 functions
    // https://github.com/apache/spark/blob/branch-3.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L2070-L2083
    assumeThat(catalogName).isNotEqualTo(SparkCatalogConfig.SPARK.catalogName());

    SystemFunctionPushDownHelper.createPartitionedTable(spark, tableName, "id");
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s',"
            + " where => '%s.system.bucket(2, data) >= 0')",
        catalogName, tableIdent, catalogName);
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s',"
            + " where => '%s.system.truncate(4, id) >= 1')",
        catalogName, tableIdent, catalogName);
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s',"
            + " where => '%s.system.years(ts) >= 1')",
        catalogName, tableIdent, catalogName);
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s',"
            + " where => '%s.system.months(ts) >= 1')",
        catalogName, tableIdent, catalogName);
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s',"
            + " where => '%s.system.days(ts) >= date(\"2023-01-01\")')",
        catalogName, tableIdent, catalogName);
    sql(
        "CALL %s.system.rewrite_data_files(table => '%s',"
            + " where => '%s.system.hours(ts) >= 1')",
        catalogName, tableIdent, catalogName);
  }

  @TestTemplate
  public void testRewriteDataFilesWithInvalidInputs() {
    createTable();
    // create 2 files under non-partitioned table
    insertData(2);

    // Test for invalid strategy
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files','2'), "
                        + "strategy => 'temp')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("unsupported strategy: temp. Only binpack or sort is supported");

    // Test for sort_order with binpack strategy
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'binpack', "
                        + "sort_order => 'c1 ASC NULLS FIRST')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must use only one rewriter type (bin-pack, sort, zorder)");

    // Test for sort strategy without any (default/user defined) sort_order
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot sort data without a valid sort order");

    // Test for sort_order with invalid null order
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                        + "sort_order => 'c1 ASC none')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to parse sortOrder: c1 ASC none");

    // Test for sort_order with invalid sort direction
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                        + "sort_order => 'c1 none NULLS FIRST')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to parse sortOrder: c1 none NULLS FIRST");

    // Test for sort_order with invalid column name
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                        + "sort_order => 'col1 DESC NULLS FIRST')",
                    catalogName, tableIdent))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'col1' in struct:");

    // Test with invalid filter column col1
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', " + "where => 'col1 = 3')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse predicates in where option: col1 = 3");

    // Test for z_order with invalid column name
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                        + "sort_order => 'zorder(col1)')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot find column 'col1' in table schema (case sensitive = false): "
                + "struct<1: c1: optional int, 2: c2: optional string, 3: c3: optional string>");

    // Test for z_order with sort_order
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                        + "sort_order => 'c1,zorder(c2,c3)')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot mix identity sort columns and a Zorder sort expression:" + " c1,zorder(c2,c3)");
  }

  @TestTemplate
  public void testInvalidCasesForRewriteDataFiles() {
    assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_data_files('n', table => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    assertThatThrownBy(() -> sql("CALL %s.custom.rewrite_data_files('n', 't')", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.rewrite_data_files not found");

    assertThatThrownBy(() -> sql("CALL %s.system.rewrite_data_files()", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_data_files(table => 't', table => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Duplicate procedure argument: table");

    assertThatThrownBy(() -> sql("CALL %s.system.rewrite_data_files('')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for parameter 'table'");
  }

  @TestTemplate
  public void testBinPackTableWithSpecialChars() {
    assumeThat(catalogName).isEqualTo(SparkCatalogConfig.HADOOP.catalogName());

    TableIdentifier identifier =
        TableIdentifier.of("default", QUOTED_SPECIAL_CHARS_TABLE_NAME.replaceAll("`", ""));
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg",
        tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    insertData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME), 10);

    List<Object[]> expectedRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', where => 'c2 is not null')",
            catalogName, tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    assertEquals(
        "Action should rewrite 10 data files and add 1 data file",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isEqualTo(
            Long.valueOf(snapshotSummary(identifier).get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);

    assertThat(SparkTableCache.get().size()).as("Table cache must be empty").isEqualTo(0);
  }

  @TestTemplate
  public void testSortTableWithSpecialChars() {
    assumeThat(catalogName).isEqualTo(SparkCatalogConfig.HADOOP.catalogName());

    TableIdentifier identifier =
        TableIdentifier.of("default", QUOTED_SPECIAL_CHARS_TABLE_NAME.replaceAll("`", ""));
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg",
        tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    insertData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME), 10);

    List<Object[]> expectedRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files("
                + "  table => '%s',"
                + "  strategy => 'sort',"
                + "  sort_order => 'c1',"
                + "  where => 'c2 is not null')",
            catalogName, tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    assertEquals(
        "Action should rewrite 10 data files and add 1 data file",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(
            Long.valueOf(snapshotSummary(identifier).get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);

    assertThat(SparkTableCache.get().size()).as("Table cache must be empty").isEqualTo(0);
  }

  @TestTemplate
  public void testZOrderTableWithSpecialChars() {
    assumeThat(catalogName).isEqualTo(SparkCatalogConfig.HADOOP.catalogName());

    TableIdentifier identifier =
        TableIdentifier.of("default", QUOTED_SPECIAL_CHARS_TABLE_NAME.replaceAll("`", ""));
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg",
        tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    insertData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME), 10);

    List<Object[]> expectedRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files("
                + "  table => '%s',"
                + "  strategy => 'sort',"
                + "  sort_order => 'zorder(c1, c2)',"
                + "  where => 'c2 is not null')",
            catalogName, tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));

    assertEquals(
        "Action should rewrite 10 data files and add 1 data file",
        row(10, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(
            Long.valueOf(snapshotSummary(identifier).get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);

    assertThat(SparkTableCache.get().size()).as("Table cache must be empty").isEqualTo(0);
  }

  @TestTemplate
  public void testDefaultSortOrder() {
    createTable();
    // add a default sort order for a table
    sql("ALTER TABLE %s WRITE ORDERED BY c2", tableName);

    // this creates 2 files under non-partitioned table due to sort order.
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // When the strategy is set to 'sort' but the sort order is not specified,
    // use table's default sort order.
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', "
                + "options => map('min-input-files','2'))",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 2 data files and add 1 data files",
        row(2, 1),
        Arrays.copyOf(output.get(0), 2));
    // verify rewritten bytes separately
    assertThat(output.get(0)).hasSize(4);
    assertThat(output.get(0)[2])
        .isInstanceOf(Long.class)
        .isEqualTo(Long.valueOf(snapshotSummary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)));

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testRewriteWithUntranslatedOrUnconvertedFilter() {
    createTable();
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', where => 'substr(encode(c2, \"utf-8\"), 2) = \"fo\"')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot translate Spark expression");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', where => 'substr(c2, 2) = \"fo\"')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot convert Spark filter");
  }

  @TestTemplate
  public void testRewriteDataFilesSummary() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    sql("CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    Map<String, String> summary = snapshotSummary();
    assertThat(summary)
        .containsKey(CatalogProperties.APP_ID)
        .containsEntry(EnvironmentContext.ENGINE_NAME, "spark")
        .hasEntrySatisfying(
            EnvironmentContext.ENGINE_VERSION, v -> assertThat(v).startsWith("4.0"));
  }

  private void createTable() {
    sql("CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg", tableName);
  }

  private void createPartitionTable() {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) "
            + "USING iceberg "
            + "PARTITIONED BY (c2) "
            + "TBLPROPERTIES ('%s' '%s')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.WRITE_DISTRIBUTION_MODE_NONE);
  }

  private void createBucketPartitionTable() {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2)) "
            + "TBLPROPERTIES ('%s' '%s')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.WRITE_DISTRIBUTION_MODE_NONE);
  }

  private void insertData(int filesCount) {
    insertData(tableName, filesCount);
  }

  private void insertData(String table, int filesCount) {
    ThreeColumnRecord record1 = new ThreeColumnRecord(1, "foo", null);
    ThreeColumnRecord record2 = new ThreeColumnRecord(2, "bar", null);

    List<ThreeColumnRecord> records = Lists.newArrayList();
    IntStream.range(0, filesCount / 2)
        .forEach(
            i -> {
              records.add(record1);
              records.add(record2);
            });

    Dataset<Row> df =
        spark.createDataFrame(records, ThreeColumnRecord.class).repartition(filesCount);
    try {
      df.writeTo(table).append();
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, String> snapshotSummary() {
    return snapshotSummary(tableIdent);
  }

  private Map<String, String> snapshotSummary(TableIdentifier tableIdentifier) {
    return validationCatalog.loadTable(tableIdentifier).currentSnapshot().summary();
  }

  private List<Object[]> currentData() {
    return currentData(tableName);
  }

  private List<Object[]> currentData(String table) {
    return rowsToJava(spark.sql("SELECT * FROM " + table + " order by c1, c2, c3").collectAsList());
  }
}
