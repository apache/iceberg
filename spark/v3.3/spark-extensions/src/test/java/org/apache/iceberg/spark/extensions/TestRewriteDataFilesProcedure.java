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

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestRewriteDataFilesProcedure extends SparkExtensionsTestBase {

  private static final String QUOTED_SPECIAL_CHARS_TABLE_NAME = "`table:with.special:chars`";

  public TestRewriteDataFilesProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
  }

  @Test
  public void testZOrderSortExpression() {
    List<ExtendedParser.RawOrderField> order =
        ExtendedParser.parseSortOrder(spark, "c1, zorder(c2, c3)");
    Assert.assertEquals("Should parse 2 order fields", 2, order.size());
    Assert.assertEquals(
        "First field should be a ref", "c1", ((NamedReference<?>) order.get(0).term()).name());
    Assert.assertTrue("Second field should be zorder", order.get(1).term() instanceof Zorder);
  }

  @Test
  public void testRewriteDataFilesInEmptyTable() {
    createTable();
    List<Object[]> output = sql("CALL %s.system.rewrite_data_files('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0)), output);
  }

  @Test
  public void testRewriteDataFilesOnPartitionTable() {
    createPartitionTable();
    // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    List<Object[]> output =
        sql("CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 2 data files (one per partition) ",
        ImmutableList.of(row(10, 2)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesOnNonPartitionTable() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    List<Object[]> output =
        sql("CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        ImmutableList.of(row(10, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
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
        ImmutableList.of(row(0, 0)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data should not change", expectedRecords, actualRecords);
  }

  @Test
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
        ImmutableList.of(row(10, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
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
        ImmutableList.of(row(10, 1)),
        output);

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

  @Test
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
        ImmutableList.of(row(5, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
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
        ImmutableList.of(row(5, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
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
        ImmutableList.of(row(5, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
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

  @Test
  public void testRewriteDataFilesWithInvalidInputs() {
    createTable();
    // create 2 files under non-partitioned table
    insertData(2);

    // Test for invalid strategy
    AssertHelpers.assertThrows(
        "Should reject calls with unsupported strategy error message",
        IllegalArgumentException.class,
        "unsupported strategy: temp. Only binpack or sort is supported",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files','2'), "
                    + "strategy => 'temp')",
                catalogName, tableIdent));

    // Test for sort_order with binpack strategy
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        IllegalArgumentException.class,
        "Cannot set strategy to sort, it has already been set",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'binpack', "
                    + "sort_order => 'c1 ASC NULLS FIRST')",
                catalogName, tableIdent));

    // Test for sort_order with invalid null order
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        IllegalArgumentException.class,
        "Unable to parse sortOrder:",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                    + "sort_order => 'c1 ASC none')",
                catalogName, tableIdent));

    // Test for sort_order with invalid sort direction
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        IllegalArgumentException.class,
        "Unable to parse sortOrder:",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                    + "sort_order => 'c1 none NULLS FIRST')",
                catalogName, tableIdent));

    // Test for sort_order with invalid column name
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        ValidationException.class,
        "Cannot find field 'col1' in struct:"
            + " struct<1: c1: optional int, 2: c2: optional string, 3: c3: optional string>",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                    + "sort_order => 'col1 DESC NULLS FIRST')",
                catalogName, tableIdent));

    // Test for sort_order with invalid filter column col1
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        IllegalArgumentException.class,
        "Cannot parse predicates in where option: col1 = 3",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', " + "where => 'col1 = 3')",
                catalogName, tableIdent));

    // Test for z_order with invalid column name
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        IllegalArgumentException.class,
        "Cannot find column 'col1' in table schema: "
            + "struct<1: c1: optional int, 2: c2: optional string, 3: c3: optional string>",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                    + "sort_order => 'zorder(col1)')",
                catalogName, tableIdent));

    // Test for z_order with sort_order
    AssertHelpers.assertThrows(
        "Should reject calls with error message",
        IllegalArgumentException.class,
        "Cannot mix identity sort columns and a Zorder sort expression:" + " c1,zorder(c2,c3)",
        () ->
            sql(
                "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', "
                    + "sort_order => 'c1,zorder(c2,c3)')",
                catalogName, tableIdent));
  }

  @Test
  public void testInvalidCasesForRewriteDataFiles() {
    AssertHelpers.assertThrows(
        "Should not allow mixed args",
        AnalysisException.class,
        "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.rewrite_data_files('n', table => 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class,
        "not found",
        () -> sql("CALL %s.custom.rewrite_data_files('n', 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls without all required args",
        AnalysisException.class,
        "Missing required parameters",
        () -> sql("CALL %s.system.rewrite_data_files()", catalogName));

    AssertHelpers.assertThrows(
        "Should reject duplicate arg names name",
        AnalysisException.class,
        "Duplicate procedure argument: table",
        () -> sql("CALL %s.system.rewrite_data_files(table => 't', table => 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with empty table identifier",
        IllegalArgumentException.class,
        "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.rewrite_data_files('')", catalogName));
  }

  @Test
  public void testBinPackTableWithSpecialChars() {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.HADOOP.catalogName()));

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
        ImmutableList.of(row(10, 1)),
        output);

    List<Object[]> actualRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);

    Assert.assertEquals("Table cache must be empty", 0, SparkTableCache.get().size());
  }

  @Test
  public void testSortTableWithSpecialChars() {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.HADOOP.catalogName()));

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
        ImmutableList.of(row(10, 1)),
        output);

    List<Object[]> actualRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);

    Assert.assertEquals("Table cache must be empty", 0, SparkTableCache.get().size());
  }

  @Test
  public void testZOrderTableWithSpecialChars() {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.HADOOP.catalogName()));

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
        ImmutableList.of(row(10, 1)),
        output);

    List<Object[]> actualRecords = currentData(tableName(QUOTED_SPECIAL_CHARS_TABLE_NAME));
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);

    Assert.assertEquals("Table cache must be empty", 0, SparkTableCache.get().size());
  }

  private void createTable() {
    sql("CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg", tableName);
  }

  private void createPartitionTable() {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg PARTITIONED BY (c2)",
        tableName);
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

  private List<Object[]> currentData() {
    return currentData(tableName);
  }

  private List<Object[]> currentData(String table) {
    return rowsToJava(spark.sql("SELECT * FROM " + table + " order by c1, c2, c3").collectAsList());
  }
}
