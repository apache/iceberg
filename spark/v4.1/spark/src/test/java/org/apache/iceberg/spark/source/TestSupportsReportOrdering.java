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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SortExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSupportsReportOrdering extends TestBaseWithCatalog {

  private static final Map<String, String> ENABLED_ORDERING_SQL_CONF = orderingConfig(true);
  private static final Map<String, String> DISABLED_ORDERING_SQL_CONF = orderingConfig(false);

  private static Map<String, String> orderingConfig(boolean preserveOrdering) {
    return ImmutableMap.<String, String>builder()
        .put(SparkSQLProperties.PRESERVE_DATA_ORDERING, String.valueOf(preserveOrdering))
        .put(SparkSQLProperties.PRESERVE_DATA_GROUPING, "true")
        .put("spark.sql.autoBroadcastJoinThreshold", "-1")
        .put("spark.sql.adaptive.enabled", "false")
        .put("spark.sql.sources.v2.bucketing.enabled", "true")
        .put("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
        .put("spark.sql.requireAllClusterKeysForCoPartition", "false")
        .buildOrThrow();
  }

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName("table_source"));
    spark.conf().unset(SparkSQLProperties.PRESERVE_DATA_ORDERING);
  }

  @TestTemplate
  public void testMergingMultipleSortedFiles() throws NoSuchTableException {
    Table table = createSimpleTable(tableName);
    setSortOrder(table, "id");

    writeBatches(
        tableName,
        SimpleRecord.class,
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b")),
        ImmutableList.of(new SimpleRecord(3, "c"), new SimpleRecord(4, "d")),
        ImmutableList.of(new SimpleRecord(5, "e"), new SimpleRecord(6, "f")),
        ImmutableList.of(new SimpleRecord(7, "g"), new SimpleRecord(8, "h")));

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_ORDERING, "true");

    Dataset<Row> result =
        spark.sql(String.format("SELECT id, data FROM %s ORDER BY id", tableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows)
        .hasSize(8)
        .containsExactly(
            row(1, "a"),
            row(2, "b"),
            row(3, "c"),
            row(4, "d"),
            row(5, "e"),
            row(6, "f"),
            row(7, "g"),
            row(8, "h"));
  }

  @TestTemplate
  public void testDescendingSortOrder() throws NoSuchTableException {
    Table table = createSimpleTable(tableName);
    table.replaceSortOrder().desc("id").commit();

    writeBatches(
        tableName,
        SimpleRecord.class,
        ImmutableList.of(new SimpleRecord(10, "j"), new SimpleRecord(9, "i")),
        ImmutableList.of(new SimpleRecord(8, "h"), new SimpleRecord(7, "g")),
        ImmutableList.of(new SimpleRecord(6, "f"), new SimpleRecord(4, "d")));

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_ORDERING, "true");

    Dataset<Row> result = spark.sql(String.format("SELECT id FROM %s ORDER BY id DESC", tableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows).hasSize(6).containsExactly(row(10), row(9), row(8), row(7), row(6), row(4));
  }

  @TestTemplate
  public void testMultiColumnSortOrder() throws NoSuchTableException {
    Table table = createThreeColumnTable(tableName);
    setSortOrder(table, "c3", "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "a", "A"), new ThreeColumnRecord(3, "c", "A")),
        ImmutableList.of(new ThreeColumnRecord(2, "b", "A"), new ThreeColumnRecord(1, "a", "B")),
        ImmutableList.of(new ThreeColumnRecord(2, "b", "B"), new ThreeColumnRecord(3, "c", "B")));

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_ORDERING, "true");

    Dataset<Row> result =
        spark.sql(String.format("SELECT c3, c1, c2 FROM %s ORDER BY c3, c1", tableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows)
        .hasSize(6)
        .containsExactly(
            row("A", 1, "a"),
            row("A", 2, "b"),
            row("A", 3, "c"),
            row("B", 1, "a"),
            row("B", 2, "b"),
            row("B", 3, "c"));
  }

  @TestTemplate
  public void testSingleFileDoesNotRequireMerging() throws NoSuchTableException {
    Table table = createSimpleTable(tableName);
    setSortOrder(table, "id");

    List<SimpleRecord> batch = ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(batch, SimpleRecord.class).coalesce(1).writeTo(tableName).append();

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_ORDERING, "true");

    Dataset<Row> result = spark.sql(String.format("SELECT * FROM %s", tableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows).hasSize(2);
  }

  @TestTemplate
  public void testPartitionedTableWithMultipleFilesPerPartition() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (c3)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    setSortOrder(table, "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "a", "P1"), new ThreeColumnRecord(3, "c", "P1")),
        ImmutableList.of(new ThreeColumnRecord(2, "b", "P1"), new ThreeColumnRecord(4, "d", "P1")),
        ImmutableList.of(new ThreeColumnRecord(5, "e", "P2"), new ThreeColumnRecord(7, "g", "P2")),
        ImmutableList.of(new ThreeColumnRecord(6, "f", "P2"), new ThreeColumnRecord(8, "h", "P2")));

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_ORDERING, "true");

    Dataset<Row> p1Result =
        spark.sql(String.format("SELECT c1, c2 FROM %s WHERE c3 = 'P1' ORDER BY c1", tableName));
    List<Object[]> p1Rows = rowsToJava(p1Result.collectAsList());

    assertThat(p1Rows)
        .hasSize(4)
        .containsExactly(row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d"));

    Dataset<Row> p2Result =
        spark.sql(String.format("SELECT c1, c2 FROM %s WHERE c3 = 'P2' ORDER BY c1", tableName));
    List<Object[]> p2Rows = rowsToJava(p2Result.collectAsList());

    assertThat(p2Rows)
        .hasSize(4)
        .containsExactly(row(5, "e"), row(6, "f"), row(7, "g"), row(8, "h"));
  }

  @TestTemplate
  public void testOrderingNotReportedWhenDisabled() throws NoSuchTableException {
    Table table = createSimpleTable(tableName);
    setSortOrder(table, "id");

    List<SimpleRecord> batch = ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(batch, SimpleRecord.class).coalesce(1).writeTo(tableName).append();

    spark.conf().unset(SparkSQLProperties.PRESERVE_DATA_ORDERING);

    Dataset<Row> result = spark.sql(String.format("SELECT * FROM %s", tableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows).hasSize(2);
  }

  @TestTemplate
  public void testOrderingNotReportedForUnsortedTable() throws NoSuchTableException {
    createSimpleTable(tableName);

    List<SimpleRecord> batch = ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(batch, SimpleRecord.class).coalesce(1).writeTo(tableName).append();

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_ORDERING, "true");

    Dataset<Row> result = spark.sql(String.format("SELECT * FROM %s", tableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows).hasSize(2);
  }

  @TestTemplate
  public void testSortRequiredWhenOrderingNotReported() throws NoSuchTableException {
    Table table = createSimpleTable(tableName);
    setSortOrder(table, "id");

    writeBatches(
        tableName,
        SimpleRecord.class,
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b")),
        ImmutableList.of(new SimpleRecord(3, "c"), new SimpleRecord(4, "d")));

    spark.conf().unset(SparkSQLProperties.PRESERVE_DATA_ORDERING);

    SparkPlan plan =
        executeAndKeepPlan(String.format("SELECT id, data FROM %s ORDER BY id", tableName));

    List<SortExec> sorts = collectPlans(plan, SortExec.class);

    assertThat(sorts).isNotEmpty();
  }

  @TestTemplate
  public void testSortMergeJoinWithSortedTables() throws NoSuchTableException {
    createBucketedTable(tableName, "c1");
    createBucketedTable(tableName("table_source"), "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "a", "X"), new ThreeColumnRecord(2, "b", "X")),
        ImmutableList.of(new ThreeColumnRecord(3, "c", "X"), new ThreeColumnRecord(4, "d", "X")));

    writeBatches(
        tableName("table_source"),
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "A", "Y"), new ThreeColumnRecord(2, "B", "Y")),
        ImmutableList.of(new ThreeColumnRecord(3, "C", "Y"), new ThreeColumnRecord(4, "D", "Y")));

    assertPlanWithoutSort(
        0,
        2,
        null,
        "SELECT t1.c1, t1.c2, t2.c2 FROM %s t1 JOIN %s t2 ON t1.c1 = t2.c1",
        tableName,
        tableName("table_source"));
  }

  @TestTemplate
  public void testMergeWithSortedBucketedTables() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('write.merge.mode' = 'merge-on-read', '%s' = '%d')",
        tableName, TableProperties.SPLIT_SIZE, 1024);

    Table targetTable = validationCatalog.loadTable(tableIdent);
    setSortOrder(targetTable, "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(1, "old1", "data1"), new ThreeColumnRecord(2, "old2", "data2")),
        ImmutableList.of(
            new ThreeColumnRecord(3, "old3", "data3"), new ThreeColumnRecord(4, "old4", "data4")));

    String sourceTableName = tableName("table_source");
    TableIdentifier sourceTableIdent = TableIdentifier.of(Namespace.of("default"), "table_source");
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        sourceTableName, TableProperties.SPLIT_SIZE, 1024);

    Table sourceTable = validationCatalog.loadTable(sourceTableIdent);
    setSortOrder(sourceTable, "c1");

    writeBatches(
        sourceTableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(2, "new2", "data2"), new ThreeColumnRecord(3, "new3", "data3")),
        ImmutableList.of(
            new ThreeColumnRecord(5, "new5", "data5"), new ThreeColumnRecord(6, "new6", "data6")));

    refreshTables(tableName, sourceTableName);

    validationCatalog.loadTable(tableIdent).refresh();
    validationCatalog.loadTable(sourceTableIdent).refresh();

    assertPlanWithoutSort(
        1,
        3,
        this::verifyMergeResults,
        "MERGE INTO %s t USING %s s ON t.c1 = s.c1 "
            + "WHEN MATCHED THEN UPDATE SET t.c2 = s.c2, t.c3 = s.c3 "
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName,
        sourceTableName);
  }

  @TestTemplate
  public void testHistoricalSortOrderInJoin() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        tableName, TableProperties.SPLIT_SIZE, 1024);

    Table table1 = validationCatalog.loadTable(tableIdent);
    setSortOrder(table1, "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "a", "X"), new ThreeColumnRecord(2, "b", "X")),
        ImmutableList.of(new ThreeColumnRecord(3, "c", "X"), new ThreeColumnRecord(4, "d", "X")));

    table1.replaceSortOrder().asc("c2").asc("c1").commit();

    String table2Name = tableName("table_source");
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        table2Name, TableProperties.SPLIT_SIZE, 1024);

    TableIdentifier table2Ident = TableIdentifier.of(Namespace.of("default"), "table_source");
    Table table2 = validationCatalog.loadTable(table2Ident);
    setSortOrder(table2, "c1");

    writeBatches(
        table2Name,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "A", "Y"), new ThreeColumnRecord(2, "B", "Y")),
        ImmutableList.of(new ThreeColumnRecord(3, "C", "Y"), new ThreeColumnRecord(4, "D", "Y")));

    table2.replaceSortOrder().asc("c2").asc("c1").commit();

    // Both tables have files with historical sort order [c1 ASC]
    // but current table sort order is [c2 ASC, c1 ASC]
    assertPlanWithoutSort(
        2,
        2,
        null,
        "SELECT t1.c1, t1.c2, t2.c2 FROM %s t1 JOIN %s t2 ON t1.c1 = t2.c1",
        tableName,
        table2Name);
  }

  @TestTemplate
  public void testMixedSortOrdersNoReporting() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        tableName, TableProperties.SPLIT_SIZE, 1024);

    Table table1 = validationCatalog.loadTable(tableIdent);
    setSortOrder(table1, "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "a", "X"), new ThreeColumnRecord(2, "b", "X")));

    table1.replaceSortOrder().asc("c2").asc("c1").commit();

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(3, "c", "X"), new ThreeColumnRecord(4, "d", "X")));

    String table2Name = tableName("table_source");
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        table2Name, TableProperties.SPLIT_SIZE, 1024);

    TableIdentifier table2Ident = TableIdentifier.of(Namespace.of("default"), "table_source");
    Table table2 = validationCatalog.loadTable(table2Ident);
    setSortOrder(table2, "c1");

    writeBatches(
        table2Name,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(1, "A", "Y"), new ThreeColumnRecord(2, "B", "Y")));

    table2.replaceSortOrder().asc("c2").asc("c1").commit();

    writeBatches(
        table2Name,
        ThreeColumnRecord.class,
        ImmutableList.of(new ThreeColumnRecord(3, "C", "Y"), new ThreeColumnRecord(4, "D", "Y")));

    assertPlanWithoutSort(
        2,
        2,
        null,
        "SELECT t1.c1, t1.c2, t2.c2 FROM %s t1 JOIN %s t2 ON t1.c1 = t2.c1",
        tableName,
        table2Name);
  }

  @TestTemplate
  public void testSPJWithDifferentPartitionAndSortKeys() throws NoSuchTableException {
    createBucketedTable(tableName, "c3", "c1");
    createBucketedTable(tableName("table_source"), "c3", "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(1, "a", "2024-01-01"),
            new ThreeColumnRecord(2, "b", "2024-01-02")),
        ImmutableList.of(
            new ThreeColumnRecord(1, "c", "2024-01-03"),
            new ThreeColumnRecord(2, "d", "2024-01-04")));

    writeBatches(
        tableName("table_source"),
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(1, "A", "2024-01-01"),
            new ThreeColumnRecord(2, "B", "2024-01-02")),
        ImmutableList.of(
            new ThreeColumnRecord(1, "C", "2024-01-03"),
            new ThreeColumnRecord(2, "D", "2024-01-04")));

    refreshTables(tableName, tableName("table_source"));

    assertPlanWithoutSort(
        0,
        2,
        null,
        "SELECT t1.c1, t1.c2, t2.c2 FROM %s t1 JOIN %s t2 ON t1.c3 = t2.c3 AND t1.c1 = t2.c1",
        tableName,
        tableName("table_source"));
  }

  @TestTemplate
  public void testHistoricalSortOrderInMerge() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('write.merge.mode' = 'merge-on-read', '%s' = '%d')",
        tableName, TableProperties.SPLIT_SIZE, 1024);

    Table targetTable = validationCatalog.loadTable(tableIdent);
    setSortOrder(targetTable, "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(1, "old1", "data1"), new ThreeColumnRecord(2, "old2", "data2")),
        ImmutableList.of(
            new ThreeColumnRecord(3, "old3", "data3"), new ThreeColumnRecord(4, "old4", "data4")));

    targetTable.replaceSortOrder().asc("c2").asc("c1").commit();

    String sourceTableName = tableName("table_source");
    TableIdentifier sourceTableIdent = TableIdentifier.of(Namespace.of("default"), "table_source");
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        sourceTableName, TableProperties.SPLIT_SIZE, 1024);

    Table sourceTable = validationCatalog.loadTable(sourceTableIdent);
    setSortOrder(sourceTable, "c1");

    writeBatches(
        sourceTableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(2, "new2", "data2"), new ThreeColumnRecord(3, "new3", "data3")),
        ImmutableList.of(
            new ThreeColumnRecord(5, "new5", "data5"), new ThreeColumnRecord(6, "new6", "data6")));

    sourceTable.replaceSortOrder().asc("c2").asc("c1").commit();

    refreshTables(tableName, sourceTableName);

    validationCatalog.loadTable(tableIdent).refresh();
    validationCatalog.loadTable(sourceTableIdent).refresh();

    // Files have historical sort order [c1 ASC] but tables have [c2 ASC, c1 ASC]
    assertPlanWithoutSort(
        3,
        3,
        this::verifyMergeResults,
        "MERGE INTO %s t USING %s s ON t.c1 = s.c1 "
            + "WHEN MATCHED THEN UPDATE SET t.c2 = s.c2, t.c3 = s.c3 "
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName,
        sourceTableName);
  }

  @TestTemplate
  public void testMergeOnReadWithDeleteFiles() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES("
            + "  'write.delete.mode' = 'merge-on-read', "
            + "  'write.merge.mode' = 'merge-on-read', "
            + "  'format-version' = '2', "
            + "  '%s' = '%d'"
            + ")",
        tableName, TableProperties.SPLIT_SIZE, 1024);

    Table targetTable = validationCatalog.loadTable(tableIdent);
    setSortOrder(targetTable, "c1");

    writeBatches(
        tableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(1, "old1", "data1"), new ThreeColumnRecord(2, "old2", "data2")),
        ImmutableList.of(
            new ThreeColumnRecord(3, "old3", "data3"), new ThreeColumnRecord(4, "old4", "data4")));

    sql("DELETE FROM %s WHERE c1 = 2", tableName);

    targetTable.refresh();
    long deleteFileCount =
        Iterables.size(targetTable.currentSnapshot().addedDeleteFiles(targetTable.io()));
    assertThat(deleteFileCount).isGreaterThan(0);

    String sourceTableName = tableName("table_source");
    TableIdentifier sourceTableIdent = TableIdentifier.of(Namespace.of("default"), "table_source");
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        sourceTableName, TableProperties.SPLIT_SIZE, 1024);

    Table sourceTable = validationCatalog.loadTable(sourceTableIdent);
    setSortOrder(sourceTable, "c1");

    writeBatches(
        sourceTableName,
        ThreeColumnRecord.class,
        ImmutableList.of(
            new ThreeColumnRecord(2, "new2", "data2"), new ThreeColumnRecord(3, "new3", "data3")),
        ImmutableList.of(
            new ThreeColumnRecord(5, "new5", "data5"), new ThreeColumnRecord(6, "new6", "data6")));

    refreshTables(tableName, sourceTableName);

    validationCatalog.loadTable(tableIdent).refresh();
    validationCatalog.loadTable(sourceTableIdent).refresh();

    assertPlanWithoutSort(
        1,
        3,
        this::verifyMergeResults,
        "MERGE INTO %s t USING %s s ON t.c1 = s.c1 "
            + "WHEN MATCHED THEN UPDATE SET t.c2 = s.c2, t.c3 = s.c3 "
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName,
        sourceTableName);
  }

  private Table createSimpleTable(String name) {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", name);
    return validationCatalog.loadTable(tableIdent);
  }

  private Table createThreeColumnTable(String name) {
    sql("CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) USING iceberg", name);
    return validationCatalog.loadTable(tableIdent);
  }

  private void createBucketedTable(String name, String... sortCols) {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, c1)) "
            + "TBLPROPERTIES('%s' = '%d')",
        name, TableProperties.SPLIT_SIZE, 1024);

    TableIdentifier ident =
        name.equals(tableName)
            ? tableIdent
            : TableIdentifier.of(Namespace.of("default"), "table_source");
    Table table = validationCatalog.loadTable(ident);

    if (sortCols.length > 0) {
      ReplaceSortOrder sortOrder = table.replaceSortOrder();
      for (String col : sortCols) {
        sortOrder = sortOrder.asc(col);
      }
      sortOrder.commit();
    }
  }

  @SafeVarargs
  private <T> void writeBatches(String tableName, Class<T> recordClass, List<T>... batches)
      throws NoSuchTableException {
    for (List<T> batch : batches) {
      spark.createDataFrame(batch, recordClass).coalesce(1).writeTo(tableName).append();
    }
  }

  private void setSortOrder(Table table, String... columns) {
    ReplaceSortOrder sortOrder = table.replaceSortOrder();
    for (String col : columns) {
      sortOrder = sortOrder.asc(col);
    }
    sortOrder.commit();
  }

  private void refreshTable(String table) {
    sql("REFRESH TABLE %s", table);
  }

  private void refreshTables(String... tables) {
    for (String table : tables) {
      refreshTable(table);
    }
  }

  private void verifyMergeResults(String targetTableName) {
    Dataset<Row> result =
        spark.sql(String.format("SELECT c1, c2, c3 FROM %s ORDER BY c1", targetTableName));
    List<Object[]> rows = rowsToJava(result.collectAsList());

    assertThat(rows)
        .hasSize(6)
        .containsExactly(
            row(1, "old1", "data1"), // unchanged
            row(2, "new2", "data2"), // updated from source
            row(3, "new3", "data3"), // updated from source
            row(4, "old4", "data4"), // unchanged
            row(5, "new5", "data5"), // inserted from source
            row(6, "new6", "data6")); // inserted from source
  }

  private void assertPlanWithoutSort(
      int expectedNumSortsWithOrdering,
      int expectedNumSortsWithoutOrdering,
      Consumer<String> dataVerification,
      String query,
      Object... args) {

    AtomicReference<List<Object[]>> rowsWithOrdering = new AtomicReference<>();
    AtomicReference<List<Object[]>> rowsWithoutOrdering = new AtomicReference<>();

    Table targetTable = validationCatalog.loadTable(tableIdent);
    long snapshotBeforeExecution = targetTable.currentSnapshot().snapshotId();

    withSQLConf(
        ENABLED_ORDERING_SQL_CONF,
        () -> {
          String plan = executeAndKeepPlan(query, args).toString();
          int actualNumSorts = StringUtils.countMatches(plan, "Sort [");
          assertThat(actualNumSorts)
              .as("Number of sorts with enabled ordering must match")
              .isEqualTo(expectedNumSortsWithOrdering);

          sql("REFRESH TABLE %s", tableName);
          validationCatalog.loadTable(tableIdent).refresh();

          if (dataVerification != null) {
            dataVerification.accept(tableName);
          } else {
            rowsWithOrdering.set(sql(query, args));
          }
        });

    sql(
        "CALL %s.system.rollback_to_snapshot('%s', %dL)",
        catalogName, tableName, snapshotBeforeExecution);

    sql("REFRESH TABLE %s", tableName);
    validationCatalog.loadTable(tableIdent).refresh();

    withSQLConf(
        DISABLED_ORDERING_SQL_CONF,
        () -> {
          String plan = executeAndKeepPlan(query, args).toString();
          int actualNumSorts = StringUtils.countMatches(plan, "Sort [");
          assertThat(actualNumSorts)
              .as("Number of sorts with disabled ordering must match")
              .isEqualTo(expectedNumSortsWithoutOrdering);

          sql("REFRESH TABLE %s", tableName);
          validationCatalog.loadTable(tableIdent).refresh();
          if (dataVerification != null) {
            dataVerification.accept(tableName);
          } else {
            rowsWithoutOrdering.set(sql(query, args));
          }
        });

    if (dataVerification == null) {
      assertEquals(
          "Sort elimination should not change query output",
          rowsWithoutOrdering.get(),
          rowsWithOrdering.get());
    }
  }
}
