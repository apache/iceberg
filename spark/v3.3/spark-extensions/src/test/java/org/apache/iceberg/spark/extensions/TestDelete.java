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

import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.SnapshotSummary.ADD_POS_DELETE_FILES_PROP;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.DELETE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromIcebergTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.OptimizeMetadataOnlyDeleteFromIcebergTable;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestDelete extends SparkRowLevelOperationsTestBase {

  public TestDelete(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      Boolean vectorized,
      String distributionMode,
      String branch) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode, branch);
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS deleted_id");
    sql("DROP TABLE IF EXISTS deleted_dep");
  }

  @Test
  public void testDeleteWithoutScanningTable() throws Exception {
    createAndInitPartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(3, "hr"));
    createBranchIfNeeded();
    append(new Employee(1, "hardware"), new Employee(2, "hardware"));

    Table table = validationCatalog.loadTable(tableIdent);

    List<String> manifestLocations =
        SnapshotUtil.latestSnapshot(table, branch).allManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toList());

    withUnavailableLocations(
        manifestLocations,
        () -> {
          LogicalPlan parsed = parsePlan("DELETE FROM %s WHERE dep = 'hr'", commitTarget());

          DeleteFromIcebergTable analyzed =
              (DeleteFromIcebergTable) spark.sessionState().analyzer().execute(parsed);
          Assert.assertTrue("Should have rewrite plan", analyzed.rewritePlan().isDefined());

          DeleteFromIcebergTable optimized =
              (DeleteFromIcebergTable) OptimizeMetadataOnlyDeleteFromIcebergTable.apply(analyzed);
          Assert.assertTrue("Should discard rewrite plan", optimized.rewritePlan().isEmpty());
        });

    sql("DELETE FROM %s WHERE dep = 'hr'", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hardware"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testDeleteFileThenMetadataDelete() throws Exception {
    Assume.assumeFalse("Avro does not support metadata delete", fileFormat.equals("avro"));
    createAndInitUnpartitionedTable();
    createBranchIfNeeded();
    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", commitTarget());

    // MOR mode: writes a delete file as null cannot be deleted by metadata
    sql("DELETE FROM %s AS t WHERE t.id IS NULL", commitTarget());

    // Metadata Delete
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<DataFile> dataFilesBefore = TestHelpers.dataFiles(table, branch);

    sql("DELETE FROM %s AS t WHERE t.id = 1", commitTarget());

    List<DataFile> dataFilesAfter = TestHelpers.dataFiles(table, branch);
    Assert.assertTrue(
        "Data file should have been removed", dataFilesBefore.size() > dataFilesAfter.size());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testDeleteWithPartitionedTable() throws Exception {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (3, 'hr')", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'hardware'), (2, 'hardware')", tableName);

    // row level delete
    sql("DELETE FROM %s WHERE id = 1", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
    List<Row> rowLevelDeletePartitions =
        spark.sql("SELECT * FROM " + tableName + ".partitions ").collectAsList();
    Assert.assertEquals(
        "row level delete does not reduce number of partition", 2, rowLevelDeletePartitions.size());

    // partition aligned delete
    sql("DELETE FROM %s WHERE dep = 'hr'", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
    List<Row> actualPartitions =
        spark.sql("SELECT * FROM " + tableName + ".partitions ").collectAsList();
    Assert.assertEquals(
        "partition aligned delete results in 1 partition", 1, actualPartitions.size());
  }

  @Test
  public void testDeleteWithFalseCondition() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware')", tableName);
    createBranchIfNeeded();

    sql("DELETE FROM %s WHERE id = 1 AND id > 20", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testDeleteFromEmptyTable() {
    Assume.assumeFalse("Custom branch does not exist for empty table", "test".equals(branch));
    createAndInitUnpartitionedTable();

    sql("DELETE FROM %s WHERE id IN (1)", commitTarget());
    sql("DELETE FROM %s WHERE dep = 'hr'", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testDeleteFromNonExistingCustomBranch() {
    Assume.assumeTrue("Test only applicable to custom branch", "test".equals(branch));
    createAndInitUnpartitionedTable();

    Assertions.assertThatThrownBy(() -> sql("DELETE FROM %s WHERE id IN (1)", commitTarget()))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot use branch (does not exist): test");
  }

  @Test
  public void testExplain() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    sql("EXPLAIN DELETE FROM %s WHERE id <=> 1", commitTarget());

    sql("EXPLAIN DELETE FROM %s WHERE true", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", commitTarget()));
  }

  @Test
  public void testDeleteWithAlias() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    sql("DELETE FROM %s AS t WHERE t.id IS NULL", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testDeleteWithDynamicFileFiltering() throws NoSuchTableException {
    createAndInitPartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(3, "hr"));
    createBranchIfNeeded();
    append(new Employee(1, "hardware"), new Employee(2, "hardware"));

    sql("DELETE FROM %s WHERE id = 2", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "1", "1", "1");
    } else {
      validateMergeOnRead(currentSnapshot, "1", "1", null);
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }

  @Test
  public void testDeleteNonExistingRecords() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    sql("DELETE FROM %s AS t WHERE t.id > 10", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (fileFormat.equals("orc") || fileFormat.equals("parquet")) {
      validateDelete(currentSnapshot, "0", null);
    } else {
      if (mode(table) == COPY_ON_WRITE) {
        validateCopyOnWrite(currentSnapshot, "0", null, null);
      } else {
        validateMergeOnRead(currentSnapshot, "0", null, null);
      }
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void deleteSingleRecordProducesDeleteOperation() throws NoSuchTableException {
    createAndInitPartitionedTable();
    append(tableName, new Employee(1, "eng"), new Employee(2, "eng"), new Employee(3, "eng"));

    sql("DELETE FROM %s WHERE id = 2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSize(2);

    Snapshot currentSnapshot = table.currentSnapshot();

    if (mode(table) == COPY_ON_WRITE) {
      // this is an OverwriteFiles and produces "overwrite"
      validateCopyOnWrite(currentSnapshot, "1", "1", "1");
    } else {
      // this is a RowDelta that produces a "delete" instead of "overwrite"
      validateMergeOnRead(currentSnapshot, "1", "1", null);
      validateProperty(currentSnapshot, ADD_POS_DELETE_FILES_PROP, "1");
    }

    assertThat(sql("SELECT * FROM %s", tableName))
        .containsExactlyInAnyOrder(row(1, "eng"), row(3, "eng"));
  }

  @Test
  public void testDeleteWithoutCondition() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", commitTarget());
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", commitTarget());

    sql("DELETE FROM %s", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    // should be a delete instead of an overwrite as it is done through a metadata operation
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    validateDelete(currentSnapshot, "2", "3");

    assertEquals(
        "Should have expected rows", ImmutableList.of(), sql("SELECT * FROM %s", commitTarget()));
  }

  @Test
  public void testDeleteUsingMetadataWithComplexCondition() {
    createAndInitPartitionedTable();

    sql("INSERT INTO %s VALUES (1, 'dep1')", tableName);
    createBranchIfNeeded();
    sql("INSERT INTO %s VALUES (2, 'dep2')", commitTarget());
    sql("INSERT INTO %s VALUES (null, 'dep3')", commitTarget());

    sql(
        "DELETE FROM %s WHERE dep > 'dep2' OR dep = CAST(4 AS STRING) OR dep = 'dep2'",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    // should be a delete instead of an overwrite as it is done through a metadata operation
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    validateDelete(currentSnapshot, "2", "2");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "dep1")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testDeleteWithArbitraryPartitionPredicates() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", commitTarget());
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", commitTarget());

    // %% is an escaped version of %
    sql("DELETE FROM %s WHERE id = 10 OR dep LIKE '%%ware'", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    // should be an overwrite since cannot be executed using a metadata operation
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "1", "1", null);
    } else {
      validateMergeOnRead(currentSnapshot, "1", "1", null);
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testDeleteWithNonDeterministicCondition() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware')", tableName);
    createBranchIfNeeded();

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic expressions",
        AnalysisException.class,
        "nondeterministic expressions are only allowed",
        () -> sql("DELETE FROM %s WHERE id = 1 AND rand() > 0.5", commitTarget()));
  }

  @Test
  public void testDeleteWithFoldableConditions() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware')", tableName);
    createBranchIfNeeded();

    // should keep all rows and don't trigger execution
    sql("DELETE FROM %s WHERE false", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // should keep all rows and don't trigger execution
    sql("DELETE FROM %s WHERE 50 <> 50", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // should keep all rows and don't trigger execution
    sql("DELETE FROM %s WHERE 1 > null", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // should remove all rows
    sql("DELETE FROM %s WHERE 21 = 21", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));
  }

  @Test
  public void testDeleteWithNullConditions() {
    createAndInitPartitionedTable();

    sql(
        "INSERT INTO TABLE %s VALUES (0, null), (1, 'hr'), (2, 'hardware'), (null, 'hr')",
        tableName);
    createBranchIfNeeded();

    // should keep all rows as null is never equal to null
    sql("DELETE FROM %s WHERE dep = null", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    // null = 'software' -> null
    // should delete using metadata operation only
    sql("DELETE FROM %s WHERE dep = 'software'", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    // should delete using metadata operation only
    sql("DELETE FROM %s WHERE dep <=> NULL", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    validateDelete(currentSnapshot, "1", "1");
  }

  @Test
  public void testDeleteWithInAndNotInConditions() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    sql("DELETE FROM %s WHERE id IN (1, null)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql("DELETE FROM %s WHERE id NOT IN (null, 1)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql("DELETE FROM %s WHERE id NOT IN (1, 10)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testDeleteWithMultipleRowGroupsParquet() throws NoSuchTableException {
    Assume.assumeTrue(fileFormat.equalsIgnoreCase("parquet"));

    createAndInitPartitionedTable();

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')",
        tableName, PARQUET_ROW_GROUP_SIZE_BYTES, 100);
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')", tableName, SPLIT_SIZE, 100);

    List<Integer> ids = Lists.newArrayListWithCapacity(200);
    for (int id = 1; id <= 200; id++) {
      ids.add(id);
    }
    Dataset<Row> df =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("hr"));
    df.coalesce(1).writeTo(tableName).append();
    createBranchIfNeeded();

    Assert.assertEquals(200, spark.table(commitTarget()).count());

    // delete a record from one of two row groups and copy over the second one
    sql("DELETE FROM %s WHERE id IN (200, 201)", commitTarget());

    Assert.assertEquals(199, spark.table(commitTarget()).count());
  }

  @Test
  public void testDeleteWithConditionOnNestedColumn() {
    createAndInitNestedColumnsTable();

    sql("INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", 3, \"c2\", \"v1\"))", tableName);
    createBranchIfNeeded();
    sql("INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", 2, \"c2\", \"v2\"))", commitTarget());

    sql("DELETE FROM %s WHERE complex.c1 = id + 2", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2)),
        sql("SELECT id FROM %s", selectTarget()));

    sql("DELETE FROM %s t WHERE t.complex.c1 = id", commitTarget());
    assertEquals(
        "Should have expected rows", ImmutableList.of(), sql("SELECT id FROM %s", selectTarget()));
  }

  @Test
  public void testDeleteWithInSubquery() throws NoSuchTableException {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    createOrReplaceView("deleted_id", Arrays.asList(0, 1, null), Encoders.INT());
    createOrReplaceView("deleted_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    sql(
        "DELETE FROM %s WHERE id IN (SELECT * FROM deleted_id) AND dep IN (SELECT * from deleted_dep)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    append(new Employee(1, "hr"), new Employee(-1, "hr"));
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s WHERE id IS NULL OR id IN (SELECT value + 2 FROM deleted_id)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(1, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    append(new Employee(null, "hr"), new Employee(2, "hr"));
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(1, "hr"), row(2, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s WHERE id IN (SELECT value + 2 FROM deleted_id) AND dep = 'hr'",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(1, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testDeleteWithMultiColumnInSubquery() throws NoSuchTableException {
    createAndInitUnpartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(2, "hardware"), new Employee(null, "hr"));
    createBranchIfNeeded();

    List<Employee> deletedEmployees =
        Arrays.asList(new Employee(null, "hr"), new Employee(1, "hr"));
    createOrReplaceView("deleted_employee", deletedEmployees, Encoders.bean(Employee.class));

    sql("DELETE FROM %s WHERE (id, dep) IN (SELECT id, dep FROM deleted_employee)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testDeleteWithNotInSubquery() throws NoSuchTableException {
    createAndInitUnpartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(2, "hardware"), new Employee(null, "hr"));
    createBranchIfNeeded();

    createOrReplaceView("deleted_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("deleted_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    // the file filter subquery (nested loop lef-anti join) returns 0 records
    sql("DELETE FROM %s WHERE id NOT IN (SELECT * FROM deleted_id)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s WHERE id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s WHERE id NOT IN (SELECT * FROM deleted_id) OR dep IN ('software', 'hr')",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s t WHERE "
            + "id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL) AND "
            + "EXISTS (SELECT 1 FROM FROM deleted_dep WHERE t.dep = deleted_dep.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s t WHERE "
            + "id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL) OR "
            + "EXISTS (SELECT 1 FROM FROM deleted_dep WHERE t.dep = deleted_dep.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testDeleteOnNonIcebergTableNotSupported() {
    createOrReplaceView("testtable", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Delete is supported only for Iceberg tables",
        AnalysisException.class,
        "DELETE is only supported with v2 tables.",
        () -> sql("DELETE FROM %s WHERE c1 = -100", "testtable"));
  }

  @Test
  public void testDeleteWithExistSubquery() throws NoSuchTableException {
    createAndInitUnpartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(2, "hardware"), new Employee(null, "hr"));
    createBranchIfNeeded();

    createOrReplaceView("deleted_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("deleted_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    sql(
        "DELETE FROM %s t WHERE EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s t WHERE EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s t WHERE EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value) OR t.id IS NULL",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware")),
        sql("SELECT * FROM %s", selectTarget()));

    sql(
        "DELETE FROM %s t WHERE "
            + "EXISTS (SELECT 1 FROM deleted_id di WHERE t.id = di.value) AND "
            + "EXISTS (SELECT 1 FROM deleted_dep dd WHERE t.dep = dd.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testDeleteWithNotExistsSubquery() throws NoSuchTableException {
    createAndInitUnpartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(2, "hardware"), new Employee(null, "hr"));
    createBranchIfNeeded();

    createOrReplaceView("deleted_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("deleted_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    sql(
        "DELETE FROM %s t WHERE "
            + "NOT EXISTS (SELECT 1 FROM deleted_id di WHERE t.id = di.value + 2) AND "
            + "NOT EXISTS (SELECT 1 FROM deleted_dep dd WHERE t.dep = dd.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "DELETE FROM %s t WHERE NOT EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    String subquery = "SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2";
    sql("DELETE FROM %s t WHERE NOT EXISTS (%s) OR t.id = 1", commitTarget(), subquery);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testDeleteWithScalarSubquery() throws NoSuchTableException {
    createAndInitUnpartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(2, "hardware"), new Employee(null, "hr"));
    createBranchIfNeeded();

    createOrReplaceView("deleted_id", Arrays.asList(1, 100, null), Encoders.INT());

    // TODO: Spark does not support AQE and DPP with aggregates at the moment
    withSQLConf(
        ImmutableMap.of(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false"),
        () -> {
          sql("DELETE FROM %s t WHERE id <= (SELECT min(value) FROM deleted_id)", commitTarget());
          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(2, "hardware"), row(null, "hr")),
              sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
        });
  }

  @Test
  public void testDeleteThatRequiresGroupingBeforeWrite() throws NoSuchTableException {
    createAndInitPartitionedTable();

    append(tableName, new Employee(0, "hr"), new Employee(1, "hr"), new Employee(2, "hr"));
    createBranchIfNeeded();
    append(new Employee(0, "ops"), new Employee(1, "ops"), new Employee(2, "ops"));
    append(new Employee(0, "hr"), new Employee(1, "hr"), new Employee(2, "hr"));
    append(new Employee(0, "ops"), new Employee(1, "ops"), new Employee(2, "ops"));

    createOrReplaceView("deleted_id", Arrays.asList(1, 100), Encoders.INT());

    String originalNumOfShufflePartitions = spark.conf().get("spark.sql.shuffle.partitions");
    try {
      // set the num of shuffle partitions to 1 to ensure we have only 1 writing task
      spark.conf().set("spark.sql.shuffle.partitions", "1");

      sql("DELETE FROM %s t WHERE id IN (SELECT * FROM deleted_id)", commitTarget());
      Assert.assertEquals(
          "Should have expected num of rows", 8L, spark.table(commitTarget()).count());
    } finally {
      spark.conf().set("spark.sql.shuffle.partitions", originalNumOfShufflePartitions);
    }
  }

  @Test
  public synchronized void testDeleteWithSerializableIsolation() throws InterruptedException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitUnpartitionedTable();
    createOrReplaceView("deleted_id", Collections.singletonList(1), Encoders.INT());

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, DELETE_ISOLATION_LEVEL, "serializable");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicBoolean shouldAppend = new AtomicBoolean(true);

    // delete thread
    Future<?> deleteFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                sql("DELETE FROM %s WHERE id IN (SELECT * FROM deleted_id)", commitTarget());

                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              // load the table via the validation catalog to use another table instance
              Table table = validationCatalog.loadTable(tableIdent);

              GenericRecord record = GenericRecord.create(SnapshotUtil.schemaFor(table, branch));
              record.set(0, 1); // id
              record.set(1, "hr"); // dep

              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (shouldAppend.get() && barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                if (!shouldAppend.get()) {
                  return;
                }

                for (int numAppends = 0; numAppends < 5; numAppends++) {
                  DataFile dataFile = writeDataFile(table, ImmutableList.of(record));
                  AppendFiles appendFiles = table.newFastAppend().appendFile(dataFile);
                  if (branch != null) {
                    appendFiles.toBranch(branch);
                  }

                  appendFiles.commit();
                  sleep(10);
                }

                barrier.incrementAndGet();
              }
            });

    try {
      Assertions.assertThatThrownBy(deleteFuture::get)
          .isInstanceOf(ExecutionException.class)
          .cause()
          .isInstanceOf(SparkException.class)
          .cause()
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Found conflicting files that can contain");
    } finally {
      shouldAppend.set(false);
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }

  @Test
  public synchronized void testDeleteWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitUnpartitionedTable();
    createOrReplaceView("deleted_id", Collections.singletonList(1), Encoders.INT());

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, DELETE_ISOLATION_LEVEL, "snapshot");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicBoolean shouldAppend = new AtomicBoolean(true);

    // delete thread
    Future<?> deleteFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < 20; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                sql("DELETE FROM %s WHERE id IN (SELECT * FROM deleted_id)", commitTarget());

                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              // load the table via the validation catalog to use another table instance for inserts
              Table table = validationCatalog.loadTable(tableIdent);

              GenericRecord record = GenericRecord.create(SnapshotUtil.schemaFor(table, branch));
              record.set(0, 1); // id
              record.set(1, "hr"); // dep

              for (int numOperations = 0; numOperations < 20; numOperations++) {
                while (shouldAppend.get() && barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                if (!shouldAppend.get()) {
                  return;
                }

                for (int numAppends = 0; numAppends < 5; numAppends++) {
                  DataFile dataFile = writeDataFile(table, ImmutableList.of(record));
                  AppendFiles appendFiles = table.newFastAppend().appendFile(dataFile);
                  if (branch != null) {
                    appendFiles.toBranch(branch);
                  }

                  appendFiles.commit();
                  sleep(10);
                }

                barrier.incrementAndGet();
              }
            });

    try {
      deleteFuture.get();
    } finally {
      shouldAppend.set(false);
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }

  @Test
  public void testDeleteRefreshesRelationCache() throws NoSuchTableException {
    createAndInitPartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(3, "hr"));
    createBranchIfNeeded();
    append(new Employee(1, "hardware"), new Employee(2, "hardware"));

    Dataset<Row> query = spark.sql("SELECT * FROM " + commitTarget() + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have correct data",
        ImmutableList.of(row(1, "hardware"), row(1, "hr")),
        sql("SELECT * FROM tmp ORDER BY id, dep"));

    sql("DELETE FROM %s WHERE id = 1", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "2", "2", "2");
    } else {
      validateMergeOnRead(currentSnapshot, "2", "2", null);
    }
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", commitTarget()));

    assertEquals(
        "Should refresh the relation cache",
        ImmutableList.of(),
        sql("SELECT * FROM tmp ORDER BY id, dep"));

    spark.sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testDeleteWithMultipleSpecs() {
    createAndInitTable("id INT, dep STRING, category STRING");

    // write an unpartitioned file
    append(tableName, "{ \"id\": 1, \"dep\": \"hr\", \"category\": \"c1\"}");
    createBranchIfNeeded();

    // write a file partitioned by dep
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);
    append(
        commitTarget(),
        "{ \"id\": 2, \"dep\": \"hr\", \"category\": \"c1\" }\n"
            + "{ \"id\": 3, \"dep\": \"hr\", \"category\": \"c1\" }");

    // write a file partitioned by dep and category
    sql("ALTER TABLE %s ADD PARTITION FIELD category", tableName);
    append(commitTarget(), "{ \"id\": 5, \"dep\": \"hr\", \"category\": \"c1\"}");

    // write another file partitioned by dep
    sql("ALTER TABLE %s DROP PARTITION FIELD category", tableName);
    append(commitTarget(), "{ \"id\": 7, \"dep\": \"hr\", \"category\": \"c1\"}");

    sql("DELETE FROM %s WHERE id IN (1, 3, 5, 7)", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 5 snapshots", 5, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "3", "4", "1");
    } else {
      validateMergeOnRead(currentSnapshot, "3", "3", null);
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(2, "hr", "c1")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testDeleteToWapBranch() throws NoSuchTableException {
    Assume.assumeTrue("WAP branch only works for table identifier without branch", branch == null);

    createAndInitPartitionedTable();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    append(new Employee(0, "hr"), new Employee(1, "hr"), new Employee(2, "hr"));

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          sql("DELETE FROM %s t WHERE id=0", tableName);
          Assert.assertEquals(
              "Should have expected num of rows when reading table",
              2L,
              spark.table(tableName).count());
          Assert.assertEquals(
              "Should have expected num of rows when reading WAP branch",
              2L,
              spark.table(tableName + ".branch_wap").count());
          Assert.assertEquals(
              "Should not modify main branch", 3L, spark.table(tableName + ".branch_main").count());
        });

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          sql("DELETE FROM %s t WHERE id=1", tableName);
          Assert.assertEquals(
              "Should have expected num of rows when reading table with multiple writes",
              1L,
              spark.table(tableName).count());
          Assert.assertEquals(
              "Should have expected num of rows when reading WAP branch with multiple writes",
              1L,
              spark.table(tableName + ".branch_wap").count());
          Assert.assertEquals(
              "Should not modify main branch with multiple writes",
              3L,
              spark.table(tableName + ".branch_main").count());
        });
  }

  @Test
  public void testDeleteToWapBranchWithTableBranchIdentifier() throws NoSuchTableException {
    Assume.assumeTrue("Test must have branch name part in table identifier", branch != null);

    createAndInitPartitionedTable();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    append(tableName, new Employee(0, "hr"), new Employee(1, "hr"), new Employee(2, "hr"));
    createBranchIfNeeded();

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () ->
            Assertions.assertThatThrownBy(() -> sql("DELETE FROM %s t WHERE id=0", commitTarget()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                    String.format(
                        "Cannot write to both branch and WAP branch, but got branch [%s] and WAP branch [wap]",
                        branch)));
  }

  @Test
  public void testDeleteToCustomWapBranchWithoutWhereClause() throws NoSuchTableException {
    assumeThat(branch)
        .as("Run only if custom WAP branch is not main")
        .isNotNull()
        .isNotEqualTo(SnapshotRef.MAIN_BRANCH);

    createAndInitPartitionedTable();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    append(tableName, new Employee(0, "hr"), new Employee(1, "hr"), new Employee(2, "hr"));
    createBranchIfNeeded();

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, branch),
        () -> {
          sql("DELETE FROM %s t WHERE id=1", tableName);
          Assertions.assertThat(spark.table(tableName).count()).isEqualTo(2L);
          Assertions.assertThat(spark.table(tableName + ".branch_" + branch).count()).isEqualTo(2L);
          Assertions.assertThat(spark.table(tableName + ".branch_main").count())
              .as("Should not modify main branch")
              .isEqualTo(3L);
        });
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, branch),
        () -> {
          sql("DELETE FROM %s t", tableName);
          Assertions.assertThat(spark.table(tableName).count()).isEqualTo(0L);
          Assertions.assertThat(spark.table(tableName + ".branch_" + branch).count()).isEqualTo(0L);
          Assertions.assertThat(spark.table(tableName + ".branch_main").count())
              .as("Should not modify main branch")
              .isEqualTo(3L);
        });
  }

  // TODO: multiple stripes for ORC

  protected void createAndInitPartitionedTable() {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tableName);
    initTable();
  }

  protected void createAndInitUnpartitionedTable() {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg", tableName);
    initTable();
  }

  protected void createAndInitNestedColumnsTable() {
    sql("CREATE TABLE %s (id INT, complex STRUCT<c1:INT,c2:STRING>) USING iceberg", tableName);
    initTable();
  }

  protected void append(Employee... employees) throws NoSuchTableException {
    append(commitTarget(), employees);
  }

  protected void append(String target, Employee... employees) throws NoSuchTableException {
    List<Employee> input = Arrays.asList(employees);
    Dataset<Row> inputDF = spark.createDataFrame(input, Employee.class);
    inputDF.coalesce(1).writeTo(target).append();
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(DELETE_MODE, DELETE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }

  private LogicalPlan parsePlan(String query, Object... args) {
    try {
      return spark.sessionState().sqlParser().parsePlan(String.format(query, args));
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
}
