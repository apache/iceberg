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

import static org.apache.iceberg.DataOperations.OVERWRITE;
import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.UPDATE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE_DEFAULT;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
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
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestUpdate extends SparkRowLevelOperationsTestBase {

  public TestUpdate(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode,
      boolean fanoutEnabled,
      String branch,
      PlanningMode planningMode) {
    super(
        catalogName,
        implementation,
        config,
        fileFormat,
        vectorized,
        distributionMode,
        fanoutEnabled,
        branch,
        planningMode);
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS updated_id");
    sql("DROP TABLE IF EXISTS updated_dep");
    sql("DROP TABLE IF EXISTS deleted_employee");
  }

  @Test
  public void testUpdateWithVectorizedReads() {
    assumeThat(supportsVectorization()).isTrue();

    createAndInitTable(
        "id INT, value INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"value\": 100, \"dep\": \"hr\" }");

    SparkPlan plan = executeAndKeepPlan("UPDATE %s SET value = -1 WHERE id = 1", commitTarget());

    assertAllBatchScansVectorized(plan);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, -1, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testCoalesceUpdate() {
    createAndInitTable("id INT, dep STRING");

    String[] records = new String[100];
    for (int index = 0; index < 100; index++) {
      records[index] = String.format("{ \"id\": %d, \"dep\": \"hr\" }", index);
    }
    append(tableName, records);
    append(tableName, records);
    append(tableName, records);
    append(tableName, records);

    // set the open file cost large enough to produce a separate scan task per file
    // use range distribution to trigger a shuffle
    Map<String, String> tableProps =
        ImmutableMap.of(
            SPLIT_OPEN_FILE_COST,
            String.valueOf(Integer.MAX_VALUE),
            UPDATE_DISTRIBUTION_MODE,
            DistributionMode.RANGE.modeName());
    sql("ALTER TABLE %s SET TBLPROPERTIES (%s)", tableName, tablePropsAsString(tableProps));

    createBranchIfNeeded();

    // enable AQE and set the advisory partition size big enough to trigger combining
    // set the number of shuffle partitions to 200 to distribute the work across reducers
    // set the advisory partition size for shuffles small enough to ensure writes override it
    withSQLConf(
        ImmutableMap.of(
            SQLConf.SHUFFLE_PARTITIONS().key(),
            "200",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
            "true",
            SQLConf.COALESCE_PARTITIONS_ENABLED().key(),
            "true",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES().key(),
            "100",
            SparkSQLProperties.ADVISORY_PARTITION_SIZE,
            String.valueOf(256 * 1024 * 1024)),
        () -> {
          SparkPlan plan =
              executeAndKeepPlan("UPDATE %s SET id = -1 WHERE mod(id, 2) = 0", commitTarget());
          Assertions.assertThat(plan.toString()).contains("REBALANCE_PARTITIONS_BY_COL");
        });

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (mode(table) == COPY_ON_WRITE) {
      // CoW UPDATE requests the updated records to be range distributed by `_file`, `_pos`
      // every task has data for each of 200 reducers
      // AQE detects that all shuffle blocks are small and processes them in 1 task
      // otherwise, there would be 200 tasks writing to the table
      validateProperty(snapshot, SnapshotSummary.ADDED_FILES_PROP, "1");
    } else {
      // MoR UPDATE requests the deleted records to be range distributed by partition and `_file`
      // each task contains only 1 file and therefore writes only 1 shuffle block
      // that means 4 shuffle blocks are distributed among 200 reducers
      // AQE detects that all 4 shuffle blocks are small and processes them in 1 task
      // otherwise, there would be 4 tasks processing 1 shuffle block each
      validateProperty(snapshot, SnapshotSummary.ADDED_DELETE_FILES_PROP, "1");
    }

    Assert.assertEquals(
        "Row count must match",
        200L,
        scalarSql("SELECT COUNT(*) FROM %s WHERE id = -1", commitTarget()));
  }

  @Test
  public void testSkewUpdate() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    String[] records = new String[100];
    for (int index = 0; index < 100; index++) {
      records[index] = String.format("{ \"id\": %d, \"dep\": \"hr\" }", index);
    }
    append(tableName, records);
    append(tableName, records);
    append(tableName, records);
    append(tableName, records);

    // set the open file cost large enough to produce a separate scan task per file
    // use hash distribution to trigger a shuffle
    Map<String, String> tableProps =
        ImmutableMap.of(
            SPLIT_OPEN_FILE_COST,
            String.valueOf(Integer.MAX_VALUE),
            UPDATE_DISTRIBUTION_MODE,
            DistributionMode.HASH.modeName());
    sql("ALTER TABLE %s SET TBLPROPERTIES (%s)", tableName, tablePropsAsString(tableProps));

    createBranchIfNeeded();

    // enable AQE and set the advisory partition size small enough to trigger a split
    // set the number of shuffle partitions to 2 to only have 2 reducers
    // set the advisory partition size for shuffles big enough to ensure writes override it
    withSQLConf(
        ImmutableMap.of(
            SQLConf.SHUFFLE_PARTITIONS().key(),
            "2",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
            "true",
            SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED().key(),
            "true",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES().key(),
            "256MB",
            SparkSQLProperties.ADVISORY_PARTITION_SIZE,
            "100"),
        () -> {
          SparkPlan plan =
              executeAndKeepPlan("UPDATE %s SET id = -1 WHERE mod(id, 2) = 0", commitTarget());
          Assertions.assertThat(plan.toString()).contains("REBALANCE_PARTITIONS_BY_COL");
        });

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (mode(table) == COPY_ON_WRITE) {
      // CoW UPDATE requests the updated records to be clustered by `_file`
      // each task contains only 1 file and therefore writes only 1 shuffle block
      // that means 4 shuffle blocks are distributed among 2 reducers
      // AQE detects that all shuffle blocks are big and processes them in 4 independent tasks
      // otherwise, there would be 2 tasks processing 2 shuffle blocks each
      validateProperty(snapshot, SnapshotSummary.ADDED_FILES_PROP, "4");
    } else {
      // MoR UPDATE requests the deleted records to be clustered by `_spec_id` and `_partition`
      // all tasks belong to the same partition and therefore write only 1 shuffle block per task
      // that means there are 4 shuffle blocks, all assigned to the same reducer
      // AQE detects that all 4 shuffle blocks are big and processes them in 4 separate tasks
      // otherwise, there would be 1 task processing 4 shuffle blocks
      validateProperty(snapshot, SnapshotSummary.ADDED_DELETE_FILES_PROP, "4");
    }

    Assert.assertEquals(
        "Row count must match",
        200L,
        scalarSql("SELECT COUNT(*) FROM %s WHERE id = -1", commitTarget()));
  }

  @Test
  public void testExplain() {
    createAndInitTable("id INT, dep STRING");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    sql("EXPLAIN UPDATE %s SET dep = 'invalid' WHERE id <=> 1", commitTarget());

    sql("EXPLAIN UPDATE %s SET dep = 'invalid' WHERE true", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testUpdateEmptyTable() {
    Assume.assumeFalse("Custom branch does not exist for empty table", "test".equals(branch));
    createAndInitTable("id INT, dep STRING");

    sql("UPDATE %s SET dep = 'invalid' WHERE id IN (1)", commitTarget());
    sql("UPDATE %s SET id = -1 WHERE dep = 'hr'", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testUpdateNonExistingCustomBranch() {
    Assume.assumeTrue("Test only applicable to custom branch", "test".equals(branch));
    createAndInitTable("id INT, dep STRING");

    Assertions.assertThatThrownBy(
            () -> sql("UPDATE %s SET dep = 'invalid' WHERE id IN (1)", commitTarget()))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot use branch (does not exist): test");
  }

  @Test
  public void testUpdateWithAlias() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"a\" }");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("UPDATE %s AS t SET t.dep = 'invalid'", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "invalid")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testUpdateAlignsAssignments() {
    createAndInitTable("id INT, c1 INT, c2 INT");

    sql("INSERT INTO TABLE %s VALUES (1, 11, 111), (2, 22, 222)", tableName);
    createBranchIfNeeded();

    sql("UPDATE %s SET `c2` = c2 - 2, c1 = `c1` - 1 WHERE id <=> 1", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, 10, 109), row(2, 22, 222)),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testUpdateWithUnsupportedPartitionPredicate() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'software'), (2, 'hr')", tableName);
    createBranchIfNeeded();

    sql("UPDATE %s t SET `t`.`id` = -1 WHERE t.dep LIKE '%%r' ", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(1, "software")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testUpdateWithDynamicFileFiltering() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");
    createBranchIfNeeded();
    append(
        commitTarget(),
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    sql("UPDATE %s SET id = cast('-1' AS INT) WHERE id = 2", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "1", "1", "1");
    } else {
      validateMergeOnRead(currentSnapshot, "1", "1", "1");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", commitTarget()));
  }

  @Test
  public void testUpdateNonExistingRecords() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);
    createBranchIfNeeded();

    sql("UPDATE %s SET id = -1 WHERE id > 10", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "0", null, null);
    } else {
      validateMergeOnRead(currentSnapshot, "0", null, null);
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testUpdateWithoutCondition() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", commitTarget());
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", commitTarget());

    // set the num of shuffle partitions to 200 instead of default 4 to reduce the chance of hashing
    // records for multiple source files to one writing task (needed for a predictable num of output
    // files)
    withSQLConf(
        ImmutableMap.of(SQLConf.SHUFFLE_PARTITIONS().key(), "200"),
        () -> {
          sql("UPDATE %s SET id = -1", commitTarget());
        });

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);

    Assert.assertEquals("Operation must match", OVERWRITE, currentSnapshot.operation());
    if (mode(table) == COPY_ON_WRITE) {
      Assert.assertEquals("Operation must match", OVERWRITE, currentSnapshot.operation());
      validateProperty(currentSnapshot, CHANGED_PARTITION_COUNT_PROP, "2");
      validateProperty(currentSnapshot, DELETED_FILES_PROP, "3");
      validateProperty(currentSnapshot, ADDED_FILES_PROP, ImmutableSet.of("2", "3"));
    } else {
      validateMergeOnRead(currentSnapshot, "2", "2", "2");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(-1, "hr")),
        sql("SELECT * FROM %s ORDER BY dep ASC", selectTarget()));
  }

  @Test
  public void testUpdateWithNullConditions() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 0, \"dep\": null }\n"
            + "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }");
    createBranchIfNeeded();

    // should not update any rows as null is never equal to null
    sql("UPDATE %s SET id = -1 WHERE dep = NULL", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // should not update any rows the condition does not match any records
    sql("UPDATE %s SET id = -1 WHERE dep = 'software'", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // should update one matching row with a null-safe condition
    sql("UPDATE %s SET dep = 'invalid', id = -1 WHERE dep <=> NULL", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "invalid"), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testUpdateWithInAndNotInConditions() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    sql("UPDATE %s SET id = -1 WHERE id IN (1, null)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql("UPDATE %s SET id = 100 WHERE id NOT IN (null, 1)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql("UPDATE %s SET id = 100 WHERE id NOT IN (1, 10)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(100, "hardware"), row(100, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", selectTarget()));
  }

  @Test
  public void testUpdateWithMultipleRowGroupsParquet() throws NoSuchTableException {
    Assume.assumeTrue(fileFormat.equalsIgnoreCase("parquet"));

    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

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

    // update a record from one of two row groups and copy over the second one
    sql("UPDATE %s SET id = -1 WHERE id IN (200, 201)", commitTarget());

    Assert.assertEquals(200, spark.table(commitTarget()).count());
  }

  @Test
  public void testUpdateNestedStructFields() {
    createAndInitTable(
        "id INT, s STRUCT<c1:INT,c2:STRUCT<a:ARRAY<INT>,m:MAP<STRING, STRING>>>",
        "{ \"id\": 1, \"s\": { \"c1\": 2, \"c2\": { \"a\": [1,2], \"m\": { \"a\": \"b\"} } } } }");

    // update primitive, array, map columns inside a struct
    sql("UPDATE %s SET s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1)", commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-1, row(ImmutableList.of(-1), ImmutableMap.of("k", "v"))))),
        sql("SELECT * FROM %s", selectTarget()));

    // set primitive, array, map columns to NULL (proper casts should be in place)
    sql("UPDATE %s SET s.c1 = NULL, s.c2 = NULL WHERE id IN (1)", commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(null, null))),
        sql("SELECT * FROM %s", selectTarget()));

    // update all fields in a struct
    sql(
        "UPDATE %s SET s = named_struct('c1', 1, 'c2', named_struct('a', array(1), 'm', null))",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(1, row(ImmutableList.of(1), null)))),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testUpdateWithUserDefinedDistribution() {
    createAndInitTable("id INT, c2 INT, c3 INT");
    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(8, c3)", tableName);

    append(
        tableName,
        "{ \"id\": 1, \"c2\": 11, \"c3\": 1 }\n"
            + "{ \"id\": 2, \"c2\": 22, \"c3\": 1 }\n"
            + "{ \"id\": 3, \"c2\": 33, \"c3\": 1 }");
    createBranchIfNeeded();

    // request a global sort
    sql("ALTER TABLE %s WRITE ORDERED BY c2", tableName);
    sql("UPDATE %s SET c2 = -22 WHERE id NOT IN (1, 3)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, 11, 1), row(2, -22, 1), row(3, 33, 1)),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    // request a local sort
    sql("ALTER TABLE %s WRITE LOCALLY ORDERED BY id", tableName);
    sql("UPDATE %s SET c2 = -33 WHERE id = 3", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, 11, 1), row(2, -22, 1), row(3, -33, 1)),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    // request a hash distribution + local sort
    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY id", tableName);
    sql("UPDATE %s SET c2 = -11 WHERE id = 1", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, -11, 1), row(2, -22, 1), row(3, -33, 1)),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public synchronized void testUpdateWithSerializableIsolation() throws InterruptedException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));
    // if caching is off, the table is eagerly refreshed during runtime filtering
    // this can cause a validation exception as concurrent changes would be visible
    Assume.assumeTrue(cachingCatalogEnabled());

    createAndInitTable("id INT, dep STRING");

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, UPDATE_ISOLATION_LEVEL, "serializable");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicBoolean shouldAppend = new AtomicBoolean(true);

    // update thread
    Future<?> updateFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                sql("UPDATE %s SET id = -1 WHERE id = 1", commitTarget());

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
      Assertions.assertThatThrownBy(updateFuture::get)
          .isInstanceOf(ExecutionException.class)
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
  public synchronized void testUpdateWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));
    // if caching is off, the table is eagerly refreshed during runtime filtering
    // this can cause a validation exception as concurrent changes would be visible
    Assume.assumeTrue(cachingCatalogEnabled());

    createAndInitTable("id INT, dep STRING");

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, UPDATE_ISOLATION_LEVEL, "snapshot");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicBoolean shouldAppend = new AtomicBoolean(true);

    // update thread
    Future<?> updateFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < 20; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                sql("UPDATE %s SET id = -1 WHERE id = 1", tableName);

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
      updateFuture.get();
    } finally {
      shouldAppend.set(false);
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }

  @Test
  public void testUpdateWithInferredCasts() {
    createAndInitTable("id INT, s STRING", "{ \"id\": 1, \"s\": \"value\" }");

    sql("UPDATE %s SET s = -1 WHERE id = 1", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "-1")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testUpdateModifiesNullStruct() {
    createAndInitTable("id INT, s STRUCT<n1:INT,n2:INT>", "{ \"id\": 1, \"s\": null }");

    sql("UPDATE %s SET s.n1 = -1 WHERE id = 1", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, row(-1, null))),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testUpdateRefreshesRelationCache() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    append(
        commitTarget(),
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    Dataset<Row> query = spark.sql("SELECT * FROM " + commitTarget() + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have correct data",
        ImmutableList.of(row(1, "hardware"), row(1, "hr")),
        sql("SELECT * FROM tmp ORDER BY id, dep"));

    sql("UPDATE %s SET id = -1 WHERE id = 1", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "2", "2", "2");
    } else {
      validateMergeOnRead(currentSnapshot, "2", "2", "2");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(2, "hardware"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", commitTarget()));

    assertEquals(
        "Should refresh the relation cache",
        ImmutableList.of(),
        sql("SELECT * FROM tmp ORDER BY id, dep"));

    spark.sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testUpdateWithInSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    createOrReplaceView("updated_id", Arrays.asList(0, 1, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    sql(
        "UPDATE %s SET id = -1 WHERE "
            + "id IN (SELECT * FROM updated_id) AND "
            + "dep IN (SELECT * from updated_dep)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "UPDATE %s SET id = 5 WHERE id IS NULL OR id IN (SELECT value + 1 FROM updated_id)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(5, "hardware"), row(5, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));

    append(
        commitTarget(), "{ \"id\": null, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(-1, "hr"), row(2, "hr"), row(5, "hardware"), row(5, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", selectTarget()));

    sql(
        "UPDATE %s SET id = 10 WHERE id IN (SELECT value + 2 FROM updated_id) AND dep = 'hr'",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(-1, "hr"), row(5, "hardware"), row(5, "hr"), row(10, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", selectTarget()));
  }

  @Test
  public void testUpdateWithInSubqueryAndDynamicFileFiltering() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");
    createBranchIfNeeded();
    append(
        commitTarget(),
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    createOrReplaceView("updated_id", Arrays.asList(-1, 2), Encoders.INT());

    sql("UPDATE %s SET id = -1 WHERE id IN (SELECT * FROM updated_id)", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "1", "1", "1");
    } else {
      validateMergeOnRead(currentSnapshot, "1", "1", "1");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", commitTarget()));
  }

  @Test
  public void testUpdateWithSelfSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    sql(
        "UPDATE %s SET dep = 'x' WHERE id IN (SELECT id + 1 FROM %s)",
        commitTarget(), commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "x")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // TODO: Spark does not support AQE and DPP with aggregates at the moment
    withSQLConf(
        ImmutableMap.of(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false"),
        () -> {
          sql(
              "UPDATE %s SET dep = 'y' WHERE "
                  + "id = (SELECT count(*) FROM (SELECT DISTINCT id FROM %s) AS t)",
              commitTarget(), commitTarget());
          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(1, "hr"), row(2, "y")),
              sql("SELECT * FROM %s ORDER BY id", selectTarget()));
        });

    sql("UPDATE %s SET id = (SELECT id - 2 FROM %s WHERE id = 1)", commitTarget(), commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(-1, "y")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }

  @Test
  public void testUpdateWithMultiColumnInSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    List<Employee> deletedEmployees =
        Arrays.asList(new Employee(null, "hr"), new Employee(1, "hr"));
    createOrReplaceView("deleted_employee", deletedEmployees, Encoders.bean(Employee.class));

    sql(
        "UPDATE %s SET dep = 'x', id = -1 WHERE (id, dep) IN (SELECT id, dep FROM deleted_employee)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "x"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testUpdateWithNotInSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    createOrReplaceView("updated_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    // the file filter subquery (nested loop lef-anti join) returns 0 records
    sql("UPDATE %s SET id = -1 WHERE id NOT IN (SELECT * FROM updated_id)", commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "UPDATE %s SET id = -1 WHERE id NOT IN (SELECT * FROM updated_id WHERE value IS NOT NULL)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", selectTarget()));

    sql(
        "UPDATE %s SET id = 5 WHERE id NOT IN (SELECT * FROM updated_id) OR dep IN ('software', 'hr')",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(5, "hr"), row(5, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", selectTarget()));
  }

  @Test
  public void testUpdateWithExistSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    createOrReplaceView("updated_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("hr", null), Encoders.STRING());

    sql(
        "UPDATE %s t SET id = -1 WHERE EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "UPDATE %s t SET dep = 'x', id = -1 WHERE "
            + "EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value + 2)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "x"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));

    sql(
        "UPDATE %s t SET id = -2 WHERE "
            + "EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value) OR "
            + "t.id IS NULL",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-2, "hr"), row(-2, "x"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));

    sql(
        "UPDATE %s t SET id = 1 WHERE "
            + "EXISTS (SELECT 1 FROM updated_id ui WHERE t.id = ui.value) AND "
            + "EXISTS (SELECT 1 FROM updated_dep ud WHERE t.dep = ud.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-2, "x"), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }

  @Test
  public void testUpdateWithNotExistsSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    createOrReplaceView("updated_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("hr", "software"), Encoders.STRING());

    sql(
        "UPDATE %s t SET id = -1 WHERE NOT EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value + 2)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(1, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));

    sql(
        "UPDATE %s t SET id = 5 WHERE "
            + "NOT EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value) OR "
            + "t.id = 1",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(5, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));

    sql(
        "UPDATE %s t SET id = 10 WHERE "
            + "NOT EXISTS (SELECT 1 FROM updated_id ui WHERE t.id = ui.value) AND "
            + "EXISTS (SELECT 1 FROM updated_dep ud WHERE t.dep = ud.value)",
        commitTarget());
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(10, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }

  @Test
  public void testUpdateWithScalarSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    createOrReplaceView("updated_id", Arrays.asList(1, 100, null), Encoders.INT());

    // TODO: Spark does not support AQE and DPP with aggregates at the moment
    withSQLConf(
        ImmutableMap.of(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false"),
        () -> {
          sql(
              "UPDATE %s SET id = -1 WHERE id <= (SELECT min(value) FROM updated_id)",
              commitTarget());
          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
              sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
        });
  }

  @Test
  public void testUpdateThatRequiresGroupingBeforeWrite() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    append(
        tableName,
        "{ \"id\": 0, \"dep\": \"hr\" }\n"
            + "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    append(
        commitTarget(),
        "{ \"id\": 0, \"dep\": \"ops\" }\n"
            + "{ \"id\": 1, \"dep\": \"ops\" }\n"
            + "{ \"id\": 2, \"dep\": \"ops\" }");

    append(
        commitTarget(),
        "{ \"id\": 0, \"dep\": \"hr\" }\n"
            + "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }");

    append(
        commitTarget(),
        "{ \"id\": 0, \"dep\": \"ops\" }\n"
            + "{ \"id\": 1, \"dep\": \"ops\" }\n"
            + "{ \"id\": 2, \"dep\": \"ops\" }");

    createOrReplaceView("updated_id", Arrays.asList(1, 100), Encoders.INT());

    String originalNumOfShufflePartitions = spark.conf().get("spark.sql.shuffle.partitions");
    try {
      // set the num of shuffle partitions to 1 to ensure we have only 1 writing task
      spark.conf().set("spark.sql.shuffle.partitions", "1");

      sql("UPDATE %s t SET id = -1 WHERE id IN (SELECT * FROM updated_id)", commitTarget());
      Assert.assertEquals(
          "Should have expected num of rows", 12L, spark.table(commitTarget()).count());
    } finally {
      spark.conf().set("spark.sql.shuffle.partitions", originalNumOfShufflePartitions);
    }
  }

  @Test
  public void testUpdateWithVectorization() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 0, \"dep\": \"hr\" }\n"
            + "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }");
    createBranchIfNeeded();

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.VECTORIZATION_ENABLED, "true"),
        () -> {
          sql("UPDATE %s t SET id = -1", commitTarget());

          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(-1, "hr"), row(-1, "hr"), row(-1, "hr")),
              sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
        });
  }

  @Test
  public void testUpdateModifyPartitionSourceField() throws NoSuchTableException {
    createAndInitTable("id INT, dep STRING, country STRING");

    sql("ALTER TABLE %s ADD PARTITION FIELD bucket(4, id)", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    List<Integer> ids = Lists.newArrayListWithCapacity(100);
    for (int id = 1; id <= 100; id++) {
      ids.add(id);
    }

    Dataset<Row> df1 =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("hr"))
            .withColumn("country", lit("usa"));
    df1.coalesce(1).writeTo(tableName).append();
    createBranchIfNeeded();

    Dataset<Row> df2 =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("software"))
            .withColumn("country", lit("usa"));
    df2.coalesce(1).writeTo(commitTarget()).append();

    Dataset<Row> df3 =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("hardware"))
            .withColumn("country", lit("usa"));
    df3.coalesce(1).writeTo(commitTarget()).append();

    sql(
        "UPDATE %s SET id = -1 WHERE id IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19)",
        commitTarget());
    Assert.assertEquals(30L, scalarSql("SELECT count(*) FROM %s WHERE id = -1", selectTarget()));
  }

  @Test
  public void testUpdateWithStaticPredicatePushdown() {
    createAndInitTable("id INT, dep STRING");

    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    // add a data file to the 'software' partition
    append(tableName, "{ \"id\": 1, \"dep\": \"software\" }");
    createBranchIfNeeded();

    // add a data file to the 'hr' partition
    append(commitTarget(), "{ \"id\": 1, \"dep\": \"hr\" }");

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    String dataFilesCount = snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP);
    Assert.assertEquals("Must have 2 files before UPDATE", "2", dataFilesCount);

    // remove the data file from the 'hr' partition to ensure it is not scanned
    DataFile dataFile = Iterables.getOnlyElement(snapshot.addedDataFiles(table.io()));
    table.io().deleteFile(dataFile.path().toString());

    // disable dynamic pruning and rely only on static predicate pushdown
    withSQLConf(
        ImmutableMap.of(
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false",
            SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED().key(), "false"),
        () -> {
          sql("UPDATE %s SET id = -1 WHERE dep IN ('software') AND id == 1", commitTarget());
        });
  }

  @Test
  public void testUpdateWithInvalidUpdates() {
    createAndInitTable(
        "id INT, a ARRAY<STRUCT<c1:INT,c2:INT>>, m MAP<STRING,STRING>",
        "{ \"id\": 0, \"a\": null, \"m\": null }");

    Assertions.assertThatThrownBy(() -> sql("UPDATE %s SET a.c1 = 1", commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Updating nested fields is only supported for StructType");

    Assertions.assertThatThrownBy(() -> sql("UPDATE %s SET m.key = 'new_key'", commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Updating nested fields is only supported for StructType");
  }

  @Test
  public void testUpdateWithConflictingAssignments() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>", "{ \"id\": 0, \"s\": null }");

    Assertions.assertThatThrownBy(
            () -> sql("UPDATE %s t SET t.id = 1, t.c.n1 = 2, t.id = 2", commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Multiple assignments for 'id'");

    Assertions.assertThatThrownBy(
            () -> sql("UPDATE %s t SET t.c.n1 = 1, t.id = 2, t.c.n1 = 2", commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Multiple assignments for 'c.n1");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "UPDATE %s SET c.n1 = 1, c = named_struct('n1', 1, 'n2', named_struct('dn1', 1, 'dn2', 2))",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Conflicting assignments for 'c'");
  }

  @Test
  public void testUpdateWithInvalidAssignmentsAnsi() {
    createAndInitTable(
        "id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL",
        "{ \"id\": 0, \"s\": { \"n1\": 1, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");

    withSQLConf(
        ImmutableMap.of("spark.sql.storeAssignmentPolicy", "ansi"),
        () -> {
          Assertions.assertThatThrownBy(() -> sql("UPDATE %s t SET t.id = NULL", commitTarget()))
              .isInstanceOf(SparkException.class)
              .hasMessageContaining("Null value appeared in non-nullable field");

          Assertions.assertThatThrownBy(() -> sql("UPDATE %s t SET t.s.n1 = NULL", commitTarget()))
              .isInstanceOf(SparkException.class)
              .hasMessageContaining("Null value appeared in non-nullable field");

          Assertions.assertThatThrownBy(
                  () -> sql("UPDATE %s t SET t.s = named_struct('n1', 1)", commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column `s`.`n2`");

          Assertions.assertThatThrownBy(() -> sql("UPDATE %s t SET t.s.n1 = 'str'", commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot safely cast");

          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "UPDATE %s t SET t.s.n2 = named_struct('dn3', 1, 'dn1', 2)",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column `s`.`n2`.`dn2`");
        });
  }

  @Test
  public void testUpdateWithInvalidAssignmentsStrict() {
    createAndInitTable(
        "id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL",
        "{ \"id\": 0, \"s\": { \"n1\": 1, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");

    withSQLConf(
        ImmutableMap.of("spark.sql.storeAssignmentPolicy", "strict"),
        () -> {
          Assertions.assertThatThrownBy(() -> sql("UPDATE %s t SET t.id = NULL", commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot safely cast `id` \"VOID\" to \"INT\"");

          Assertions.assertThatThrownBy(() -> sql("UPDATE %s t SET t.s.n1 = NULL", commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot safely cast `s`.`n1` \"VOID\" to \"INT\"");

          Assertions.assertThatThrownBy(
                  () -> sql("UPDATE %s t SET t.s = named_struct('n1', 1)", commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column");

          Assertions.assertThatThrownBy(() -> sql("UPDATE %s t SET t.s.n1 = 'str'", commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot safely cast");

          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "UPDATE %s t SET t.s.n2 = named_struct('dn3', 1, 'dn1', 2)",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column");
        });
  }

  @Test
  public void testUpdateWithNonDeterministicCondition() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    Assertions.assertThatThrownBy(
            () -> sql("UPDATE %s SET id = -1 WHERE id = 1 AND rand() > 0.5", commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("The operator expects a deterministic expression");
  }

  @Test
  public void testUpdateOnNonIcebergTableNotSupported() {
    createOrReplaceView("testtable", "{ \"c1\": -100, \"c2\": -200 }");

    Assertions.assertThatThrownBy(() -> sql("UPDATE %s SET c1 = -1 WHERE c2 = 1", "testtable"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("UPDATE TABLE is not supported temporarily.");
  }

  @Test
  public void testUpdateToWAPBranch() {
    Assume.assumeTrue("WAP branch only works for table identifier without branch", branch == null);

    createAndInitTable(
        "id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"a\" }");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          sql("UPDATE %s SET dep='hr' WHERE dep='a'", tableName);
          Assert.assertEquals(
              "Should have expected num of rows when reading table",
              2L,
              sql("SELECT * FROM %s WHERE dep='hr'", tableName).size());
          Assert.assertEquals(
              "Should have expected num of rows when reading WAP branch",
              2L,
              sql("SELECT * FROM %s.branch_wap WHERE dep='hr'", tableName).size());
          Assert.assertEquals(
              "Should not modify main branch",
              1L,
              sql("SELECT * FROM %s.branch_main WHERE dep='hr'", tableName).size());
        });

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          sql("UPDATE %s SET dep='b' WHERE dep='hr'", tableName);
          Assert.assertEquals(
              "Should have expected num of rows when reading table with multiple writes",
              2L,
              sql("SELECT * FROM %s WHERE dep='b'", tableName).size());
          Assert.assertEquals(
              "Should have expected num of rows when reading WAP branch with multiple writes",
              2L,
              sql("SELECT * FROM %s.branch_wap WHERE dep='b'", tableName).size());
          Assert.assertEquals(
              "Should not modify main branch with multiple writes",
              0L,
              sql("SELECT * FROM %s.branch_main WHERE dep='b'", tableName).size());
        });
  }

  @Test
  public void testUpdateToWapBranchWithTableBranchIdentifier() {
    Assume.assumeTrue("Test must have branch name part in table identifier", branch != null);

    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () ->
            Assertions.assertThatThrownBy(
                    () -> sql("UPDATE %s SET dep='hr' WHERE dep='a'", commitTarget()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                    String.format(
                        "Cannot write to both branch and WAP branch, but got branch [%s] and WAP branch [wap]",
                        branch)));
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }
}
