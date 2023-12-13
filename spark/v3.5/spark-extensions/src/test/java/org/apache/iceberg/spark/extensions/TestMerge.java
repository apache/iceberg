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
import static org.apache.iceberg.TableProperties.MERGE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.time.Duration;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.SparkException;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMerge extends SparkRowLevelOperationsTestBase {

  public TestMerge(
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
    sql("DROP TABLE IF EXISTS source");
  }

  @Test
  public void testMergeWithAllClauses() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-5\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 2 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT * "
            + "WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN "
            + "  UPDATE SET dep = 'invalid' "
            + "WHEN NOT MATCHED BY SOURCE AND t.id = 4 THEN "
            + "  DELETE ",
        commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, "emp-id-1"), // updated (matched)
            // row(2, "emp-id-two) // deleted (matched)
            row(3, "invalid"), // updated (not matched by source)
            // row(4, "emp-id-4) // deleted (not matched by source)
            row(5, "emp-id-5")), // new
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithOneNotMatchedBySourceClause() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView("source", ImmutableList.of(1, 4), Encoders.INT());

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.value "
            + "WHEN NOT MATCHED BY SOURCE THEN "
            + "  DELETE ",
        commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, "emp-id-1"), // existing
            // row(2, "emp-id-2) // deleted (not matched by source)
            // row(3, "emp-id-3") // deleted (not matched by source)
            row(4, "emp-id-4")), // existing
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeNotMatchedBySourceClausesPartitionedTable() {
    createAndInitTable(
        "id INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"support\" }");

    createOrReplaceView("source", ImmutableList.of(1, 2), Encoders.INT());

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.value AND t.dep = 'hr' "
            + "WHEN MATCHED THEN "
            + " UPDATE SET dep = 'support' "
            + "WHEN NOT MATCHED BY SOURCE THEN "
            + "  UPDATE SET dep = 'invalid' ",
        commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, "support"), // updated (matched)
            row(2, "support"), // updated (matched)
            row(3, "invalid")), // updated (not matched by source)
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithVectorizedReads() {
    assumeThat(supportsVectorization()).isTrue();

    createAndInitTable(
        "id INT, value INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"value\": 100, \"dep\": \"hr\" }\n"
            + "{ \"id\": 6, \"value\": 600, \"dep\": \"software\" }");

    createOrReplaceView(
        "source",
        "id INT, value INT",
        "{ \"id\": 2, \"value\": 201 }\n"
            + "{ \"id\": 1, \"value\": 101 }\n"
            + "{ \"id\": 6, \"value\": 601 }");

    SparkPlan plan =
        executeAndKeepPlan(
            "MERGE INTO %s AS t USING source AS s "
                + "ON t.id == s.id "
                + "WHEN MATCHED AND t.id = 1 THEN "
                + "  UPDATE SET t.value = s.value "
                + "WHEN MATCHED AND t.id = 6 THEN "
                + "  DELETE "
                + "WHEN NOT MATCHED AND s.id = 2 THEN "
                + "  INSERT (id, value, dep) VALUES (s.id, s.value, 'invalid')",
            commitTarget());

    assertAllBatchScansVectorized(plan);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, 101, "hr"), // updated
            row(2, 201, "invalid")); // new

    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testCoalesceMerge() {
    createAndInitTable("id INT, salary INT, dep STRING");

    String[] records = new String[100];
    for (int index = 0; index < 100; index++) {
      records[index] = String.format("{ \"id\": %d, \"salary\": 100, \"dep\": \"hr\" }", index);
    }
    append(tableName, records);
    append(tableName, records);
    append(tableName, records);
    append(tableName, records);

    // set the open file cost large enough to produce a separate scan task per file
    // disable any write distribution
    Map<String, String> tableProps =
        ImmutableMap.of(
            SPLIT_OPEN_FILE_COST,
            String.valueOf(Integer.MAX_VALUE),
            MERGE_DISTRIBUTION_MODE,
            DistributionMode.NONE.modeName());
    sql("ALTER TABLE %s SET TBLPROPERTIES (%s)", tableName, tablePropsAsString(tableProps));

    createBranchIfNeeded();

    spark.range(0, 100).createOrReplaceTempView("source");

    // enable AQE and set the advisory partition big enough to trigger combining
    // set the number of shuffle partitions to 200 to distribute the work across reducers
    // disable broadcast joins to make sure the join triggers a shuffle
    // set the advisory partition size for shuffles small enough to ensure writes override it
    withSQLConf(
        ImmutableMap.of(
            SQLConf.SHUFFLE_PARTITIONS().key(),
            "200",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
            "-1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
            "true",
            SQLConf.COALESCE_PARTITIONS_ENABLED().key(),
            "true",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES().key(),
            "100",
            SparkSQLProperties.ADVISORY_PARTITION_SIZE,
            String.valueOf(256 * 1024 * 1024)),
        () -> {
          sql(
              "MERGE INTO %s t USING source "
                  + "ON t.id = source.id "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET salary = -1 ",
              commitTarget());
        });

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (mode(table) == COPY_ON_WRITE) {
      // CoW MERGE would perform a join on `id`
      // every task has data for each of 200 reducers
      // AQE detects that all shuffle blocks are small and processes them in 1 task
      // otherwise, there would be 200 tasks writing to the table
      validateProperty(currentSnapshot, SnapshotSummary.ADDED_FILES_PROP, "1");
    } else {
      // MoR MERGE would perform a join on `id`
      // every task has data for each of 200 reducers
      // AQE detects that all shuffle blocks are small and processes them in 1 task
      // otherwise, there would be 200 tasks writing to the table
      validateProperty(currentSnapshot, SnapshotSummary.ADDED_DELETE_FILES_PROP, "1");
    }

    Assert.assertEquals(
        "Row count must match",
        400L,
        scalarSql("SELECT COUNT(*) FROM %s WHERE salary = -1", commitTarget()));
  }

  @Test
  public void testSkewMerge() {
    createAndInitTable("id INT, salary INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    String[] records = new String[100];
    for (int index = 0; index < 100; index++) {
      records[index] = String.format("{ \"id\": %d, \"salary\": 100, \"dep\": \"hr\" }", index);
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
            MERGE_DISTRIBUTION_MODE,
            DistributionMode.HASH.modeName());
    sql("ALTER TABLE %s SET TBLPROPERTIES (%s)", tableName, tablePropsAsString(tableProps));

    createBranchIfNeeded();

    spark.range(0, 100).createOrReplaceTempView("source");

    // enable AQE and set the advisory partition size small enough to trigger a split
    // set the number of shuffle partitions to 2 to only have 2 reducers
    // set the min coalesce partition size small enough to avoid coalescing
    // set the advisory partition size for shuffles big enough to ensure writes override it
    withSQLConf(
        ImmutableMap.of(
            SQLConf.SHUFFLE_PARTITIONS().key(),
            "4",
            SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE().key(),
            "100",
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
              executeAndKeepPlan(
                  "MERGE INTO %s t USING source "
                      + "ON t.id = source.id "
                      + "WHEN MATCHED THEN "
                      + "  UPDATE SET salary = -1 ",
                  commitTarget());
          Assertions.assertThat(plan.toString()).contains("REBALANCE_PARTITIONS_BY_COL");
        });

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (mode(table) == COPY_ON_WRITE) {
      // CoW MERGE would perform a join on `id` and then cluster records by `dep`
      // the first shuffle distributes records into 4 shuffle partitions so that rows can be merged
      // after existing and new rows are merged, the data is clustered by `dep`
      // each task with merged data contains records for the same table partition
      // that means there are 4 shuffle blocks, all assigned to the same reducer
      // AQE detects that all shuffle blocks are big and processes them in 4 independent tasks
      // otherwise, there would be 1 task processing all 4 shuffle blocks
      validateProperty(currentSnapshot, SnapshotSummary.ADDED_FILES_PROP, "4");
    } else {
      // MoR MERGE would perform a join on `id` and then cluster data based on the partition
      // all tasks belong to the same partition and therefore write only 1 shuffle block per task
      // that means there are 4 shuffle blocks, all assigned to the same reducer
      // AQE detects that all 4 shuffle blocks are big and processes them in 4 separate tasks
      // otherwise, there would be 1 task processing 4 shuffle blocks
      validateProperty(currentSnapshot, SnapshotSummary.ADDED_DELETE_FILES_PROP, "4");
    }

    Assert.assertEquals(
        "Row count must match",
        400L,
        scalarSql("SELECT COUNT(*) FROM %s WHERE salary = -1", commitTarget()));
  }

  @Test
  public void testMergeConditionSplitIntoTargetPredicateAndJoinCondition() {
    createAndInitTable(
        "id INT, salary INT, dep STRING, sub_dep STRING",
        "PARTITIONED BY (dep, sub_dep)",
        "{ \"id\": 1, \"salary\": 100, \"dep\": \"d1\", \"sub_dep\": \"sd1\" }\n"
            + "{ \"id\": 6, \"salary\": 600, \"dep\": \"d6\", \"sub_dep\": \"sd6\" }");

    createOrReplaceView(
        "source",
        "id INT, salary INT, dep STRING, sub_dep STRING",
        "{ \"id\": 1, \"salary\": 101, \"dep\": \"d1\", \"sub_dep\": \"sd1\" }\n"
            + "{ \"id\": 2, \"salary\": 200, \"dep\": \"d2\", \"sub_dep\": \"sd2\" }\n"
            + "{ \"id\": 3, \"salary\": 300, \"dep\": \"d3\", \"sub_dep\": \"sd3\"  }");

    String query =
        String.format(
            "MERGE INTO %s AS t USING source AS s "
                + "ON t.id == s.id AND ((t.dep = 'd1' AND t.sub_dep IN ('sd1', 'sd3')) OR (t.dep = 'd6' AND t.sub_dep IN ('sd2', 'sd6'))) "
                + "WHEN MATCHED THEN "
                + "  UPDATE SET salary = s.salary "
                + "WHEN NOT MATCHED THEN "
                + "  INSERT *",
            commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);

    if (mode(table) == COPY_ON_WRITE) {
      checkJoinAndFilterConditions(
          query,
          "Join [id], [id], FullOuter",
          "((dep = 'd1' AND sub_dep IN ('sd1', 'sd3')) OR (dep = 'd6' AND sub_dep IN ('sd2', 'sd6')))");
    } else {
      checkJoinAndFilterConditions(
          query,
          "Join [id], [id], RightOuter",
          "((dep = 'd1' AND sub_dep IN ('sd1', 'sd3')) OR (dep = 'd6' AND sub_dep IN ('sd2', 'sd6')))");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, 101, "d1", "sd1"), // updated
            row(2, 200, "d2", "sd2"), // new
            row(3, 300, "d3", "sd3"), // new
            row(6, 600, "d6", "sd6")), // existing
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithStaticPredicatePushDown() {
    createAndInitTable("id BIGINT, dep STRING");

    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    // add a data file to the 'software' partition
    append(tableName, "{ \"id\": 1, \"dep\": \"software\" }");
    createBranchIfNeeded();

    // add a data file to the 'hr' partition
    append(commitTarget(), "{ \"id\": 1, \"dep\": \"hr\" }");

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    String dataFilesCount = snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP);
    Assert.assertEquals("Must have 2 files before MERGE", "2", dataFilesCount);

    createOrReplaceView(
        "source", "{ \"id\": 1, \"dep\": \"finance\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    // remove the data file from the 'hr' partition to ensure it is not scanned
    withUnavailableFiles(
        snapshot.addedDataFiles(table.io()),
        () -> {
          // disable dynamic pruning and rely only on static predicate pushdown
          withSQLConf(
              ImmutableMap.of(
                  SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false",
                  SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED().key(), "false"),
              () -> {
                sql(
                    "MERGE INTO %s t USING source "
                        + "ON t.id == source.id AND t.dep IN ('software') AND source.id < 10 "
                        + "WHEN MATCHED AND source.id = 1 THEN "
                        + "  UPDATE SET dep = source.dep "
                        + "WHEN NOT MATCHED THEN "
                        + "  INSERT (dep, id) VALUES (source.dep, source.id)",
                    commitTarget());
              });
        });

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1L, "finance"), // updated
            row(1L, "hr"), // kept
            row(2L, "hardware") // new
            );
    assertEquals(
        "Output should match",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }

  @Test
  public void testMergeIntoEmptyTargetInsertAllNonMatchingRows() {
    Assume.assumeFalse("Custom branch does not exist for empty table", "test".equals(branch));
    createAndInitTable("id INT, dep STRING");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // new
            row(2, "emp-id-2"), // new
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeIntoEmptyTargetInsertOnlyMatchingRows() {
    Assume.assumeFalse("Custom branch does not exist for empty table", "test".equals(branch));
    createAndInitTable("id INT, dep STRING");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED AND (s.id >=2) THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "emp-id-2"), // new
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithOnlyUpdateClause() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-six\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(6, "emp-id-six") // kept
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithOnlyUpdateClauseAndNullValues() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": null, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-six\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id AND t.id < 3 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "emp-id-one"), // kept
            row(1, "emp-id-1"), // updated
            row(6, "emp-id-six")); // kept
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithOnlyDeleteClause() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-one") // kept
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithMatchedAndNotMatchedClauses() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithAllCausesWithExplicitColumnSpecification() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET t.id = s.id, t.dep = s.dep "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT (t.id, t.dep) VALUES (s.id, s.dep)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithSourceCTE() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-6\" }");

    sql(
        "WITH cte1 AS (SELECT id + 1 AS id, dep FROM source) "
            + "MERGE INTO %s AS t USING cte1 AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 2 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 3 THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "emp-id-2"), // updated
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithSourceFromSetOps() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String derivedSource =
        "SELECT * FROM source WHERE id = 2 "
            + "UNION ALL "
            + "SELECT * FROM source WHERE id = 1 OR id = 6";

    sql(
        "MERGE INTO %s AS t USING (%s) AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        commitTarget(), derivedSource);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithOneMatchingBranchButMultipleSourceRowsForTargetRow() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"state\": \"on\" }\n"
            + "{ \"id\": 1, \"state\": \"off\" }\n"
            + "{ \"id\": 10, \"state\": \"on\" }");

    String errorMsg =
        "MERGE statement matched a single row from the target table with multiple rows of the source table.";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id == s.id "
                        + "WHEN MATCHED AND t.id = 6 THEN "
                        + "  DELETE "
                        + "WHEN NOT MATCHED THEN "
                        + "  INSERT (id, dep) VALUES (s.id, 'unknown')",
                    commitTarget()))
        .cause()
        .isInstanceOf(SparkRuntimeException.class)
        .hasMessageContaining(errorMsg);

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSource() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    List<Integer> sourceIds = Lists.newArrayList();
    for (int i = 0; i < 10_000; i++) {
      sourceIds.add(i);
    }
    Dataset<Integer> ds = spark.createDataset(sourceIds, Encoders.INT());
    ds.union(ds).createOrReplaceTempView("source");

    String errorMsg =
        "MERGE statement matched a single row from the target table with multiple rows of the source table.";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id == s.value "
                        + "WHEN MATCHED AND t.id = 1 THEN "
                        + "  UPDATE SET id = 10 "
                        + "WHEN MATCHED AND t.id = 6 THEN "
                        + "  DELETE "
                        + "WHEN NOT MATCHED AND s.value = 2 THEN "
                        + "  INSERT (id, dep) VALUES (s.value, null)",
                    commitTarget()))
        .cause()
        .isInstanceOf(SparkRuntimeException.class)
        .hasMessageContaining(errorMsg);

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void
      testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    List<Integer> sourceIds = Lists.newArrayList();
    for (int i = 0; i < 10_000; i++) {
      sourceIds.add(i);
    }
    Dataset<Integer> ds = spark.createDataset(sourceIds, Encoders.INT());
    ds.union(ds).createOrReplaceTempView("source");

    withSQLConf(
        ImmutableMap.of(SQLConf.PREFER_SORTMERGEJOIN().key(), "false"),
        () -> {
          String errorMsg =
              "MERGE statement matched a single row from the target table with multiple rows of the source table.";
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s AS t USING source AS s "
                              + "ON t.id == s.value "
                              + "WHEN MATCHED AND t.id = 1 THEN "
                              + "  UPDATE SET id = 10 "
                              + "WHEN MATCHED AND t.id = 6 THEN "
                              + "  DELETE "
                              + "WHEN NOT MATCHED AND s.value = 2 THEN "
                              + "  INSERT (id, dep) VALUES (s.value, null)",
                          commitTarget()))
              .cause()
              .isInstanceOf(SparkRuntimeException.class)
              .hasMessageContaining(errorMsg);
        });

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoEqualityCondition() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"emp-id-one\" }");

    List<Integer> sourceIds = Lists.newArrayList();
    for (int i = 0; i < 10_000; i++) {
      sourceIds.add(i);
    }
    Dataset<Integer> ds = spark.createDataset(sourceIds, Encoders.INT());
    ds.union(ds).createOrReplaceTempView("source");

    withSQLConf(
        ImmutableMap.of(SQLConf.PREFER_SORTMERGEJOIN().key(), "false"),
        () -> {
          String errorMsg =
              "MERGE statement matched a single row from the target table with multiple rows of the source table.";
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s AS t USING source AS s "
                              + "ON t.id > s.value "
                              + "WHEN MATCHED AND t.id = 1 THEN "
                              + "  UPDATE SET id = 10 "
                              + "WHEN MATCHED AND t.id = 6 THEN "
                              + "  DELETE "
                              + "WHEN NOT MATCHED AND s.value = 2 THEN "
                              + "  INSERT (id, dep) VALUES (s.value, null)",
                          commitTarget()))
              .cause()
              .isInstanceOf(SparkRuntimeException.class)
              .hasMessageContaining(errorMsg);
        });

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActions() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    List<Integer> sourceIds = Lists.newArrayList();
    for (int i = 0; i < 10_000; i++) {
      sourceIds.add(i);
    }
    Dataset<Integer> ds = spark.createDataset(sourceIds, Encoders.INT());
    ds.union(ds).createOrReplaceTempView("source");

    String errorMsg =
        "MERGE statement matched a single row from the target table with multiple rows of the source table.";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id == s.value "
                        + "WHEN MATCHED AND t.id = 1 THEN "
                        + "  UPDATE SET id = 10 "
                        + "WHEN MATCHED AND t.id = 6 THEN "
                        + "  DELETE",
                    commitTarget()))
        .cause()
        .isInstanceOf(SparkRuntimeException.class)
        .hasMessageContaining(errorMsg);
    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void
      testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActionsNoEqualityCondition() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"emp-id-one\" }");

    List<Integer> sourceIds = Lists.newArrayList();
    for (int i = 0; i < 10_000; i++) {
      sourceIds.add(i);
    }
    Dataset<Integer> ds = spark.createDataset(sourceIds, Encoders.INT());
    ds.union(ds).createOrReplaceTempView("source");

    String errorMsg =
        "MERGE statement matched a single row from the target table with multiple rows of the source table.";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id > s.value "
                        + "WHEN MATCHED AND t.id = 1 THEN "
                        + "  UPDATE SET id = 10 "
                        + "WHEN MATCHED AND t.id = 6 THEN "
                        + "  DELETE",
                    commitTarget()))
        .cause()
        .isInstanceOf(SparkRuntimeException.class)
        .hasMessageContaining(errorMsg);

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRow() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String errorMsg =
        "MERGE statement matched a single row from the target table with multiple rows of the source table.";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id == s.id "
                        + "WHEN MATCHED AND t.id = 1 THEN "
                        + "  UPDATE SET * "
                        + "WHEN MATCHED AND t.id = 6 THEN "
                        + "  DELETE "
                        + "WHEN NOT MATCHED AND s.id = 2 THEN "
                        + "  INSERT *",
                    commitTarget()))
        .cause()
        .isInstanceOf(SparkRuntimeException.class)
        .hasMessageContaining(errorMsg);

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testMergeWithUnconditionalDelete() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithSingleConditionalDelete() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String errorMsg =
        "MERGE statement matched a single row from the target table with multiple rows of the source table.";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id == s.id "
                        + "WHEN MATCHED AND t.id = 1 THEN "
                        + "  DELETE "
                        + "WHEN NOT MATCHED AND s.id = 2 THEN "
                        + "  INSERT *",
                    commitTarget()))
        .cause()
        .isInstanceOf(SparkRuntimeException.class)
        .hasMessageContaining(errorMsg);

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", selectTarget()));
  }

  @Test
  public void testMergeWithIdentityTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD identity(dep)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");
      createBranchIfNeeded();

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          commitTarget());

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", selectTarget()));

      removeTables();
    }
  }

  @Test
  public void testMergeWithDaysTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, ts TIMESTAMP");
      sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "id INT, ts TIMESTAMP",
          "{ \"id\": 1, \"ts\": \"2000-01-01 00:00:00\" }\n"
              + "{ \"id\": 6, \"ts\": \"2000-01-06 00:00:00\" }");
      createBranchIfNeeded();

      createOrReplaceView(
          "source",
          "id INT, ts TIMESTAMP",
          "{ \"id\": 2, \"ts\": \"2001-01-02 00:00:00\" }\n"
              + "{ \"id\": 1, \"ts\": \"2001-01-01 00:00:00\" }\n"
              + "{ \"id\": 6, \"ts\": \"2001-01-06 00:00:00\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          commitTarget());

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "2001-01-01 00:00:00"), // updated
              row(2, "2001-01-02 00:00:00") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT id, CAST(ts AS STRING) FROM %s ORDER BY id", selectTarget()));

      removeTables();
    }
  }

  @Test
  public void testMergeWithBucketTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD bucket(2, dep)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");
      createBranchIfNeeded();

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          commitTarget());

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", selectTarget()));

      removeTables();
    }
  }

  @Test
  public void testMergeWithTruncateTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD truncate(dep, 2)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");
      createBranchIfNeeded();

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          commitTarget());

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", selectTarget()));

      removeTables();
    }
  }

  @Test
  public void testMergeIntoPartitionedAndOrderedTable() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);
      sql("ALTER TABLE %s WRITE ORDERED BY (id)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");
      createBranchIfNeeded();

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          commitTarget());

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", selectTarget()));

      removeTables();
    }
  }

  @Test
  public void testSelfMerge() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    sql(
        "MERGE INTO %s t USING %s s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET v = 'x' "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget(), commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "x"), // updated
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testSelfMergeWithCaching() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    sql("CACHE TABLE %s", tableName);

    sql(
        "MERGE INTO %s t USING %s s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET v = 'x' "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget(), commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "x"), // updated
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", commitTarget()));
  }

  @Test
  public void testMergeWithSourceAsSelfSubquery() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView("source", Arrays.asList(1, null), Encoders.INT());

    sql(
        "MERGE INTO %s t USING (SELECT id AS value FROM %s r JOIN source ON r.id = source.value) s "
            + "ON t.id == s.value "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET v = 'x' "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES ('invalid', -1) ",
        commitTarget(), commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "x"), // updated
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public synchronized void testMergeWithSerializableIsolation() throws InterruptedException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));
    // if caching is off, the table is eagerly refreshed during runtime filtering
    // this can cause a validation exception as concurrent changes would be visible
    Assume.assumeTrue(cachingCatalogEnabled());

    createAndInitTable("id INT, dep STRING");
    createOrReplaceView("source", Collections.singletonList(1), Encoders.INT());

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, MERGE_ISOLATION_LEVEL, "serializable");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicBoolean shouldAppend = new AtomicBoolean(true);

    // merge thread
    Future<?> mergeFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                final int finalNumOperations = numOperations;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .untilAsserted(
                        () ->
                            assertThat(barrier.get())
                                .isGreaterThanOrEqualTo(finalNumOperations * 2));

                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.value "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET dep = 'x'",
                    commitTarget());

                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              // load the table via the validation catalog to use another table instance
              Table table = validationCatalog.loadTable(tableIdent);

              GenericRecord record = GenericRecord.create(table.schema());
              record.set(0, 1); // id
              record.set(1, "hr"); // dep

              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                final int finalNumOperations = numOperations;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .untilAsserted(
                        () -> {
                          assertThat(
                              !shouldAppend.get() || barrier.get() >= finalNumOperations * 2);
                        });

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
      Assertions.assertThatThrownBy(mergeFuture::get)
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
  public synchronized void testMergeWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));
    // if caching is off, the table is eagerly refreshed during runtime filtering
    // this can cause a validation exception as concurrent changes would be visible
    Assume.assumeTrue(cachingCatalogEnabled());

    createAndInitTable("id INT, dep STRING");
    createOrReplaceView("source", Collections.singletonList(1), Encoders.INT());

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, MERGE_ISOLATION_LEVEL, "snapshot");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    createBranchIfNeeded();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicBoolean shouldAppend = new AtomicBoolean(true);

    // merge thread
    Future<?> mergeFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < 20; numOperations++) {
                final int finalNumOperations = numOperations;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .untilAsserted(
                        () ->
                            assertThat(barrier.get())
                                .isGreaterThanOrEqualTo(finalNumOperations * 2));

                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.value "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET dep = 'x'",
                    commitTarget());

                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              // load the table via the validation catalog to use another table instance for inserts
              Table table = validationCatalog.loadTable(tableIdent);

              GenericRecord record = GenericRecord.create(table.schema());
              record.set(0, 1); // id
              record.set(1, "hr"); // dep

              for (int numOperations = 0; numOperations < 20; numOperations++) {
                final int finalNumOperations = numOperations;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .untilAsserted(
                        () -> {
                          assertThat(
                              !shouldAppend.get() || barrier.get() >= finalNumOperations * 2);
                        });

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
      mergeFuture.get();
    } finally {
      shouldAppend.set(false);
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }

  @Test
  public void testMergeWithExtraColumnsInSource() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"extra_col\": -1, \"v\": \"v1_1\" }\n"
            + "{ \"id\": 3, \"extra_col\": -1, \"v\": \"v3\" }\n"
            + "{ \"id\": 4, \"extra_col\": -1, \"v\": \"v4\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "v1_1"), // new
            row(2, "v2"), // kept
            row(3, "v3"), // new
            row(4, "v4") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithNullsInTargetAndSource() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": null, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 4, \"v\": \"v4\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "v1"), // kept
            row(null, "v1_1"), // new
            row(2, "v2"), // kept
            row(4, "v4") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", selectTarget()));
  }

  @Test
  public void testMergeWithNullSafeEquals() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": null, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 4, \"v\": \"v4\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id <=> source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "v1_1"), // updated
            row(2, "v2"), // kept
            row(4, "v4") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", selectTarget()));
  }

  @Test
  public void testMergeWithNullCondition() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": null, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 2, \"v\": \"v2_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id AND NULL "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "v1"), // kept
            row(null, "v1_1"), // new
            row(2, "v2"), // kept
            row(2, "v2_2") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", selectTarget()));
  }

  @Test
  public void testMergeWithNullActionConditions() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"v\": \"v1_1\" }\n"
            + "{ \"id\": 2, \"v\": \"v2_2\" }\n"
            + "{ \"id\": 3, \"v\": \"v3_3\" }");

    // all conditions are NULL and will never match any rows
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED AND source.id = 1 AND NULL THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN MATCHED AND source.v = 'v1_1' AND NULL THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND source.id = 3 AND NULL THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows1 =
        ImmutableList.of(
            row(1, "v1"), // kept
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows1, sql("SELECT * FROM %s ORDER BY v", selectTarget()));

    // only the update and insert conditions are NULL
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED AND source.id = 1 AND NULL THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN MATCHED AND source.v = 'v1_1' THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND source.id = 3 AND NULL THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows2 =
        ImmutableList.of(
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows2, sql("SELECT * FROM %s ORDER BY v", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleMatchingActions() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": 1, \"v\": \"v1_1\" }\n" + "{ \"id\": 2, \"v\": \"v2_2\" }");

    // the order of match actions is important in this case
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED AND source.id = 1 THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN MATCHED AND source.v = 'v1_1' THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "v1_1"), // updated (also matches the delete cond but update is first)
            row(2, "v2") // kept (matches neither the update nor the delete cond)
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleRowGroupsParquet() throws NoSuchTableException {
    Assume.assumeTrue(fileFormat.equalsIgnoreCase("parquet"));

    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')",
        tableName, PARQUET_ROW_GROUP_SIZE_BYTES, 100);
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')", tableName, SPLIT_SIZE, 100);

    createOrReplaceView("source", Collections.singletonList(1), Encoders.INT());

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
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.value "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET dep = 'x'",
        commitTarget());

    Assert.assertEquals(200, spark.table(commitTarget()).count());
  }

  @Test
  public void testMergeInsertOnly() {
    createAndInitTable(
        "id STRING, v STRING",
        "{ \"id\": \"a\", \"v\": \"v1\" }\n" + "{ \"id\": \"b\", \"v\": \"v2\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": \"a\", \"v\": \"v1_1\" }\n"
            + "{ \"id\": \"a\", \"v\": \"v1_2\" }\n"
            + "{ \"id\": \"c\", \"v\": \"v3\" }\n"
            + "{ \"id\": \"d\", \"v\": \"v4_1\" }\n"
            + "{ \"id\": \"d\", \"v\": \"v4_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row("a", "v1"), // kept
            row("b", "v2"), // kept
            row("c", "v3"), // new
            row("d", "v4_1"), // new
            row("d", "v4_2") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeInsertOnlyWithCondition() {
    createAndInitTable("id INTEGER, v INTEGER", "{ \"id\": 1, \"v\": 1 }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"v\": 11, \"is_new\": true }\n"
            + "{ \"id\": 2, \"v\": 21, \"is_new\": true }\n"
            + "{ \"id\": 2, \"v\": 22, \"is_new\": false }");

    // validate assignments are reordered to match the table attrs
    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED AND is_new = TRUE THEN "
            + "  INSERT (v, id) VALUES (s.v + 100, s.id)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, 1), // kept
            row(2, 121) // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeAlignsUpdateAndInsertActions() {
    createAndInitTable("id INT, a INT, b STRING", "{ \"id\": 1, \"a\": 2, \"b\": \"str\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"c1\": -2, \"c2\": \"new_str_1\" }\n"
            + "{ \"id\": 2, \"c1\": -20, \"c2\": \"new_str_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET b = c2, a = c1, t.id = source.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (b, a, id) VALUES (c2, c1, id)",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, -2, "new_str_1"), row(2, -20, "new_str_2")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeMixedCaseAlignsUpdateAndInsertActions() {
    createAndInitTable("id INT, a INT, b STRING", "{ \"id\": 1, \"a\": 2, \"b\": \"str\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"c1\": -2, \"c2\": \"new_str_1\" }\n"
            + "{ \"id\": 2, \"c1\": -20, \"c2\": \"new_str_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.iD == source.Id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET B = c2, A = c1, t.Id = source.ID "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (b, A, iD) VALUES (c2, c1, id)",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, -2, "new_str_1"), row(2, -20, "new_str_2")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, -2, "new_str_1")),
        sql("SELECT * FROM %s WHERE id = 1 ORDER BY id", selectTarget()));
    assertEquals(
        "Output should match",
        ImmutableList.of(row(2, -20, "new_str_2")),
        sql("SELECT * FROM %s WHERE b = 'new_str_2'ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeUpdatesNestedStructFields() {
    createAndInitTable(
        "id INT, s STRUCT<c1:INT,c2:STRUCT<a:ARRAY<INT>,m:MAP<STRING, STRING>>>",
        "{ \"id\": 1, \"s\": { \"c1\": 2, \"c2\": { \"a\": [1,2], \"m\": { \"a\": \"b\"} } } } }");
    createOrReplaceView("source", "{ \"id\": 1, \"c1\": -2 }");

    // update primitive, array, map columns inside a struct
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s.c1 = source.c1, t.s.c2.a = array(-1, -2), t.s.c2.m = map('k', 'v')",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-2, row(ImmutableList.of(-1, -2), ImmutableMap.of("k", "v"))))),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // set primitive, array, map columns to NULL (proper casts should be in place)
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s.c1 = NULL, t.s.c2 = NULL",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(null, null))),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // update all fields in a struct
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s = named_struct('c1', 100, 'c2', named_struct('a', array(1), 'm', map('x', 'y')))",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(100, row(ImmutableList.of(1), ImmutableMap.of("x", "y"))))),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithInferredCasts() {
    createAndInitTable("id INT, s STRING", "{ \"id\": 1, \"s\": \"value\" }");
    createOrReplaceView("source", "{ \"id\": 1, \"c1\": -2}");

    // -2 in source should be casted to "-2" in target
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s = source.c1",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, "-2")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeModifiesNullStruct() {
    createAndInitTable("id INT, s STRUCT<n1:INT,n2:INT>", "{ \"id\": 1, \"s\": null }");
    createOrReplaceView("source", "{ \"id\": 1, \"n1\": -10 }");

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s.n1 = s.n1",
        commitTarget());

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-10, null))),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testMergeRefreshesRelationCache() {
    createAndInitTable("id INT, name STRING", "{ \"id\": 1, \"name\": \"n1\" }");
    createOrReplaceView("source", "{ \"id\": 1, \"name\": \"n2\" }");

    Dataset<Row> query = spark.sql("SELECT name FROM " + commitTarget());
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have correct data", ImmutableList.of(row("n1")), sql("SELECT * FROM tmp"));

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.name = s.name",
        commitTarget());

    assertEquals(
        "View should have correct data", ImmutableList.of(row("n2")), sql("SELECT * FROM tmp"));

    spark.sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testMergeWithMultipleNotMatchedActions() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 0, \"dep\": \"emp-id-0\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED AND s.id = 1 THEN "
            + "  INSERT (dep, id) VALUES (s.dep, -1)"
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(-1, "emp-id-1"), // new
            row(0, "emp-id-0"), // kept
            row(2, "emp-id-2"), // new
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleConditionalNotMatchedActions() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 0, \"dep\": \"emp-id-0\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED AND s.id = 1 THEN "
            + "  INSERT (dep, id) VALUES (s.dep, -1)"
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(-1, "emp-id-1"), // new
            row(0, "emp-id-0"), // kept
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeResolvesColumnsByName() {
    createAndInitTable(
        "id INT, badge INT, dep STRING",
        "{ \"id\": 1, \"badge\": 1000, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 6, \"badge\": 6000, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "badge INT, id INT, dep STRING",
        "{ \"badge\": 1001, \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"badge\": 6006, \"id\": 6, \"dep\": \"emp-id-6\" }\n"
            + "{ \"badge\": 7007, \"id\": 7, \"dep\": \"emp-id-7\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT * ",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, 1001, "emp-id-1"), // updated
            row(6, 6006, "emp-id-6"), // updated
            row(7, 7007, "emp-id-7") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT id, badge, dep FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeShouldResolveWhenThereAreNoUnresolvedExpressionsOrColumns() {
    // ensures that MERGE INTO will resolve into the correct action even if no columns
    // or otherwise unresolved expressions exist in the query (testing SPARK-34962)
    createAndInitTable("id INT, dep STRING");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON 1 != 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        tableName);
    createBranchIfNeeded();

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // new
            row(2, "emp-id-2"), // new
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithTableWithNonNullableColumn() {
    createAndInitTable(
        "id INT NOT NULL, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT NOT NULL, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2")); // new
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithNonExistingColumns() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.invalid_col = s.c2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "A column or function parameter with name `t`.`invalid_col` cannot be resolved");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n2.invalid_col = s.c2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("No such struct field `invalid_col`");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n2.dn1 = s.c2 "
                        + "WHEN NOT MATCHED THEN "
                        + "  INSERT (id, invalid_col) VALUES (s.c1, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "A column or function parameter with name `invalid_col` cannot be resolved");
  }

  @Test
  public void testMergeWithInvalidColumnsInInsert() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n2.dn1 = s.c2 "
                        + "WHEN NOT MATCHED THEN "
                        + "  INSERT (id, c.n2) VALUES (s.c1, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("INSERT assignment keys cannot be nested fields");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n2.dn1 = s.c2 "
                        + "WHEN NOT MATCHED THEN "
                        + "  INSERT (id, id) VALUES (s.c1, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Multiple assignments for 'id'");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN NOT MATCHED THEN "
                        + "  INSERT (id) VALUES (s.c1)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("No assignment for 'c'");
  }

  @Test
  public void testMergeWithMissingOptionalColumnsInInsert() {
    createAndInitTable("id INT, value LONG", "{ \"id\": 1, \"value\": 100}");
    createOrReplaceView("source", "{ \"c1\": 2, \"c2\": 200 }");

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.c1 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (id) VALUES (s.c1)",
        commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, 100L), // existing
            row(2, null)), // new
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithInvalidUpdates() {
    createAndInitTable(
        "id INT, a ARRAY<STRUCT<c1:INT,c2:INT>>, m MAP<STRING,STRING>",
        "{ \"id\": 1, \"a\": [ { \"c1\": 2, \"c2\": 3 } ], \"m\": { \"k\": \"v\"} }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.a.c1 = s.c2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Updating nested fields is only supported for StructType");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.m.key = 'new_key'",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Updating nested fields is only supported for StructType");
  }

  @Test
  public void testMergeWithConflictingUpdates() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.id = 1, t.c.n1 = 2, t.id = 2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Multiple assignments for 'id");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n1 = 1, t.id = 2, t.c.n1 = 2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Multiple assignments for 'c.n1'");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET c.n1 = 1, c = named_struct('n1', 1, 'n2', named_struct('dn1', 1, 'dn2', 2))",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Conflicting assignments for 'c'");
  }

  @Test
  public void testMergeWithInvalidAssignmentsAnsi() {
    createAndInitTable(
        "id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL",
        "{ \"id\": 1, \"s\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView(
        "source",
        "c1 INT, c2 STRUCT<n1:INT NOT NULL> NOT NULL, c3 STRING NOT NULL, c4 STRUCT<dn3:INT,dn1:INT>",
        "{ \"c1\": 1, \"c2\": { \"n1\" : 1 }, \"c3\" : 'str', \"c4\": { \"dn3\": 1, \"dn1\": 2 } }");

    withSQLConf(
        ImmutableMap.of(SQLConf.STORE_ASSIGNMENT_POLICY().key(), "ansi"),
        () -> {
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.id = cast(NULL as int)",
                          commitTarget()))
              .isInstanceOf(SparkException.class)
              .hasMessageContaining("Null value appeared in non-nullable field");
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s.n1 = NULL",
                          commitTarget()))
              .isInstanceOf(SparkException.class)
              .hasMessageContaining("Null value appeared in non-nullable field");
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s = s.c2",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column `s`.`n2`");
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s.n1 = s.c3",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageEndingWith("Cannot safely cast `s`.`n1` \"STRING\" to \"INT\".");

          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s.n2 = s.c4",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column `s`.`n2`.`dn2`");
        });
  }

  @Test
  public void testMergeWithInvalidAssignmentsStrict() {
    createAndInitTable(
        "id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL",
        "{ \"id\": 1, \"s\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView(
        "source",
        "c1 INT, c2 STRUCT<n1:INT NOT NULL> NOT NULL, c3 STRING NOT NULL, c4 STRUCT<dn3:INT,dn1:INT>",
        "{ \"c1\": 1, \"c2\": { \"n1\" : 1 }, \"c3\" : 'str', \"c4\": { \"dn3\": 1, \"dn1\": 2 } }");

    withSQLConf(
        ImmutableMap.of(SQLConf.STORE_ASSIGNMENT_POLICY().key(), "strict"),
        () -> {
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.id = NULL",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot safely cast `id` \"VOID\" to \"INT\"");
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s.n1 = NULL",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot safely cast `s`.`n1` \"VOID\" to \"INT\"");

          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s = s.c2",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column `s`.`n2`");
          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s.n1 = s.c3",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageEndingWith("Cannot safely cast `s`.`n1` \"STRING\" to \"INT\".");

          Assertions.assertThatThrownBy(
                  () ->
                      sql(
                          "MERGE INTO %s t USING source s "
                              + "ON t.id == s.c1 "
                              + "WHEN MATCHED THEN "
                              + "  UPDATE SET t.s.n2 = s.c4",
                          commitTarget()))
              .isInstanceOf(AnalysisException.class)
              .hasMessageContaining("Cannot find data for the output column `s`.`n2`.`dn2`");
        });
  }

  @Test
  public void testMergeWithNonDeterministicConditions() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 AND rand() > t.id "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n1 = -1",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported SEARCH condition. Non-deterministic expressions are not allowed");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED AND rand() > t.id THEN "
                        + "  UPDATE SET t.c.n1 = -1",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported UPDATE condition. Non-deterministic expressions are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED AND rand() > t.id THEN "
                        + "  DELETE",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported DELETE condition. Non-deterministic expressions are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN NOT MATCHED AND rand() > c1 THEN "
                        + "  INSERT (id, c) VALUES (1, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported INSERT condition. Non-deterministic expressions are not allowed");
  }

  @Test
  public void testMergeWithAggregateExpressions() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 AND max(t.id) == 1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n1 = -1",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported SEARCH condition. Aggregates are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED AND sum(t.id) < 1 THEN "
                        + "  UPDATE SET t.c.n1 = -1",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported UPDATE condition. Aggregates are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED AND sum(t.id) THEN "
                        + "  DELETE",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported DELETE condition. Aggregates are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN NOT MATCHED AND sum(c1) < 1 THEN "
                        + "  INSERT (id, c) VALUES (1, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported INSERT condition. Aggregates are not allowed");
  }

  @Test
  public void testMergeWithSubqueriesInConditions() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 AND t.id < (SELECT max(c2) FROM source) "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET t.c.n1 = s.c2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported SEARCH condition. Subqueries are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED AND t.id < (SELECT max(c2) FROM source) THEN "
                        + "  UPDATE SET t.c.n1 = s.c2",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported UPDATE condition. Subqueries are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN MATCHED AND t.id NOT IN (SELECT c2 FROM source) THEN "
                        + "  DELETE",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported DELETE condition. Subqueries are not allowed");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.c1 "
                        + "WHEN NOT MATCHED AND s.c1 IN (SELECT c2 FROM source) THEN "
                        + "  INSERT (id, c) VALUES (1, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "MERGE operation contains unsupported INSERT condition. Subqueries are not allowed");
  }

  @Test
  public void testMergeWithTargetColumnsInInsertConditions() {
    createAndInitTable("id INT, c2 INT", "{ \"id\": 1, \"c2\": 2 }");
    createOrReplaceView("source", "{ \"id\": 1, \"value\": 11 }");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.id "
                        + "WHEN NOT MATCHED AND c2 = 1 THEN "
                        + "  INSERT (id, c2) VALUES (s.id, null)",
                    commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("A column or function parameter with name `c2` cannot be resolved");
  }

  @Test
  public void testMergeWithNonIcebergTargetTableNotSupported() {
    createOrReplaceView("target", "{ \"c1\": -100, \"c2\": -200 }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO target t USING source s "
                        + "ON t.c1 == s.c1 "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET *"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("MERGE INTO TABLE is not supported temporarily.");
  }

  /**
   * Tests a merge where both the source and target are evaluated to be partitioned by
   * SingePartition at planning time but DynamicFileFilterExec will return an empty target.
   */
  @Test
  public void testMergeSinglePartitionPartitioning() {
    // This table will only have a single file and a single partition
    createAndInitTable("id INT", "{\"id\": -1}");

    // Coalesce forces our source into a SinglePartition distribution
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");

    sql(
        "MERGE INTO %s t USING source s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET *"
            + "WHEN NOT MATCHED THEN INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(3), row(4));

    List<Object[]> result = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    assertEquals("Should correctly add the non-matching rows", expectedRows, result);
  }

  @Test
  public void testMergeEmptyTable() {
    Assume.assumeFalse("Custom branch does not exist for empty table", "test".equals(branch));
    // This table will only have a single file and a single partition
    createAndInitTable("id INT", null);

    // Coalesce forces our source into a SinglePartition distribution
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");

    sql(
        "MERGE INTO %s t USING source s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET *"
            + "WHEN NOT MATCHED THEN INSERT *",
        commitTarget());

    ImmutableList<Object[]> expectedRows = ImmutableList.of(row(0), row(1), row(2), row(3), row(4));

    List<Object[]> result = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    assertEquals("Should correctly add the non-matching rows", expectedRows, result);
  }

  @Test
  public void testMergeNonExistingBranch() {
    Assume.assumeTrue("Test only applicable to custom branch", "test".equals(branch));
    createAndInitTable("id INT", null);

    // Coalesce forces our source into a SinglePartition distribution
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "MERGE INTO %s t USING source s ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET *"
                        + "WHEN NOT MATCHED THEN INSERT *",
                    commitTarget()))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot use branch (does not exist): test");
  }

  @Test
  public void testMergeToWapBranch() {
    Assume.assumeTrue("WAP branch only works for table identifier without branch", branch == null);

    createAndInitTable("id INT", "{\"id\": -1}");
    ImmutableList<Object[]> originalRows = ImmutableList.of(row(-1));
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(3), row(4));

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          sql(
              "MERGE INTO %s t USING source s ON t.id = s.id "
                  + "WHEN MATCHED THEN UPDATE SET *"
                  + "WHEN NOT MATCHED THEN INSERT *",
              tableName);
          assertEquals(
              "Should have expected rows when reading table",
              expectedRows,
              sql("SELECT * FROM %s ORDER BY id", tableName));
          assertEquals(
              "Should have expected rows when reading WAP branch",
              expectedRows,
              sql("SELECT * FROM %s.branch_wap ORDER BY id", tableName));
          assertEquals(
              "Should not modify main branch",
              originalRows,
              sql("SELECT * FROM %s.branch_main ORDER BY id", tableName));
        });

    spark.range(3, 6).coalesce(1).createOrReplaceTempView("source2");
    ImmutableList<Object[]> expectedRows2 =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(5));
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          sql(
              "MERGE INTO %s t USING source2 s ON t.id = s.id "
                  + "WHEN MATCHED THEN DELETE "
                  + "WHEN NOT MATCHED THEN INSERT *",
              tableName);
          assertEquals(
              "Should have expected rows when reading table with multiple writes",
              expectedRows2,
              sql("SELECT * FROM %s ORDER BY id", tableName));
          assertEquals(
              "Should have expected rows when reading WAP branch with multiple writes",
              expectedRows2,
              sql("SELECT * FROM %s.branch_wap ORDER BY id", tableName));
          assertEquals(
              "Should not modify main branch with multiple writes",
              originalRows,
              sql("SELECT * FROM %s.branch_main ORDER BY id", tableName));
        });
  }

  @Test
  public void testMergeToWapBranchWithTableBranchIdentifier() {
    Assume.assumeTrue("Test must have branch name part in table identifier", branch != null);

    createAndInitTable("id INT", "{\"id\": -1}");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(3), row(4));

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () ->
            Assertions.assertThatThrownBy(
                    () ->
                        sql(
                            "MERGE INTO %s t USING source s ON t.id = s.id "
                                + "WHEN MATCHED THEN UPDATE SET *"
                                + "WHEN NOT MATCHED THEN INSERT *",
                            commitTarget()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                    String.format(
                        "Cannot write to both branch and WAP branch, but got branch [%s] and WAP branch [wap]",
                        branch)));
  }

  private void checkJoinAndFilterConditions(String query, String join, String icebergFilters) {
    // disable runtime filtering for easier validation
    withSQLConf(
        ImmutableMap.of(
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false",
            SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED().key(), "false"),
        () -> {
          SparkPlan sparkPlan = executeAndKeepPlan(() -> sql(query));
          String planAsString = sparkPlan.toString().replaceAll("#(\\d+L?)", "");

          Assertions.assertThat(planAsString).as("Join should match").contains(join + "\n");

          Assertions.assertThat(planAsString)
              .as("Pushed filters must match")
              .contains("[filters=" + icebergFilters + ",");
        });
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }
}
