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

import static java.lang.String.format;
import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.TableProperties.MERGE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map;
import static org.apache.spark.sql.functions.struct;

import java.util.Collections;
import java.util.HashMap;
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
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMergeAPI extends SparkRowLevelOperationsTestBase {

  public TestMergeAPI(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
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
    sql("DROP TABLE IF EXISTS source");
  }

  @Test
  public void testCoalesceMerge() {
    createAndInitTable("id INT, salary INT, dep STRING");

    String[] records = new String[100];
    for (int index = 0; index < 100; index++) {
      records[index] = format("{ \"id\": %d, \"salary\": 100, \"dep\": \"hr\" }", index);
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

    Dataset<Row> source = spark.range(0, 100).toDF();

    // enable AQE and set the advisory partition big enough to trigger combining
    // set the number of shuffle partitions to 200 to distribute the work across reducers
    // disable broadcast joins to make sure the join triggers a shuffle
    withSQLConf(
        ImmutableMap.of(
            SQLConf.SHUFFLE_PARTITIONS().key(), "200",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(), "-1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "true",
            SQLConf.COALESCE_PARTITIONS_ENABLED().key(), "true",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES().key(), "256MB"),
        () ->
            IcebergMergeInto.table(commitTarget())
                .using(source.as("source"))
                .on(format("%s.id = source.id", commitTarget()))
                .whenMatched()
                .update(
                    new HashMap<String, Column>() {
                      {
                        put("salary", lit(-1));
                      }
                    })
                .merge());

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

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"finance\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    // remove the data file from the 'hr' partition to ensure it is not scanned
    withUnavailableFiles(
        snapshot.addedDataFiles(table.io()),
        () -> {
          // disable dynamic pruning and rely only on static predicate pushdown
          withSQLConf(
              ImmutableMap.of(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false"),
              () ->
                  IcebergMergeInto.table(commitTarget())
                      .using(source.as("source"))
                      .on(
                          format(
                              "%s.id = source.id AND %s.dep IN ('software') AND source.id < 10",
                              commitTarget(), commitTarget()))
                      .whenMatched(format("%s.id = 1", commitTarget()))
                      .updateExpr(
                          new HashMap<String, String>() {
                            {
                              put("dep", "source.dep");
                            }
                          })
                      .whenNotMatched()
                      .insertExpr(
                          new HashMap<String, String>() {
                            {
                              put("dep", "source.dep");
                              put("id", "source.id");
                            }
                          })
                      .merge());
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

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    IcebergMergeInto.table(tableName)
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id", tableName))
        .whenNotMatched()
        .insertAll()
        .merge();

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

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    IcebergMergeInto.table(tableName)
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id", tableName))
        .whenNotMatched("source.id >=2")
        .insertAll()
        .merge();

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

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched(format("%s.id = 1", commitTarget()))
        .updateAll()
        .merge();

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

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id AND %s.id < 3", commitTarget(), commitTarget()))
        .whenMatched(format("%s.id = 1", commitTarget()))
        .updateAll()
        .merge();

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

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched(format("%s.id = 6", commitTarget()))
        .delete()
        .merge();

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
  public void testMergeWithAllCauses() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched(format("%s.id = 1", commitTarget()))
        .updateAll()
        .whenMatched(format("%s.id = 6", commitTarget()))
        .delete()
        .whenNotMatched("source.id = 2")
        .insertAll()
        .merge();

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

    Dataset<Row> sourceDataset =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(sourceDataset.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched(format("%s.id = 1", commitTarget()))
        .updateExpr(
            new HashMap<String, String>() {
              {
                put(format("%s.id", commitTarget()), "source.id");
                put(format("%s.dep", commitTarget()), "source.dep");
              }
            })
        .whenMatched(format("%s.id = 6", commitTarget()))
        .delete()
        .whenNotMatched("source.id = 2")
        .insertExpr(
            new HashMap<String, String>() {
              {
                put(format("%s.id", commitTarget()), "source.id");
                put(format("%s.dep", commitTarget()), "source.dep");
              }
            })
        .merge();

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
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSource() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    List<Integer> sourceIds = Lists.newArrayList();
    for (int i = 0; i < 10_000; i++) {
      sourceIds.add(i);
    }
    Dataset<Row> ds = spark.createDataset(sourceIds, Encoders.INT()).toDF();
    Dataset<Row> source = ds.union(ds);

    String errorMsg = "a single row from the target table with multiple rows of the source table";
    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.value", commitTarget()))
                    .whenMatched(format("%s.id = 1", commitTarget()))
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("id", lit(10));
                          }
                        })
                    .whenMatched(format("%s.id = 6", commitTarget()))
                    .delete()
                    .whenNotMatched("source.value = 2")
                    .insert(
                        new HashMap<String, Column>() {
                          {
                            put("id", col("source.value"));
                            put("dep", lit(null));
                          }
                        })
                    .merge(),
            "Should complain about multiple matches")
        .isInstanceOf(SparkException.class)
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

    Dataset<Row> ds = spark.createDataset(sourceIds, Encoders.INT()).toDF();
    Dataset<Row> source = ds.union(ds);

    withSQLConf(
        ImmutableMap.of(SQLConf.PREFER_SORTMERGEJOIN().key(), "false"),
        () -> {
          String errorMsg =
              "a single row from the target table with multiple rows of the source table";
          Assertions.assertThatThrownBy(
                  () ->
                      IcebergMergeInto.table(commitTarget())
                          .using(source.as("source"))
                          .on(format("%s.id = source.value", commitTarget()))
                          .whenMatched(format("%s.id = 1", commitTarget()))
                          .update(
                              new HashMap<String, Column>() {
                                {
                                  put("id", lit(10));
                                }
                              })
                          .whenMatched(format("%s.id = 6", commitTarget()))
                          .delete()
                          .whenNotMatched("source.value = 2")
                          .insert(
                              new HashMap<String, Column>() {
                                {
                                  put("id", col("source.value"));
                                  put("dep", lit(null));
                                }
                              })
                          .merge(),
                  "Should complain about multiple matches")
              .isInstanceOf(SparkException.class)
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
    Dataset<Row> ds = spark.createDataset(sourceIds, Encoders.INT()).toDF();
    Dataset<Row> source = ds.union(ds);

    withSQLConf(
        ImmutableMap.of(SQLConf.PREFER_SORTMERGEJOIN().key(), "false"),
        () -> {
          String errorMsg =
              "a single row from the target table with multiple rows of the source table";
          Assertions.assertThatThrownBy(
                  () ->
                      IcebergMergeInto.table(commitTarget())
                          .using(source.as("source"))
                          .on(format("%s.id = source.value", commitTarget()))
                          .whenMatched(format("%s.id = 1", commitTarget()))
                          .update(
                              new HashMap<String, Column>() {
                                {
                                  put("id", lit(10));
                                }
                              })
                          .whenMatched(format("%s.id = 6", commitTarget()))
                          .delete()
                          .whenNotMatched("source.value = 2")
                          .insert(
                              new HashMap<String, Column>() {
                                {
                                  put("id", col("source.value"));
                                  put("dep", lit(null));
                                }
                              })
                          .merge(),
                  "Should complain about multiple matches")
              .isInstanceOf(SparkException.class)
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
    Dataset<Row> ds = spark.createDataset(sourceIds, Encoders.INT()).toDF();
    Dataset<Row> source = ds.union(ds);

    String errorMsg = "a single row from the target table with multiple rows of the source table";
    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.value", commitTarget()))
                    .whenMatched(format("%s.id = 1", commitTarget()))
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("id", lit(10));
                          }
                        })
                    .whenMatched(format("%s.id = 6", commitTarget()))
                    .delete()
                    .merge(),
            "Should complain about multiple matches")
        .isInstanceOf(SparkException.class)
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
    Dataset<Row> ds = spark.createDataset(sourceIds, Encoders.INT()).toDF();
    Dataset<Row> source = ds.union(ds);

    String errorMsg = "a single row from the target table with multiple rows of the source table";
    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id > source.value", commitTarget()))
                    .whenMatched(format("%s.id = 1", commitTarget()))
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("id", lit(10));
                          }
                        })
                    .whenMatched(format("%s.id = 6", commitTarget()))
                    .delete()
                    .merge(),
            "Should complain about multiple matches")
        .isInstanceOf(SparkException.class)
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

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String errorMsg = "a single row from the target table with multiple rows of the source table";

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.id", commitTarget()))
                    .whenMatched(format("%s.id = 1", commitTarget()))
                    .updateAll()
                    .whenMatched(format("%s.id = 6", commitTarget()))
                    .delete()
                    .whenNotMatched("source.id = 2")
                    .insertAll()
                    .merge(),
            "Should complain about multiple matches")
        .isInstanceOf(SparkException.class)
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

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .delete()
        .whenNotMatched("source.id = 2")
        .insertAll()
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String errorMsg = "a single row from the target table with multiple rows of the source table";

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.id", commitTarget()))
                    .whenMatched(format("%s.id = 1", commitTarget()))
                    .delete()
                    .whenNotMatched("source.id = 2")
                    .insertAll()
                    .merge(),
            "Should complain about multiple matches")
        .isInstanceOf(SparkException.class)
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

      Dataset<Row> source =
          toDS(
              "id INT, dep STRING",
              "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                  + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                  + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      IcebergMergeInto.table(commitTarget())
          .using(source.as("source"))
          .on(format("%s.id = source.id", commitTarget()))
          .whenMatched(format("%s.id = 1", commitTarget()))
          .updateAll()
          .whenMatched(format("%s.id = 6", commitTarget()))
          .delete()
          .whenNotMatched("source.id = 2")
          .insertAll()
          .merge();

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

      Dataset<Row> source =
          toDS(
              "id INT, ts TIMESTAMP",
              "{ \"id\": 2, \"ts\": \"2001-01-02 00:00:00\" }\n"
                  + "{ \"id\": 1, \"ts\": \"2001-01-01 00:00:00\" }\n"
                  + "{ \"id\": 6, \"ts\": \"2001-01-06 00:00:00\" }");

      IcebergMergeInto.table(commitTarget())
          .using(source.as("source"))
          .on(format("%s.id = source.id", commitTarget()))
          .whenMatched(format("%s.id = 1", commitTarget()))
          .updateAll()
          .whenMatched(format("%s.id = 6", commitTarget()))
          .delete()
          .whenNotMatched("source.id = 2")
          .insertAll()
          .merge();

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

      Dataset<Row> source =
          toDS(
              "id INT, dep STRING",
              "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                  + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                  + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      IcebergMergeInto.table(commitTarget())
          .using(source.as("source"))
          .on(format("%s.id = source.id", commitTarget()))
          .whenMatched(format("%s.id = 1", commitTarget()))
          .updateAll()
          .whenMatched(format("%s.id = 6", commitTarget()))
          .delete()
          .whenNotMatched("source.id = 2")
          .insertAll()
          .merge();

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

      Dataset<Row> source =
          toDS(
              "id INT, dep STRING",
              "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                  + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                  + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      IcebergMergeInto.table(commitTarget())
          .using(source.as("source"))
          .on(format("%s.id = source.id", commitTarget()))
          .whenMatched(format("%s.id = 1", commitTarget()))
          .updateAll()
          .whenMatched(format("%s.id = 6", commitTarget()))
          .delete()
          .whenNotMatched("source.id = 2")
          .insertAll()
          .merge();

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

      Dataset<Row> source =
          toDS(
              "id INT, dep STRING",
              "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                  + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                  + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      IcebergMergeInto.table(commitTarget())
          .using(source.as("source"))
          .on(format("%s.id = source.id", commitTarget()))
          .whenMatched(format("%s.id = 1", commitTarget()))
          .updateAll()
          .whenMatched(format("%s.id = 6", commitTarget()))
          .delete()
          .whenNotMatched("source.id = 2")
          .insertAll()
          .merge();

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
  public synchronized void testMergeWithSerializableIsolation() throws InterruptedException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitTable("id INT, dep STRING");
    Dataset<Row> source = spark.createDataset(Collections.singletonList(1), Encoders.INT()).toDF();

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
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.value", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("dep", lit("x"));
                          }
                        })
                    .merge();

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

    createAndInitTable("id INT, dep STRING");
    Dataset<Row> source = spark.createDataset(Collections.singletonList(1), Encoders.INT()).toDF();

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
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }

                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.value", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("dep", lit("x"));
                          }
                        })
                    .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, extra_col STRING, v STRING",
            "{ \"id\": 1, \"extra_col\": -1, \"v\": \"v1_1\" }\n"
                + "{ \"id\": 3, \"extra_col\": -1, \"v\": \"v3\" }\n"
                + "{ \"id\": 4, \"extra_col\": -1, \"v\": \"v4\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
                put("id", "source.id");
              }
            })
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, v STRING",
            "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 4, \"v\": \"v4\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
                put("id", "source.id");
              }
            })
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, v STRING",
            "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 4, \"v\": \"v4\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id <=> source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
                put("id", "source.id");
              }
            })
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, v STRING",
            "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 2, \"v\": \"v2_2\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id AND NULL", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
                put("id", "source.id");
              }
            })
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, v STRING",
            "{ \"id\": 1, \"v\": \"v1_1\" }\n"
                + "{ \"id\": 2, \"v\": \"v2_2\" }\n"
                + "{ \"id\": 3, \"v\": \"v3_3\" }");

    // all conditions are NULL and will never match any rows
    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched("source.id = 1 and NULL")
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenMatched("source.v = 'v1_1' and NULL")
        .delete()
        .whenNotMatched("source.id = 3 and NULL")
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
                put("id", "source.id");
              }
            })
        .merge();

    ImmutableList<Object[]> expectedRows1 =
        ImmutableList.of(
            row(1, "v1"), // kept
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows1, sql("SELECT * FROM %s ORDER BY v", selectTarget()));

    // only the update and insert conditions are NULL
    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched("source.id = 1 and NULL")
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenMatched("source.v = 'v1_1'")
        .delete()
        .whenNotMatched("source.id = 3 and NULL")
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
                put("id", "source.id");
              }
            })
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, v STRING",
            "{ \"id\": 1, \"v\": \"v1_1\" }\n" + "{ \"id\": 2, \"v\": \"v2_2\" }");

    // the order of match actions is important in this case
    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched("source.id = 1")
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("v", "source.v");
              }
            })
        .whenMatched("source.v = 'v1_1'")
        .delete()
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("id", "source.id");
                put("v", "source.v");
              }
            })
        .merge();

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "v1_1"), // updated (also matches the delete cond but update is first)
            row(2, "v2") // kept (matches neither the update nor the delete cond)
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", selectTarget()));
  }

  @Test
  public void testMergeWithMultipleRowGroupsParquet()
      throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException {
    Assume.assumeTrue(fileFormat.equalsIgnoreCase("parquet"));

    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')",
        tableName, PARQUET_ROW_GROUP_SIZE_BYTES, 100);
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')", tableName, SPLIT_SIZE, 100);

    Dataset<Row> source = spark.createDataset(Collections.singletonList(1), Encoders.INT()).toDF();

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
    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.value", commitTarget()))
        .whenMatched()
        .update(
            new HashMap<String, Column>() {
              {
                put("dep", lit("x"));
              }
            })
        .merge();

    Assert.assertEquals(200, spark.table(commitTarget()).count());
  }

  @Test
  public void testMergeInsertOnly() {
    createAndInitTable(
        "id STRING, v STRING",
        "{ \"id\": \"a\", \"v\": \"v1\" }\n" + "{ \"id\": \"b\", \"v\": \"v2\" }");
    Dataset<Row> source =
        toDS(
            "id STRING, v STRING",
            "{ \"id\": \"a\", \"v\": \"v1_1\" }\n"
                + "{ \"id\": \"a\", \"v\": \"v1_2\" }\n"
                + "{ \"id\": \"c\", \"v\": \"v3\" }\n"
                + "{ \"id\": \"d\", \"v\": \"v4_1\" }\n"
                + "{ \"id\": \"d\", \"v\": \"v4_2\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenNotMatched()
        .insertAll()
        .merge();

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
    Dataset<Row> source =
        toDS(
            "id INTEGER, v INTEGER,is_new BOOLEAN",
            "{ \"id\": 1, \"v\": 11, \"is_new\": true }\n"
                + "{ \"id\": 2, \"v\": 21, \"is_new\": true }\n"
                + "{ \"id\": 2, \"v\": 22, \"is_new\": false }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenNotMatched("source.is_new = TRUE")
        .insert(
            new HashMap<String, Column>() {
              {
                put("v", col("source.v").plus(lit(100)));
                put("id", col("source.id"));
              }
            })
        .merge();

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
    Dataset<Row> source =
        toDS(
            "id INT, c1 INT, c2 STRING",
            "{ \"id\": 1, \"c1\": -2, \"c2\": \"new_str_1\" }\n"
                + "{ \"id\": 2, \"c1\": -20, \"c2\": \"new_str_2\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("b", "source.c2");
                put("a", "source.c1");
                put("id", "source.id");
              }
            })
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("b", "source.c2");
                put("a", "source.c1");
                put("id", "source.id");
              }
            })
        .merge();

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, -2, "new_str_1"), row(2, -20, "new_str_2")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeMixedCaseAlignsUpdateAndInsertActions() {
    createAndInitTable("id INT, a INT, b STRING", "{ \"id\": 1, \"a\": 2, \"b\": \"str\" }");
    Dataset<Row> source =
        toDS(
            "id INT, c1 INT, c2 STRING",
            "{ \"id\": 1, \"c1\": -2, \"c2\": \"new_str_1\" }\n"
                + "{ \"id\": 2, \"c1\": -20, \"c2\": \"new_str_2\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.iD = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("B", "source.c2");
                put("A", "source.c1");
                put("Id", "source.ID");
              }
            })
        .whenNotMatched()
        .insertExpr(
            new HashMap<String, String>() {
              {
                put("b", "source.c2");
                put("A", "source.c1");
                put("iD", "source.id");
              }
            })
        .merge();

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
    Dataset<Row> source = toDS("id INT, c1 INT", "{ \"id\": 1, \"c1\": -2 }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .update(
            new HashMap<String, Column>() {
              {
                put("s.c1", col("source.c1"));
                put("s.c2.a", array(lit(-1), lit(-2)));
                put("s.c2.m", map(lit('k'), lit('v')));
              }
            })
        .merge();

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-2, row(ImmutableList.of(-1, -2), ImmutableMap.of("k", "v"))))),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .update(
            new HashMap<String, Column>() {
              {
                put("s.c1", lit(null));
                put("s.c2", lit(null));
              }
            })
        .merge();

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(null, null))),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    // update all fields in a struct
    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .update(
            new HashMap<String, Column>() {
              {
                put(
                    "s",
                    struct(
                        lit(100).as("c1"),
                        struct(array(lit(1)).as("a"), map(lit("x"), lit("y")).as("m")).as("c2")));
              }
            })
        .merge();

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(100, row(ImmutableList.of(1), ImmutableMap.of("x", "y"))))),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeWithInferredCasts() {
    createAndInitTable("id INT, s STRING", "{ \"id\": 1, \"s\": \"value\" }");
    Dataset<Row> source = toDS("id INT, c1 INT", "{ \"id\": 1, \"c1\": -2}");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("s", "source.c1");
              }
            })
        .merge();

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, "-2")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @Test
  public void testMergeModifiesNullStruct() {
    createAndInitTable("id INT, s STRUCT<n1:INT,n2:INT>", "{ \"id\": 1, \"s\": null }");
    Dataset<Row> source = toDS("id INT, n1 INT", "{ \"id\": 1, \"n1\": -10 }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("s.n1", "source.n1");
              }
            })
        .merge();

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-10, null))),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @Test
  public void testMergeRefreshesRelationCache() {
    createAndInitTable("id INT, name STRING", "{ \"id\": 1, \"name\": \"n1\" }");
    Dataset<Row> source = toDS("id INT, name STRING", "{ \"id\": 1, \"name\": \"n2\" }");

    Dataset<Row> query = spark.sql("SELECT name FROM " + commitTarget());
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have correct data", ImmutableList.of(row("n1")), sql("SELECT * FROM tmp"));

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateExpr(
            new HashMap<String, String>() {
              {
                put("name", "source.name");
              }
            })
        .merge();

    assertEquals(
        "View should have correct data", ImmutableList.of(row("n2")), sql("SELECT * FROM tmp"));

    spark.sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testMergeWithMultipleNotMatchedActions() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 0, \"dep\": \"emp-id-0\" }");

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenNotMatched("source.id = 1")
        .insert(
            new HashMap<String, Column>() {
              {
                put("dep", col("source.dep"));
                put("id", lit(-1));
              }
            })
        .whenNotMatched()
        .insertAll()
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenNotMatched("source.id = 1")
        .insert(
            new HashMap<String, Column>() {
              {
                put("dep", col("source.dep"));
                put("id", lit(-1));
              }
            })
        .whenNotMatched("source.id = 2")
        .insertAll()
        .merge();

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

    Dataset<Row> source =
        toDS(
            "badge INT, id INT, dep STRING",
            "{ \"badge\": 1001, \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"badge\": 6006, \"id\": 6, \"dep\": \"emp-id-6\" }\n"
                + "{ \"badge\": 7007, \"id\": 7, \"dep\": \"emp-id-7\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT, dep STRING",
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    IcebergMergeInto.table(tableName)
        .using(source.as("source"))
        .on("1 != 1")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge();

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

    Dataset<Row> source =
        toDS(
            "id INT NOT NULL, dep STRING",
            "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
                + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
                + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched(format("%s.id = 1", commitTarget()))
        .updateAll()
        .whenMatched(format("%s.id = 6", commitTarget()))
        .delete()
        .whenNotMatched("source.id = 2")
        .insertAll()
        .merge();

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
    Dataset<Row> source = toDS("c1 INT, c2 INT", "{ \"c1\": -100, \"c2\": -200 }");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("invalid_col", col("source.c2"));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("cannot resolve invalid_col");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("c.n2.invalid_col", col("source.c2"));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("No such struct field `invalid_col`");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("c.n2.dn1", col("source.c2"));
                          }
                        })
                    .whenNotMatched()
                    .insert(
                        new HashMap<String, Column>() {
                          {
                            put("id", col("source.c1"));
                            put("invalid_col", lit(null));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("cannot resolve invalid_col");
  }

  @Test
  public void testMergeWithInvalidColumnsInInsert() {
    createAndInitTable(
        "id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>",
        "{ \"id\": 1, \"c\": { \"n1\": 2, \"n2\": { \"dn1\": 3, \"dn2\": 4 } } }");
    Dataset<Row> source = toDS("c1 INT, c2 INT", "{ \"c1\": -100, \"c2\": -200 }");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("c.n2.dn1", col("source.c2"));
                          }
                        })
                    .whenNotMatched()
                    .insert(
                        new HashMap<String, Column>() {
                          {
                            put("id", col("source.c1"));
                            put("c.n2", lit(null));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Nested fields are not supported inside INSERT clauses");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenNotMatched()
                    .insert(
                        new HashMap<String, Column>() {
                          {
                            put("id", col("source.c1"));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("must provide values for all columns of the target table");
  }

  @Test
  public void testMergeWithInvalidUpdates() {
    createAndInitTable(
        "id INT, a ARRAY<STRUCT<c1:INT,c2:INT>>, m MAP<STRING,STRING>",
        "{ \"id\": 1, \"a\": [ { \"c1\": 2, \"c2\": 3 } ], \"m\": { \"k\": \"v\"} }");
    Dataset<Row> source = toDS("c1 INT, c2 INT", "{ \"c1\": -100, \"c2\": -200 }");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("a.c1", col("source.c2"));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Updating nested fields is only supported for structs");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.c1", commitTarget()))
                    .whenMatched()
                    .update(
                        new HashMap<String, Column>() {
                          {
                            put("m.key", lit("new_key"));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Updating nested fields is only supported for structs");
  }

  @Test
  public void testMergeWithTargetColumnsInInsertConditions() {
    createAndInitTable("id INT, c2 INT", "{ \"id\": 1, \"c2\": 2 }");
    Dataset<Row> source = toDS("id INT, value INT", "{ \"id\": 1, \"value\": 11 }");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.id", commitTarget()))
                    .whenNotMatched("c2 = 1")
                    .insert(
                        new HashMap<String, Column>() {
                          {
                            put("id", col("source.id"));
                            put("c2", lit(null));
                          }
                        })
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot resolve [c2]");
  }

  @Test
  public void testMergeWithNonIcebergTargetTableNotSupported() {
    createOrReplaceView("target", "{ \"c1\": -100, \"c2\": -200 }");
    Dataset<Row> source = toDS("c1 INT, c2 INT", "{ \"c1\": -100, \"c2\": -200 }");

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table("target")
                    .using(source.as("source"))
                    .on("target.c1 = source.c1")
                    .whenMatched()
                    .updateAll()
                    .merge())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("is not an Iceberg table");
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
    Dataset<Row> source = spark.range(0, 5).coalesce(1).toDF();

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge();

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
    Dataset<Row> source = spark.range(0, 5).coalesce(1).toDF();

    IcebergMergeInto.table(commitTarget())
        .using(source.as("source"))
        .on(format("%s.id = source.id", commitTarget()))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge();

    ImmutableList<Object[]> expectedRows = ImmutableList.of(row(0), row(1), row(2), row(3), row(4));

    List<Object[]> result = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    assertEquals("Should correctly add the non-matching rows", expectedRows, result);
  }

  @Test
  public void testMergeNonExistingBranch() {
    Assume.assumeTrue("Test only applicable to custom branch", "test".equals(branch));
    createAndInitTable("id INT", null);

    // Coalesce forces our source into a SinglePartition distribution
    Dataset<Row> source = spark.range(0, 5).coalesce(1).toDF();

    Assertions.assertThatThrownBy(
            () ->
                IcebergMergeInto.table(commitTarget())
                    .using(source.as("source"))
                    .on(format("%s.id = source.id", commitTarget()))
                    .whenMatched()
                    .updateAll()
                    .whenNotMatched()
                    .insertAll()
                    .merge())
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

    Dataset<Row> source = spark.range(0, 5).coalesce(1).toDF();

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(3), row(4));

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          IcebergMergeInto.table(commitTarget())
              .using(source.as("source"))
              .on(format("%s.id = source.id", commitTarget()))
              .whenMatched()
              .updateAll()
              .whenNotMatched()
              .insertAll()
              .merge();
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

    Dataset<Row> source2 = spark.range(3, 6).coalesce(1).toDF();

    ImmutableList<Object[]> expectedRows2 =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(5));
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () -> {
          IcebergMergeInto.table(commitTarget())
              .using(source2.as("source"))
              .on(format("%s.id = source.id", commitTarget()))
              .whenMatched()
              .delete()
              .whenNotMatched()
              .insertAll()
              .merge();
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

    Dataset<Row> source = spark.range(0, 5).coalesce(1).toDF();

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.WAP_BRANCH, "wap"),
        () ->
            Assertions.assertThatThrownBy(
                    () ->
                        IcebergMergeInto.table(commitTarget())
                            .using(source.as("source"))
                            .on(format("%s.id = source.id", commitTarget()))
                            .whenMatched()
                            .updateAll()
                            .whenNotMatched()
                            .insertAll()
                            .merge())
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                    format(
                        "Cannot write to both branch and WAP branch, but got branch [%s] and WAP branch [wap]",
                        branch)));
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }
}
