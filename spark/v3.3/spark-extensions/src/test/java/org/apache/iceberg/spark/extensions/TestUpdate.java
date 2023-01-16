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
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE_DEFAULT;
import static org.apache.spark.sql.functions.lit;

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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
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
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
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
  public void testExplain() {
    createAndInitTable("id INT, dep STRING");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    sql("EXPLAIN UPDATE %s SET dep = 'invalid' WHERE id <=> 1", tableName);

    sql("EXPLAIN UPDATE %s SET dep = 'invalid' WHERE true", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testUpdateEmptyTable() {
    createAndInitTable("id INT, dep STRING");

    sql("UPDATE %s SET dep = 'invalid' WHERE id IN (1)", tableName);
    sql("UPDATE %s SET id = -1 WHERE dep = 'hr'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testUpdateWithAlias() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"a\" }");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("UPDATE %s AS t SET t.dep = 'invalid'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "invalid")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testUpdateAlignsAssignments() {
    createAndInitTable("id INT, c1 INT, c2 INT");

    sql("INSERT INTO TABLE %s VALUES (1, 11, 111), (2, 22, 222)", tableName);

    sql("UPDATE %s SET `c2` = c2 - 2, c1 = `c1` - 1 WHERE id <=> 1", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, 10, 109), row(2, 22, 222)),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testUpdateWithUnsupportedPartitionPredicate() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'software'), (2, 'hr')", tableName);

    sql("UPDATE %s t SET `t`.`id` = -1 WHERE t.dep LIKE '%%r' ", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(1, "software")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testUpdateWithDynamicFileFiltering() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");
    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    sql("UPDATE %s SET id = cast('-1' AS INT) WHERE id = 2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "1", "1", "1");
    } else {
      validateMergeOnRead(currentSnapshot, "1", "1", "1");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }

  @Test
  public void testUpdateNonExistingRecords() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    sql("UPDATE %s SET id = -1 WHERE id > 10", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "0", null, null);
    } else {
      validateMergeOnRead(currentSnapshot, "0", null, null);
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testUpdateWithoutCondition() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", tableName);
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", tableName);

    // set the num of shuffle partitions to 200 instead of default 4 to reduce the chance of hashing
    // records for multiple source files to one writing task (needed for a predictable num of output
    // files)
    withSQLConf(
        ImmutableMap.of(SQLConf.SHUFFLE_PARTITIONS().key(), "200"),
        () -> {
          sql("UPDATE %s SET id = -1", tableName);
        });

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();

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
        sql("SELECT * FROM %s ORDER BY dep ASC", tableName));
  }

  @Test
  public void testUpdateWithNullConditions() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 0, \"dep\": null }\n"
            + "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }");

    // should not update any rows as null is never equal to null
    sql("UPDATE %s SET id = -1 WHERE dep = NULL", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // should not update any rows the condition does not match any records
    sql("UPDATE %s SET id = -1 WHERE dep = 'software'", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // should update one matching row with a null-safe condition
    sql("UPDATE %s SET dep = 'invalid', id = -1 WHERE dep <=> NULL", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "invalid"), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testUpdateWithInAndNotInConditions() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");

    sql("UPDATE %s SET id = -1 WHERE id IN (1, null)", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql("UPDATE %s SET id = 100 WHERE id NOT IN (null, 1)", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql("UPDATE %s SET id = 100 WHERE id NOT IN (1, 10)", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(100, "hardware"), row(100, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", tableName));
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

    Assert.assertEquals(200, spark.table(tableName).count());

    // update a record from one of two row groups and copy over the second one
    sql("UPDATE %s SET id = -1 WHERE id IN (200, 201)", tableName);

    Assert.assertEquals(200, spark.table(tableName).count());
  }

  @Test
  public void testUpdateNestedStructFields() {
    createAndInitTable(
        "id INT, s STRUCT<c1:INT,c2:STRUCT<a:ARRAY<INT>,m:MAP<STRING, STRING>>>",
        "{ \"id\": 1, \"s\": { \"c1\": 2, \"c2\": { \"a\": [1,2], \"m\": { \"a\": \"b\"} } } } }");

    // update primitive, array, map columns inside a struct
    sql("UPDATE %s SET s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1)", tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-1, row(ImmutableList.of(-1), ImmutableMap.of("k", "v"))))),
        sql("SELECT * FROM %s", tableName));

    // set primitive, array, map columns to NULL (proper casts should be in place)
    sql("UPDATE %s SET s.c1 = NULL, s.c2 = NULL WHERE id IN (1)", tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(null, null))),
        sql("SELECT * FROM %s", tableName));

    // update all fields in a struct
    sql(
        "UPDATE %s SET s = named_struct('c1', 1, 'c2', named_struct('a', array(1), 'm', null))",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(1, row(ImmutableList.of(1), null)))),
        sql("SELECT * FROM %s", tableName));
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

    // request a global sort
    sql("ALTER TABLE %s WRITE ORDERED BY c2", tableName);
    sql("UPDATE %s SET c2 = -22 WHERE id NOT IN (1, 3)", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, 11, 1), row(2, -22, 1), row(3, 33, 1)),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    // request a local sort
    sql("ALTER TABLE %s WRITE LOCALLY ORDERED BY id", tableName);
    sql("UPDATE %s SET c2 = -33 WHERE id = 3", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, 11, 1), row(2, -22, 1), row(3, -33, 1)),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    // request a hash distribution + local sort
    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY id", tableName);
    sql("UPDATE %s SET c2 = -11 WHERE id = 1", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, -11, 1), row(2, -22, 1), row(3, -33, 1)),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public synchronized void testUpdateWithSerializableIsolation() throws InterruptedException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitTable("id INT, dep STRING");

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, UPDATE_ISOLATION_LEVEL, "serializable");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);

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

                sql("UPDATE %s SET id = -1 WHERE id = 1", tableName);

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
                  table.newFastAppend().appendFile(dataFile).commit();
                  sleep(10);
                }

                barrier.incrementAndGet();
              }
            });

    try {
      Assertions.assertThatThrownBy(updateFuture::get)
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
  public synchronized void testUpdateWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitTable("id INT, dep STRING");

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, UPDATE_ISOLATION_LEVEL, "snapshot");

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);

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
                  table.newFastAppend().appendFile(dataFile).commit();
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

    sql("UPDATE %s SET s = -1 WHERE id = 1", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "-1")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testUpdateModifiesNullStruct() {
    createAndInitTable("id INT, s STRUCT<n1:INT,n2:INT>", "{ \"id\": 1, \"s\": null }");

    sql("UPDATE %s SET s.n1 = -1 WHERE id = 1", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, row(-1, null))),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testUpdateRefreshesRelationCache() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have correct data",
        ImmutableList.of(row(1, "hardware"), row(1, "hr")),
        sql("SELECT * FROM tmp ORDER BY id, dep"));

    sql("UPDATE %s SET id = -1 WHERE id = 1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "2", "2", "2");
    } else {
      validateMergeOnRead(currentSnapshot, "2", "2", "2");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(2, "hardware"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));

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

    createOrReplaceView("updated_id", Arrays.asList(0, 1, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    sql(
        "UPDATE %s SET id = -1 WHERE "
            + "id IN (SELECT * FROM updated_id) AND "
            + "dep IN (SELECT * from updated_dep)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql(
        "UPDATE %s SET id = 5 WHERE id IS NULL OR id IN (SELECT value + 1 FROM updated_id)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(5, "hardware"), row(5, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));

    append(tableName, "{ \"id\": null, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(-1, "hr"), row(2, "hr"), row(5, "hardware"), row(5, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", tableName));

    sql(
        "UPDATE %s SET id = 10 WHERE id IN (SELECT value + 2 FROM updated_id) AND dep = 'hr'",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(-1, "hr"), row(5, "hardware"), row(5, "hr"), row(10, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", tableName));
  }

  @Test
  public void testUpdateWithInSubqueryAndDynamicFileFiltering() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");
    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    createOrReplaceView("updated_id", Arrays.asList(-1, 2), Encoders.INT());

    sql("UPDATE %s SET id = -1 WHERE id IN (SELECT * FROM updated_id)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    if (mode(table) == COPY_ON_WRITE) {
      validateCopyOnWrite(currentSnapshot, "1", "1", "1");
    } else {
      validateMergeOnRead(currentSnapshot, "1", "1", "1");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }

  @Test
  public void testUpdateWithSelfSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");

    sql("UPDATE %s SET dep = 'x' WHERE id IN (SELECT id + 1 FROM %s)", tableName, tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "x")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // TODO: Spark does not support AQE and DPP with aggregates at the moment
    withSQLConf(
        ImmutableMap.of(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false"),
        () -> {
          sql(
              "UPDATE %s SET dep = 'y' WHERE "
                  + "id = (SELECT count(*) FROM (SELECT DISTINCT id FROM %s) AS t)",
              tableName, tableName);
          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(1, "hr"), row(2, "y")),
              sql("SELECT * FROM %s ORDER BY id", tableName));
        });

    sql("UPDATE %s SET id = (SELECT id - 2 FROM %s WHERE id = 1)", tableName, tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(-1, "y")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }

  @Test
  public void testUpdateWithMultiColumnInSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");

    List<Employee> deletedEmployees =
        Arrays.asList(new Employee(null, "hr"), new Employee(1, "hr"));
    createOrReplaceView("deleted_employee", deletedEmployees, Encoders.bean(Employee.class));

    sql(
        "UPDATE %s SET dep = 'x', id = -1 WHERE (id, dep) IN (SELECT id, dep FROM deleted_employee)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "x"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testUpdateWithNotInSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");

    createOrReplaceView("updated_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("software", "hr"), Encoders.STRING());

    // the file filter subquery (nested loop lef-anti join) returns 0 records
    sql("UPDATE %s SET id = -1 WHERE id NOT IN (SELECT * FROM updated_id)", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql(
        "UPDATE %s SET id = -1 WHERE id NOT IN (SELECT * FROM updated_id WHERE value IS NOT NULL)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", tableName));

    sql(
        "UPDATE %s SET id = 5 WHERE id NOT IN (SELECT * FROM updated_id) OR dep IN ('software', 'hr')",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(5, "hr"), row(5, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST, dep", tableName));
  }

  @Test
  public void testUpdateWithExistSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");

    createOrReplaceView("updated_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("hr", null), Encoders.STRING());

    sql(
        "UPDATE %s t SET id = -1 WHERE EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql(
        "UPDATE %s t SET dep = 'x', id = -1 WHERE "
            + "EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value + 2)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "x"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql(
        "UPDATE %s t SET id = -2 WHERE "
            + "EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value) OR "
            + "t.id IS NULL",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-2, "hr"), row(-2, "x"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));

    sql(
        "UPDATE %s t SET id = 1 WHERE "
            + "EXISTS (SELECT 1 FROM updated_id ui WHERE t.id = ui.value) AND "
            + "EXISTS (SELECT 1 FROM updated_dep ud WHERE t.dep = ud.value)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-2, "x"), row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }

  @Test
  public void testUpdateWithNotExistsSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");

    createOrReplaceView("updated_id", Arrays.asList(-1, -2, null), Encoders.INT());
    createOrReplaceView("updated_dep", Arrays.asList("hr", "software"), Encoders.STRING());

    sql(
        "UPDATE %s t SET id = -1 WHERE NOT EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value + 2)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(1, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));

    sql(
        "UPDATE %s t SET id = 5 WHERE "
            + "NOT EXISTS (SELECT 1 FROM updated_id u WHERE t.id = u.value) OR "
            + "t.id = 1",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(5, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));

    sql(
        "UPDATE %s t SET id = 10 WHERE "
            + "NOT EXISTS (SELECT 1 FROM updated_id ui WHERE t.id = ui.value) AND "
            + "EXISTS (SELECT 1 FROM updated_dep ud WHERE t.dep = ud.value)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(-1, "hr"), row(10, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }

  @Test
  public void testUpdateWithScalarSubquery() {
    createAndInitTable("id INT, dep STRING");

    append(
        tableName,
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hardware\" }\n"
            + "{ \"id\": null, \"dep\": \"hr\" }");

    createOrReplaceView("updated_id", Arrays.asList(1, 100, null), Encoders.INT());

    // TODO: Spark does not support AQE and DPP with aggregates at the moment
    withSQLConf(
        ImmutableMap.of(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false"),
        () -> {
          sql("UPDATE %s SET id = -1 WHERE id <= (SELECT min(value) FROM updated_id)", tableName);
          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(-1, "hr"), row(2, "hardware"), row(null, "hr")),
              sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
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

    append(
        tableName,
        "{ \"id\": 0, \"dep\": \"ops\" }\n"
            + "{ \"id\": 1, \"dep\": \"ops\" }\n"
            + "{ \"id\": 2, \"dep\": \"ops\" }");

    append(
        tableName,
        "{ \"id\": 0, \"dep\": \"hr\" }\n"
            + "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }");

    append(
        tableName,
        "{ \"id\": 0, \"dep\": \"ops\" }\n"
            + "{ \"id\": 1, \"dep\": \"ops\" }\n"
            + "{ \"id\": 2, \"dep\": \"ops\" }");

    createOrReplaceView("updated_id", Arrays.asList(1, 100), Encoders.INT());

    String originalNumOfShufflePartitions = spark.conf().get("spark.sql.shuffle.partitions");
    try {
      // set the num of shuffle partitions to 1 to ensure we have only 1 writing task
      spark.conf().set("spark.sql.shuffle.partitions", "1");

      sql("UPDATE %s t SET id = -1 WHERE id IN (SELECT * FROM updated_id)", tableName);
      Assert.assertEquals("Should have expected num of rows", 12L, spark.table(tableName).count());
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

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.VECTORIZATION_ENABLED, "true"),
        () -> {
          sql("UPDATE %s t SET id = -1", tableName);

          assertEquals(
              "Should have expected rows",
              ImmutableList.of(row(-1, "hr"), row(-1, "hr"), row(-1, "hr")),
              sql("SELECT * FROM %s ORDER BY id, dep", tableName));
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

    Dataset<Row> df2 =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("software"))
            .withColumn("country", lit("usa"));
    df2.coalesce(1).writeTo(tableName).append();

    Dataset<Row> df3 =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("hardware"))
            .withColumn("country", lit("usa"));
    df3.coalesce(1).writeTo(tableName).append();

    sql("UPDATE %s SET id = -1 WHERE id IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19)", tableName);
    Assert.assertEquals(30L, scalarSql("SELECT count(*) FROM %s WHERE id = -1", tableName));
  }

  @Test
  public void testUpdateWithStaticPredicatePushdown() {
    createAndInitTable("id INT, dep STRING");

    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    // add a data file to the 'software' partition
    append(tableName, "{ \"id\": 1, \"dep\": \"software\" }");

    // add a data file to the 'hr' partition
    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }");

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snapshot = table.currentSnapshot();
    String dataFilesCount = snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP);
    Assert.assertEquals("Must have 2 files before UPDATE", "2", dataFilesCount);

    // remove the data file from the 'hr' partition to ensure it is not scanned
    DataFile dataFile = Iterables.getOnlyElement(snapshot.addedDataFiles(table.io()));
    table.io().deleteFile(dataFile.path().toString());

    // disable dynamic pruning and rely only on static predicate pushdown
    withSQLConf(
        ImmutableMap.of(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false"),
        () -> {
          sql("UPDATE %s SET id = -1 WHERE dep IN ('software') AND id == 1", tableName);
        });
  }

  @Test
  public void testUpdateWithInvalidUpdates() {
    createAndInitTable("id INT, a ARRAY<STRUCT<c1:INT,c2:INT>>, m MAP<STRING,STRING>");

    AssertHelpers.assertThrows(
        "Should complain about updating an array column",
        AnalysisException.class,
        "Updating nested fields is only supported for structs",
        () -> sql("UPDATE %s SET a.c1 = 1", tableName));

    AssertHelpers.assertThrows(
        "Should complain about updating a map column",
        AnalysisException.class,
        "Updating nested fields is only supported for structs",
        () -> sql("UPDATE %s SET m.key = 'new_key'", tableName));
  }

  @Test
  public void testUpdateWithConflictingAssignments() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");

    AssertHelpers.assertThrows(
        "Should complain about conflicting updates to a top-level column",
        AnalysisException.class,
        "Updates are in conflict",
        () -> sql("UPDATE %s t SET t.id = 1, t.c.n1 = 2, t.id = 2", tableName));

    AssertHelpers.assertThrows(
        "Should complain about conflicting updates to a nested column",
        AnalysisException.class,
        "Updates are in conflict for these columns",
        () -> sql("UPDATE %s t SET t.c.n1 = 1, t.id = 2, t.c.n1 = 2", tableName));

    AssertHelpers.assertThrows(
        "Should complain about conflicting updates to a nested column",
        AnalysisException.class,
        "Updates are in conflict",
        () -> {
          sql(
              "UPDATE %s SET c.n1 = 1, c = named_struct('n1', 1, 'n2', named_struct('dn1', 1, 'dn2', 2))",
              tableName);
        });
  }

  @Test
  public void testUpdateWithInvalidAssignments() {
    createAndInitTable(
        "id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL");

    for (String policy : new String[] {"ansi", "strict"}) {
      withSQLConf(
          ImmutableMap.of("spark.sql.storeAssignmentPolicy", policy),
          () -> {
            AssertHelpers.assertThrows(
                "Should complain about writing nulls to a top-level column",
                AnalysisException.class,
                "Cannot write nullable values to non-null column",
                () -> sql("UPDATE %s t SET t.id = NULL", tableName));

            AssertHelpers.assertThrows(
                "Should complain about writing nulls to a nested column",
                AnalysisException.class,
                "Cannot write nullable values to non-null column",
                () -> sql("UPDATE %s t SET t.s.n1 = NULL", tableName));

            AssertHelpers.assertThrows(
                "Should complain about writing missing fields in structs",
                AnalysisException.class,
                "missing fields",
                () -> sql("UPDATE %s t SET t.s = named_struct('n1', 1)", tableName));

            AssertHelpers.assertThrows(
                "Should complain about writing invalid data types",
                AnalysisException.class,
                "Cannot safely cast",
                () -> sql("UPDATE %s t SET t.s.n1 = 'str'", tableName));

            AssertHelpers.assertThrows(
                "Should complain about writing incompatible structs",
                AnalysisException.class,
                "field name does not match",
                () -> sql("UPDATE %s t SET t.s.n2 = named_struct('dn2', 1, 'dn1', 2)", tableName));
          });
    }
  }

  @Test
  public void testUpdateWithNonDeterministicCondition() {
    createAndInitTable("id INT, dep STRING");

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic expressions",
        AnalysisException.class,
        "nondeterministic expressions are only allowed",
        () -> sql("UPDATE %s SET id = -1 WHERE id = 1 AND rand() > 0.5", tableName));
  }

  @Test
  public void testUpdateOnNonIcebergTableNotSupported() {
    createOrReplaceView("testtable", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "UPDATE is not supported for non iceberg table",
        UnsupportedOperationException.class,
        "not supported temporarily",
        () -> sql("UPDATE %s SET c1 = -1 WHERE c2 = 1", "testtable"));
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }
}
