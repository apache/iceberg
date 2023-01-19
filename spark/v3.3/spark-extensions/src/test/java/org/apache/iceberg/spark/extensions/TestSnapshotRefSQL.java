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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestSnapshotRefSQL extends SparkExtensionsTestBase {

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties()
      }
    };
  }

  public TestSnapshotRefSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testCreateBranch() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    long snapshotId = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 10L;
    sql(
        "ALTER TABLE %s CREATE BRANCH %s AS OF VERSION %d RETAIN %d DAYS WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS",
        tableName, branchName, snapshotId, maxRefAge, minSnapshotsToKeep, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(maxSnapshotAge * 24 * 60 * 60 * 1000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref.maxRefAgeMs().longValue());

    AssertHelpers.assertThrows(
        "Cannot create an existing branch",
        IllegalArgumentException.class,
        "already exists",
        () -> sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName));
  }

  @Test
  public void testCreateBranchUseDefaultConfig() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(1L, ref.minSnapshotsToKeep().longValue());
    Assert.assertEquals(432000000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(Long.MAX_VALUE, ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateBranchUseCustomMinSnapshotsToKeep() throws NoSuchTableException {
    Integer minSnapshotsToKeep = 2;
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        tableName, branchName, minSnapshotsToKeep);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(432000000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(Long.MAX_VALUE, ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateBranchUseCustomMaxSnapshotAge() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d DAYS",
        tableName, branchName, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(1L, ref.minSnapshotsToKeep().longValue());
    Assert.assertEquals(maxSnapshotAge * 24 * 60 * 60 * 1000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(Long.MAX_VALUE, ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateBranchUseCustomMaxRefAge() throws NoSuchTableException {
    long maxRefAge = 10L;
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s RETAIN %d DAYS", tableName, branchName, maxRefAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(1L, ref.minSnapshotsToKeep().longValue());
    Assert.assertEquals(432000000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref.maxRefAgeMs().longValue());
  }

  private Table createDefaultTableAndInsert2Row() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    return validationCatalog.loadTable(tableIdent);
  }
}
