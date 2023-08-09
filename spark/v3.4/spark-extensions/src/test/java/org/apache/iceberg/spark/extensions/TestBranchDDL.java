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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.extensions.IcebergParseException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestBranchDDL extends SparkExtensionsTestBase {

  @Before
  public void before() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

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

  public TestBranchDDL(String catalog, String implementation, Map<String, String> properties) {
    super(catalog, implementation, properties);
  }

  @Test
  public void testCreateBranch() throws NoSuchTableException {
    Table table = insertRows();
    long snapshotId = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 10L;
    sql(
        "ALTER TABLE %s CREATE BRANCH %s AS OF VERSION %d RETAIN %d DAYS WITH SNAPSHOT RETENTION %d SNAPSHOTS %d days",
        tableName, branchName, snapshotId, maxRefAge, minSnapshotsToKeep, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref b1 already exists");
  }

  @Test
  public void testCreateBranchOnEmptyTable() {
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, "b1");
    Table table = validationCatalog.loadTable(tableIdent);

    SnapshotRef mainRef = table.refs().get(SnapshotRef.MAIN_BRANCH);
    Assertions.assertThat(mainRef).isNull();

    SnapshotRef ref = table.refs().get(branchName);
    Assertions.assertThat(ref).isNotNull();
    Assertions.assertThat(ref.minSnapshotsToKeep()).isNull();
    Assertions.assertThat(ref.maxSnapshotAgeMs()).isNull();
    Assertions.assertThat(ref.maxRefAgeMs()).isNull();

    Snapshot snapshot = table.snapshot(ref.snapshotId());
    Assertions.assertThat(snapshot.parentId()).isNull();
    Assertions.assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.removedDeleteFiles(table.io())).isEmpty();
  }

  @Test
  public void testCreateBranchUseDefaultConfig() throws NoSuchTableException {
    Table table = insertRows();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchUseCustomMinSnapshotsToKeep() throws NoSuchTableException {
    Integer minSnapshotsToKeep = 2;
    Table table = insertRows();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        tableName, branchName, minSnapshotsToKeep);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchUseCustomMaxSnapshotAge() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = insertRows();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d DAYS",
        tableName, branchName, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNotNull(ref);
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchIfNotExists() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = insertRows();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d DAYS",
        tableName, branchName, maxSnapshotAge);
    sql("ALTER TABLE %s CREATE BRANCH IF NOT EXISTS %s", tableName, branchName);

    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchUseCustomMinSnapshotsToKeepAndMaxSnapshotAge()
      throws NoSuchTableException {
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    Table table = insertRows();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS",
        tableName, branchName, minSnapshotsToKeep, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertNull(ref.maxRefAgeMs());

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION",
                    tableName, branchName))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("no viable alternative at input 'WITH SNAPSHOT RETENTION'");
  }

  @Test
  public void testCreateBranchUseCustomMaxRefAge() throws NoSuchTableException {
    long maxRefAge = 10L;
    Table table = insertRows();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s RETAIN %d DAYS", tableName, branchName, maxRefAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s CREATE BRANCH %s RETAIN", tableName, branchName))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input");

    Assertions.assertThatThrownBy(
            () ->
                sql("ALTER TABLE %s CREATE BRANCH %s RETAIN %s DAYS", tableName, branchName, "abc"))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE BRANCH %s RETAIN %d SECONDS",
                    tableName, branchName, maxRefAge))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'}");
  }

  @Test
  public void testDropBranch() throws NoSuchTableException {
    insertRows();

    Table table = validationCatalog.loadTable(tableIdent);
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());

    sql("ALTER TABLE %s DROP BRANCH %s", tableName, branchName);
    table.refresh();

    ref = table.refs().get(branchName);
    Assert.assertNull(ref);
  }

  @Test
  public void testDropBranchDoesNotExist() {
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, "nonExistingBranch"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: nonExistingBranch");
  }

  @Test
  public void testDropBranchFailsForTag() throws NoSuchTableException {
    String tagName = "b1";
    Table table = insertRows();
    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, tagName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref b1 is a tag not a branch");
  }

  @Test
  public void testDropBranchNonConformingName() {
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, "123"))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input '123'");
  }

  @Test
  public void testDropMainBranchFails() {
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP BRANCH main", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove main branch");
  }

  @Test
  public void testDropBranchIfExists() {
    String branchName = "nonExistingBranch";
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertNull(table.refs().get(branchName));

    sql("ALTER TABLE %s DROP BRANCH IF EXISTS %s", tableName, branchName);
    table.refresh();

    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertNull(ref);
  }

  private Table insertRows() throws NoSuchTableException {
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    return validationCatalog.loadTable(tableIdent);
  }

  @Test
  public void createOrReplace() throws NoSuchTableException {
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    insertRows();
    long second = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(branchName, second).commit();

    sql(
        "ALTER TABLE %s CREATE OR REPLACE BRANCH %s AS OF VERSION %d",
        tableName, branchName, first);
    table.refresh();
    assertThat(table.refs().get(branchName).snapshotId()).isEqualTo(second);
  }

  @Test
  public void testCreateOrReplaceBranchOnEmptyTable() {
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE OR REPLACE BRANCH %s", tableName, "b1");
    Table table = validationCatalog.loadTable(tableIdent);

    SnapshotRef mainRef = table.refs().get(SnapshotRef.MAIN_BRANCH);
    Assertions.assertThat(mainRef).isNull();

    SnapshotRef ref = table.refs().get(branchName);
    Assertions.assertThat(ref).isNotNull();
    Assertions.assertThat(ref.minSnapshotsToKeep()).isNull();
    Assertions.assertThat(ref.maxSnapshotAgeMs()).isNull();
    Assertions.assertThat(ref.maxRefAgeMs()).isNull();

    Snapshot snapshot = table.snapshot(ref.snapshotId());
    Assertions.assertThat(snapshot.parentId()).isNull();
    Assertions.assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.removedDeleteFiles(table.io())).isEmpty();
  }

  @Test
  public void createOrReplaceWithNonExistingBranch() throws NoSuchTableException {
    Table table = insertRows();
    String branchName = "b1";
    insertRows();
    long snapshotId = table.currentSnapshot().snapshotId();

    sql(
        "ALTER TABLE %s CREATE OR REPLACE BRANCH %s AS OF VERSION %d",
        tableName, branchName, snapshotId);
    table.refresh();
    assertThat(table.refs().get(branchName).snapshotId()).isEqualTo(snapshotId);
  }

  @Test
  public void replaceBranch() throws NoSuchTableException {
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    long expectedMaxRefAgeMs = 1000;
    table
        .manageSnapshots()
        .createBranch(branchName, first)
        .setMaxRefAgeMs(branchName, expectedMaxRefAgeMs)
        .commit();

    insertRows();
    long second = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d", tableName, branchName, second);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref.snapshotId()).isEqualTo(second);
    assertThat(ref.maxRefAgeMs()).isEqualTo(expectedMaxRefAgeMs);
  }

  @Test
  public void replaceBranchDoesNotExist() throws NoSuchTableException {
    Table table = insertRows();

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d",
                    tableName, "someBranch", table.currentSnapshot().snapshotId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: someBranch");
  }
}
