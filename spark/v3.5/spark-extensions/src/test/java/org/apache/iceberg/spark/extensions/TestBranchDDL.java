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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBranchDDL extends ExtensionsTestBase {

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties()
      }
    };
  }

  @TestTemplate
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
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.minSnapshotsToKeep()).isEqualTo(minSnapshotsToKeep);
    assertThat(ref.maxSnapshotAgeMs().longValue())
        .isEqualTo(TimeUnit.DAYS.toMillis(maxSnapshotAge));
    assertThat(ref.maxRefAgeMs().longValue()).isEqualTo(TimeUnit.DAYS.toMillis(maxRefAge));

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref b1 already exists");
  }

  @TestTemplate
  public void testCreateBranchOnEmptyTable() {
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, "b1");
    Table table = validationCatalog.loadTable(tableIdent);

    SnapshotRef mainRef = table.refs().get(SnapshotRef.MAIN_BRANCH);
    assertThat(mainRef).isNull();

    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref).isNotNull();
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs()).isNull();
    assertThat(ref.maxRefAgeMs()).isNull();

    Snapshot snapshot = table.snapshot(ref.snapshotId());
    assertThat(snapshot.parentId()).isNull();
    assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
    assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
    assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
    assertThat(snapshot.removedDeleteFiles(table.io())).isEmpty();
  }

  @TestTemplate
  public void testCreateBranchUseDefaultConfig() throws NoSuchTableException {
    Table table = insertRows();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs()).isNull();
    assertThat(ref.maxRefAgeMs()).isNull();
  }

  @TestTemplate
  public void testCreateBranchUseCustomMinSnapshotsToKeep() throws NoSuchTableException {
    Integer minSnapshotsToKeep = 2;
    Table table = insertRows();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        tableName, branchName, minSnapshotsToKeep);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.minSnapshotsToKeep()).isEqualTo(minSnapshotsToKeep);
    assertThat(ref.maxSnapshotAgeMs()).isNull();
    assertThat(ref.maxRefAgeMs()).isNull();
  }

  @TestTemplate
  public void testCreateBranchUseCustomMaxSnapshotAge() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = insertRows();
    String branchName = "b1";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d DAYS",
        tableName, branchName, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref).isNotNull();
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs().longValue())
        .isEqualTo(TimeUnit.DAYS.toMillis(maxSnapshotAge));
    assertThat(ref.maxRefAgeMs()).isNull();
  }

  @TestTemplate
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
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs().longValue())
        .isEqualTo(TimeUnit.DAYS.toMillis(maxSnapshotAge));
    assertThat(ref.maxRefAgeMs()).isNull();
  }

  @TestTemplate
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
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.minSnapshotsToKeep()).isEqualTo(minSnapshotsToKeep);
    assertThat(ref.maxSnapshotAgeMs().longValue())
        .isEqualTo(TimeUnit.DAYS.toMillis(maxSnapshotAge));
    assertThat(ref.maxRefAgeMs()).isNull();

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION",
                    tableName, branchName))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("no viable alternative at input 'WITH SNAPSHOT RETENTION'");
  }

  @TestTemplate
  public void testCreateBranchUseCustomMaxRefAge() throws NoSuchTableException {
    long maxRefAge = 10L;
    Table table = insertRows();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s RETAIN %d DAYS", tableName, branchName, maxRefAge);
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs()).isNull();
    assertThat(ref.maxRefAgeMs().longValue()).isEqualTo(TimeUnit.DAYS.toMillis(maxRefAge));

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

  @TestTemplate
  public void testDropBranch() throws NoSuchTableException {
    insertRows();

    Table table = validationCatalog.loadTable(tableIdent);
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());

    sql("ALTER TABLE %s DROP BRANCH %s", tableName, branchName);
    table.refresh();

    ref = table.refs().get(branchName);
    assertThat(ref).isNull();
  }

  @TestTemplate
  public void testDropBranchDoesNotExist() {
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, "nonExistingBranch"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: nonExistingBranch");
  }

  @TestTemplate
  public void testDropBranchFailsForTag() throws NoSuchTableException {
    String tagName = "b1";
    Table table = insertRows();
    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, tagName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref b1 is a tag not a branch");
  }

  @TestTemplate
  public void testDropBranchNonConformingName() {
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, "123"))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input '123'");
  }

  @TestTemplate
  public void testDropMainBranchFails() {
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP BRANCH main", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove main branch");
  }

  @TestTemplate
  public void testDropBranchIfExists() {
    String branchName = "nonExistingBranch";
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.refs().get(branchName)).isNull();

    sql("ALTER TABLE %s DROP BRANCH IF EXISTS %s", tableName, branchName);
    table.refresh();

    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref).isNull();
  }

  private Table insertRows() throws NoSuchTableException {
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    return validationCatalog.loadTable(tableIdent);
  }

  @TestTemplate
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

  @TestTemplate
  public void testCreateOrReplaceBranchOnEmptyTable() {
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE OR REPLACE BRANCH %s", tableName, "b1");
    Table table = validationCatalog.loadTable(tableIdent);

    SnapshotRef mainRef = table.refs().get(SnapshotRef.MAIN_BRANCH);
    assertThat(mainRef).isNull();

    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref).isNotNull();
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs()).isNull();
    assertThat(ref.maxRefAgeMs()).isNull();

    Snapshot snapshot = table.snapshot(ref.snapshotId());
    assertThat(snapshot.parentId()).isNull();
    assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
    assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
    assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
    assertThat(snapshot.removedDeleteFiles(table.io())).isEmpty();
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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
