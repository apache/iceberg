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
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestReplaceBranch extends ExtensionsTestBase {

  private static final String[] TIME_UNITS = {"DAYS", "HOURS", "MINUTES"};

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

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testReplaceBranchFailsForTag() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    String tagName = "tag1";

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);
    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagName, first).commit();
    df.writeTo(tableName).append();
    long second = table.currentSnapshot().snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d",
                    tableName, tagName, second))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref tag1 is a tag not a branch");
  }

  @TestTemplate
  public void testReplaceBranch() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long first = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    long expectedMaxRefAgeMs = 1000;
    int expectedMinSnapshotsToKeep = 2;
    long expectedMaxSnapshotAgeMs = 1000;
    table
        .manageSnapshots()
        .createBranch(branchName, first)
        .setMaxRefAgeMs(branchName, expectedMaxRefAgeMs)
        .setMinSnapshotsToKeep(branchName, expectedMinSnapshotsToKeep)
        .setMaxSnapshotAgeMs(branchName, expectedMaxSnapshotAgeMs)
        .commit();

    df.writeTo(tableName).append();
    long second = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d", tableName, branchName, second);

    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref).isNotNull();
    assertThat(ref.snapshotId()).isEqualTo(second);
    assertThat(ref.minSnapshotsToKeep().intValue()).isEqualTo(expectedMinSnapshotsToKeep);
    assertThat(ref.maxSnapshotAgeMs().longValue()).isEqualTo(expectedMaxSnapshotAgeMs);
    assertThat(ref.maxRefAgeMs().longValue()).isEqualTo(expectedMaxRefAgeMs);
  }

  @TestTemplate
  public void testReplaceBranchDoesNotExist() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d",
                    tableName, "someBranch", table.currentSnapshot().snapshotId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: someBranch");
  }

  @TestTemplate
  public void testReplaceBranchWithRetain() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long first = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, first).commit();
    df.writeTo(tableName).append();
    long second = table.currentSnapshot().snapshotId();

    long maxRefAge = 10;
    for (String timeUnit : TIME_UNITS) {
      sql(
          "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d RETAIN %d %s",
          tableName, branchName, second, maxRefAge, timeUnit);

      table.refresh();
      SnapshotRef ref = table.refs().get(branchName);
      assertThat(ref).isNotNull();
      assertThat(ref.snapshotId()).isEqualTo(second);
      assertThat(ref.minSnapshotsToKeep()).isNull();
      assertThat(ref.maxSnapshotAgeMs()).isNull();
      assertThat(ref.maxRefAgeMs().longValue())
          .isEqualTo(TimeUnit.valueOf(timeUnit).toMillis(maxRefAge));
    }
  }

  @TestTemplate
  public void testReplaceBranchWithSnapshotRetention() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    String branchName = "b1";
    Table table = validationCatalog.loadTable(tableIdent);
    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(branchName, first).commit();
    df.writeTo(tableName).append();
    long second = table.currentSnapshot().snapshotId();

    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2;
    Long maxRefAgeMs = table.refs().get(branchName).maxRefAgeMs();
    for (String timeUnit : TIME_UNITS) {
      sql(
          "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d WITH SNAPSHOT RETENTION %d SNAPSHOTS %d %s",
          tableName, branchName, second, minSnapshotsToKeep, maxSnapshotAge, timeUnit);

      table.refresh();
      SnapshotRef ref = table.refs().get(branchName);
      assertThat(ref).isNotNull();
      assertThat(ref.snapshotId()).isEqualTo(second);
      assertThat(ref.minSnapshotsToKeep()).isEqualTo(minSnapshotsToKeep);
      assertThat(ref.maxSnapshotAgeMs().longValue())
          .isEqualTo(TimeUnit.valueOf(timeUnit).toMillis(maxSnapshotAge));
      assertThat(ref.maxRefAgeMs()).isEqualTo(maxRefAgeMs);
    }
  }

  @TestTemplate
  public void testReplaceBranchWithRetainAndSnapshotRetention() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long first = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, first).commit();
    df.writeTo(tableName).append();
    long second = table.currentSnapshot().snapshotId();

    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2;
    long maxRefAge = 10;
    for (String timeUnit : TIME_UNITS) {
      sql(
          "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d RETAIN %d %s WITH SNAPSHOT RETENTION %d SNAPSHOTS %d %s",
          tableName,
          branchName,
          second,
          maxRefAge,
          timeUnit,
          minSnapshotsToKeep,
          maxSnapshotAge,
          timeUnit);

      table.refresh();
      SnapshotRef ref = table.refs().get(branchName);
      assertThat(ref).isNotNull();
      assertThat(ref.snapshotId()).isEqualTo(second);
      assertThat(ref.minSnapshotsToKeep()).isEqualTo(minSnapshotsToKeep);
      assertThat(ref.maxSnapshotAgeMs().longValue())
          .isEqualTo(TimeUnit.valueOf(timeUnit).toMillis(maxSnapshotAge));
      assertThat(ref.maxRefAgeMs().longValue())
          .isEqualTo(TimeUnit.valueOf(timeUnit).toMillis(maxRefAge));
    }
  }

  @TestTemplate
  public void testCreateOrReplace() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long first = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    df.writeTo(tableName).append();
    long second = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(branchName, second).commit();

    sql(
        "ALTER TABLE %s CREATE OR REPLACE BRANCH %s AS OF VERSION %d",
        tableName, branchName, first);

    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    assertThat(ref).isNotNull();
    assertThat(ref.snapshotId()).isEqualTo(first);
  }
}
