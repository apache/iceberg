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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestSnapshotRefSQL extends SparkExtensionsTestBase {
  public TestSnapshotRefSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testCreateBranch() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 10L;
    sql(
        "ALTER TABLE %s CREATE BRANCH %s AS OF VERSION %d WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS RETAIN %d DAYS",
        tableName, branchName, snapshotId, minSnapshotsToKeep, maxSnapshotAge, maxRefAge);
    table.refresh();
    SnapshotRef ref = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(maxSnapshotAge * 24 * 60 * 60 * 1000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref.maxRefAgeMs().longValue());

    AssertHelpers.assertThrows(
        "Cannot create an existing branch",
        IllegalArgumentException.class,
        "already exists",
        () -> sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName));

    String branchName2 = "b2";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName2);
    table.refresh();
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(branchName2);
    Assert.assertNotNull(ref2);
    Assert.assertEquals(1L, ref2.minSnapshotsToKeep().longValue());
    Assert.assertEquals(432000000L, ref2.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(Long.MAX_VALUE, ref2.maxRefAgeMs().longValue());

    String branchName3 = "b3";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        tableName, branchName3, minSnapshotsToKeep);
    table.refresh();
    SnapshotRef ref3 = ((BaseTable) table).operations().current().ref(branchName3);
    Assert.assertNotNull(ref3);
    Assert.assertEquals(minSnapshotsToKeep, ref3.minSnapshotsToKeep());
    Assert.assertEquals(432000000L, ref3.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(Long.MAX_VALUE, ref3.maxRefAgeMs().longValue());

    String branchName4 = "b4";
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d DAYS",
        tableName, branchName4, maxSnapshotAge);
    table.refresh();
    SnapshotRef ref4 = ((BaseTable) table).operations().current().ref(branchName4);
    Assert.assertNotNull(ref4);
    Assert.assertEquals(1L, ref2.minSnapshotsToKeep().longValue());
    Assert.assertEquals(maxSnapshotAge * 24 * 60 * 60 * 1000L, ref4.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(Long.MAX_VALUE, ref4.maxRefAgeMs().longValue());

    String branchName5 = "b5";
    sql("ALTER TABLE %s CREATE BRANCH %s RETAIN %d DAYS", tableName, branchName5, maxRefAge);
    table.refresh();
    SnapshotRef ref5 = ((BaseTable) table).operations().current().ref(branchName5);
    Assert.assertNotNull(ref5);
    Assert.assertEquals(1L, ref5.minSnapshotsToKeep().longValue());
    Assert.assertEquals(432000000L, ref5.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref5.maxRefAgeMs().longValue());
  }

  @Test
  public void testReplaceBranch() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    String branchName = "b1";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 10L;
    sql(
        "ALTER TABLE %s CREATE BRANCH %s AS OF VERSION %d WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS RETAIN %d DAYS",
        tableName, branchName, snapshotId, minSnapshotsToKeep, maxSnapshotAge, maxRefAge);

    table.refresh();
    SnapshotRef ref = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(maxSnapshotAge * 24 * 60 * 60 * 1000L, ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref.maxRefAgeMs().longValue());

    String branchName2 = "b2";
    AssertHelpers.assertThrows(
        "Cannot replace a branch that does not exist",
        IllegalArgumentException.class,
        "Branch does not exist",
        () -> sql("ALTER TABLE %s REPLACE BRANCH %s", tableName, branchName2));

    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();
    table.refresh();
    long snapshotId2 = table.currentSnapshot().snapshotId();
    Integer minSnapshotsToKeep2 = 3;
    long maxSnapshotAge2 = 5L;
    long maxRefAge2 = 20L;
    sql(
        "ALTER TABLE %s REPLACE BRANCH %s AS OF VERSION %d WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS RETAIN %d DAYS",
        tableName, branchName, snapshotId2, minSnapshotsToKeep2, maxSnapshotAge2, maxRefAge2);
    table.refresh();
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref2);
    Assert.assertEquals(snapshotId2, ref2.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep2, ref2.minSnapshotsToKeep());
    Assert.assertEquals(
        maxSnapshotAge2 * 24 * 60 * 60 * 1000L, ref2.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge2 * 24 * 60 * 60 * 1000L, ref2.maxRefAgeMs().longValue());

    Integer minSnapshotsToKeep3 = 9;
    sql(
        "ALTER TABLE %s REPLACE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        tableName, branchName, minSnapshotsToKeep3);
    table.refresh();
    SnapshotRef ref3 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref3);
    Assert.assertEquals(minSnapshotsToKeep3, ref3.minSnapshotsToKeep());
    Assert.assertEquals(
        maxSnapshotAge2 * 24 * 60 * 60 * 1000L, ref3.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge2 * 24 * 60 * 60 * 1000L, ref3.maxRefAgeMs().longValue());

    long maxSnapshotAge3 = 15L;
    sql(
        "ALTER TABLE %s REPLACE BRANCH %s WITH SNAPSHOT RETENTION %d DAYS",
        tableName, branchName, maxSnapshotAge3);
    table.refresh();
    SnapshotRef ref4 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref4);
    Assert.assertEquals(minSnapshotsToKeep3, ref3.minSnapshotsToKeep());
    Assert.assertEquals(
        maxSnapshotAge3 * 24 * 60 * 60 * 1000L, ref4.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge2 * 24 * 60 * 60 * 1000L, ref4.maxRefAgeMs().longValue());

    long maxRefAge3 = 60L;
    sql("ALTER TABLE %s REPLACE BRANCH %s RETAIN %d DAYS", tableName, branchName, maxRefAge3);
    table.refresh();
    SnapshotRef ref5 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref5);
    Assert.assertEquals(minSnapshotsToKeep3, ref3.minSnapshotsToKeep());
    Assert.assertEquals(
        maxSnapshotAge3 * 24 * 60 * 60 * 1000L, ref5.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(maxRefAge3 * 24 * 60 * 60 * 1000L, ref5.maxRefAgeMs().longValue());
  }

  @Test
  public void testDropBranch() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";

    AssertHelpers.assertThrows(
        "Cannot drop a branch that does not exist",
        IllegalArgumentException.class,
        "Branch does not exist",
        () -> sql("ALTER TABLE %s DROP BRANCH %s", tableName, branchName));

    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref1 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref1);

    sql("ALTER TABLE %s DROP BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNull(ref2);
  }

  @Test
  public void testRenameBranch() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    String newBranchName = "b2";

    AssertHelpers.assertThrows(
        "Cannot rename a branch that does not exist",
        IllegalArgumentException.class,
        "Branch does not exist",
        () -> sql("ALTER TABLE %s RENAME BRANCH %s TO %s", tableName, branchName, newBranchName));

    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNotNull(ref);

    sql("ALTER TABLE %s RENAME BRANCH %s TO %s", tableName, branchName, newBranchName);
    table.refresh();
    ref = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertNull(ref);
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(newBranchName);
    Assert.assertNotNull(ref2);
  }

  @Test
  public void testAlterBranchRefRetention() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branchName);
    table.refresh();
    SnapshotRef ref1 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertEquals(
        "will use default maxRefAgeMs.", Long.MAX_VALUE, ref1.maxRefAgeMs().longValue());

    sql("ALTER TABLE %s ALTER BRANCH %s RETAIN %d MINUTES", tableName, branchName, 1);
    table.refresh();
    SnapshotRef ref = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertEquals("Invalid modification time.", 60 * 1000, ref.maxRefAgeMs().longValue());

    String branchName2 = "b2";
    AssertHelpers.assertThrows(
        "Cannot alter an not exist branch",
        IllegalArgumentException.class,
        "Ref does not exist: " + branchName2,
        () -> sql("ALTER TABLE %s ALTER BRANCH %s RETAIN %d MINUTES", tableName, branchName2, 1));
  }

  @Test
  public void testAlterBranchSnapshotRetention() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    String branchName = "b1";
    int snapshotNum = 5;
    int days = 7;
    sql(
        "ALTER TABLE %s CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS",
        tableName, branchName, snapshotNum, days);
    table.refresh();
    SnapshotRef ref1 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertEquals(
        "Invalid modification snapshot day time.",
        7 * 24 * 60 * 60 * 1000L,
        ref1.maxSnapshotAgeMs().longValue());

    snapshotNum = 4;
    days = 6;
    sql(
        "ALTER TABLE %s ALTER BRANCH %s SET SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS",
        tableName, branchName, snapshotNum, days);
    table.refresh();
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertEquals(days * 24 * 60 * 60 * 1000L, ref2.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(snapshotNum, ref2.minSnapshotsToKeep().intValue());

    snapshotNum = 3;
    sql(
        "ALTER TABLE %s ALTER BRANCH %s SET SNAPSHOT RETENTION %d SNAPSHOTS",
        tableName, branchName, snapshotNum);
    table.refresh();
    SnapshotRef ref3 = ((BaseTable) table).operations().current().ref(branchName);
    Assert.assertEquals(days * 24 * 60 * 60 * 1000L, ref3.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(snapshotNum, ref3.minSnapshotsToKeep().intValue());
    Assert.assertEquals(snapshotNum, ref3.minSnapshotsToKeep().intValue());

    String branchName2 = "b2";
    AssertHelpers.assertThrows(
        "Cannot alter snapshot retention of a branch that doesn't exist!",
        IllegalArgumentException.class,
        String.format("Branch does not exist: %s", branchName2),
        () ->
            sql(
                "ALTER TABLE %s ALTER BRANCH %s SET SNAPSHOT RETENTION %d SNAPSHOTS",
                tableName, "b2", 4));

    AssertHelpers.assertThrows(
        "Cannot alter snapshot retention with invalid value",
        IllegalArgumentException.class,
        "Min snapshots to keep must be greater than 0",
        () ->
            sql(
                "ALTER TABLE %s ALTER BRANCH %s SET SNAPSHOT RETENTION %d SNAPSHOTS",
                tableName, branchName, 0));

    AssertHelpers.assertThrows(
        "Cannot alter branch snapshot retention with invalid value",
        IllegalArgumentException.class,
        "Max snapshot age must be greater than 0 ms",
        () ->
            sql(
                "ALTER TABLE %s ALTER BRANCH %s SET SNAPSHOT RETENTION %d DAYS",
                tableName, branchName, -1));
  }

  private Table createDefaultTableAndInsert2Row() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);
    return table;
  }
}
