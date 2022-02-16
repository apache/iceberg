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

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.GC_ENABLED;

public class TestExpireSnapshotsProcedure extends SparkExtensionsTestBase {

  public TestExpireSnapshotsProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testExpireSnapshotsInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output = sql(
        "CALL %s.system.expire_snapshots('%s')",
        catalogName, tableIdent);
    assertEquals("Should not delete any files", ImmutableList.of(row(0L, 0L, 0L)), output);
  }

  @Test
  public void testExpireSnapshotsUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();
    Timestamp secondSnapshotTimestamp = Timestamp.from(Instant.ofEpochMilli(secondSnapshot.timestampMillis()));

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    // expire without retainLast param
    List<Object[]> output1 = sql(
        "CALL %s.system.expire_snapshots('%s', TIMESTAMP '%s')",
        catalogName, tableIdent, secondSnapshotTimestamp);
    assertEquals("Procedure output must match",
        ImmutableList.of(row(0L, 0L, 1L)),
        output1);

    table.refresh();

    Assert.assertEquals("Should expire one snapshot", 1, Iterables.size(table.snapshots()));

    sql("INSERT OVERWRITE %s VALUES (3, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(3L, "c"), row(4L, "d")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    Assert.assertEquals("Should be 3 snapshots", 3, Iterables.size(table.snapshots()));

    // expire with retainLast param
    List<Object[]> output = sql(
        "CALL %s.system.expire_snapshots('%s', TIMESTAMP '%s', 2)",
        catalogName, tableIdent, currentTimestamp);
    assertEquals("Procedure output must match",
        ImmutableList.of(row(2L, 2L, 1L)),
        output);
  }

  @Test
  public void testExpireSnapshotUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    List<Object[]> output = sql(
        "CALL %s.system.expire_snapshots(" +
            "older_than => TIMESTAMP '%s'," +
            "table => '%s'," +
            "retain_last => 1)",
        catalogName, currentTimestamp, tableIdent);
    assertEquals("Procedure output must match",
        ImmutableList.of(row(0L, 0L, 1L)),
        output);
  }

  @Test
  public void testExpireSnapshotsGCDisabled() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, GC_ENABLED);

    AssertHelpers.assertThrows("Should reject call",
        ValidationException.class, "Cannot expire snapshots: GC is disabled",
        () -> sql("CALL %s.system.expire_snapshots('%s')", catalogName, tableIdent));
  }

  @Test
  public void testInvalidExpireSnapshotsCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.expire_snapshots('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.expire_snapshots('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.expire_snapshots()", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type",
        () -> sql("CALL %s.system.expire_snapshots('n', 2.2)", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.expire_snapshots('')", catalogName));
  }

  @Test
  public void testResolvingTableInAnotherCatalog() throws IOException {
    String anotherCatalog = "another_" + catalogName;
    spark.conf().set("spark.sql.catalog." + anotherCatalog, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + anotherCatalog + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + anotherCatalog + ".warehouse", "file:" + temp.newFolder().toString());

    sql("CREATE TABLE %s.%s (id bigint NOT NULL, data string) USING iceberg", anotherCatalog, tableIdent);

    AssertHelpers.assertThrows("Should reject calls for a table in another catalog",
        IllegalArgumentException.class, "Cannot run procedure in catalog",
        () -> sql("CALL %s.system.expire_snapshots('%s')", catalogName, anotherCatalog + "." + tableName));
  }

  @Test
  public void testConcurrentExpireSnapshots() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));
    List<Object[]> output = sql(
        "CALL %s.system.expire_snapshots(" +
            "older_than => TIMESTAMP '%s'," +
            "table => '%s'," +
            "max_concurrent_deletes => %s," +
            "retain_last => 1)",
        catalogName, currentTimestamp, tableIdent, 4);
    assertEquals("Expiring snapshots concurrently should succeed", ImmutableList.of(row(0L, 0L, 3L)), output);
  }

  @Test
  public void testConcurrentExpireSnapshotsWithInvalidInput() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    AssertHelpers.assertThrows("Should throw an error when max_concurrent_deletes = 0",
        IllegalArgumentException.class, "max_concurrent_deletes should have value > 0",
        () -> sql("CALL %s.system.expire_snapshots(table => '%s', max_concurrent_deletes => %s)",
            catalogName, tableIdent, 0));

    AssertHelpers.assertThrows("Should throw an error when max_concurrent_deletes < 0 ",
        IllegalArgumentException.class, "max_concurrent_deletes should have value > 0",
        () -> sql(
            "CALL %s.system.expire_snapshots(table => '%s', max_concurrent_deletes => %s)",
            catalogName, tableIdent, -1));

  }

  @Test
  public void testExpireDeleteFiles() throws Exception {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg TBLPROPERTIES" +
        "('format-version'='2', 'write.delete.mode'='merge-on-read')", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    sql("DELETE FROM %s WHERE id=1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    table.refresh();

    Assert.assertEquals("Should have 1 delete manifest", 1, deleteManifests(table).size());
    Assert.assertEquals("Should have 1 delete file", 1, deleteFiles(table).size());
    Path deleteManifestPath = new Path(deleteManifests(table).iterator().next().path());
    Path deleteFilePath = new Path(String.valueOf(deleteFiles(table).iterator().next().path()));

    sql("CALL %s.system.rewrite_data_files(table => '%s', options => map" +
            "('delete-file-threshold','1', 'use-starting-sequence-number', 'false'))",
        catalogName, tableIdent);

    table.refresh();

    sql("INSERT INTO TABLE %s VALUES (5, 'e')", tableName); // this txn moves the file to the DELETED state
    sql("INSERT INTO TABLE %s VALUES (6, 'f')", tableName); // this txn removes the file reference

    table.refresh();
    Assert.assertEquals("Should have no delete manifests", 0, deleteManifests(table).size());
    Assert.assertEquals("Should have no delete files", 0, deleteFiles(table).size());

    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Assert.assertTrue("Delete manifest should still exist", localFs.exists(deleteManifestPath));
    Assert.assertTrue("Delete file should still exist", localFs.exists(deleteFilePath));

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));
    sql("CALL %s.system.expire_snapshots(" +
            "older_than => TIMESTAMP '%s'," +
            "table => '%s'," +
            "retain_last => 1)",
        catalogName, currentTimestamp, tableIdent);

    Assert.assertFalse("Delete manifest should be removed", localFs.exists(deleteManifestPath));
    Assert.assertFalse("Delete file should be removed", localFs.exists(deleteFilePath));
  }

  private Set<ManifestFile> deleteManifests(Table table) {
    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    return manifests.stream().filter(mf -> mf.content().equals(ManifestContent.DELETES))
        .collect(Collectors.toSet());
  }

  private Set<DeleteFile> deleteFiles(Table table) {
    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Stream<DeleteFile> dataFileStream = manifests.stream().filter(mf -> mf.content().equals(ManifestContent.DELETES))
        .flatMap(mf -> (StreamSupport.stream((ManifestFiles.readDeleteManifest(
            mf, table.io(), table.specs()).spliterator()), false)));
    return dataFileStream.collect(Collectors.toSet());
  }
}
