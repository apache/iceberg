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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotUtil {
  @TempDir private File tableDir;
  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partition spec used to create tables
  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  protected File metadataDir = null;
  public TestTables.TestTable table = null;

  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  private long snapshotBaseTimestamp;
  private long snapshotBaseId;
  private long snapshotBranchId;
  private long snapshotMain1Id;
  private long snapshotMain2Id;
  private long snapshotFork0Id;
  private long snapshotFork1Id;
  private long snapshotFork2Id;

  private Snapshot appendFileTo(String branch) {
    table.newFastAppend().appendFile(FILE_A).toBranch(branch).commit();
    return table.snapshot(branch);
  }

  private Snapshot appendFileToMain() {
    return appendFileTo(SnapshotRef.MAIN_BRANCH);
  }

  @BeforeEach
  public void before() throws Exception {
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = TestTables.create(tableDir, "test", SCHEMA, SPEC, 2);
    Snapshot snapshotBase = appendFileToMain();
    this.snapshotBaseId = snapshotBase.snapshotId();
    this.snapshotBaseTimestamp = snapshotBase.timestampMillis();
    TestHelpers.waitUntilAfter(snapshotBaseTimestamp);

    this.snapshotMain1Id = appendFileToMain().snapshotId();
    this.snapshotMain2Id = appendFileToMain().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotBaseId).commit();
    this.snapshotBranchId = appendFileTo(branchName).snapshotId();

    // Create a branch that leads back to an expired snapshot
    String forkBranch = "fork";
    table.manageSnapshots().createBranch(forkBranch, snapshotBaseId).commit();
    this.snapshotFork0Id = appendFileTo(forkBranch).snapshotId();
    this.snapshotFork1Id = appendFileTo(forkBranch).snapshotId();
    this.snapshotFork2Id = appendFileTo(forkBranch).snapshotId();
    table.expireSnapshots().expireSnapshotId(snapshotFork0Id).commit();
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void isParentAncestorOf() {
    assertThat(SnapshotUtil.isParentAncestorOf(table, snapshotMain1Id, snapshotBaseId)).isTrue();
    assertThat(SnapshotUtil.isParentAncestorOf(table, snapshotBranchId, snapshotMain1Id)).isFalse();
    assertThat(SnapshotUtil.isParentAncestorOf(table, snapshotFork2Id, snapshotFork0Id)).isTrue();
  }

  @Test
  public void isAncestorOf() {
    assertThat(SnapshotUtil.isAncestorOf(table, snapshotMain1Id, snapshotBaseId)).isTrue();
    assertThat(SnapshotUtil.isAncestorOf(table, snapshotBranchId, snapshotMain1Id)).isFalse();
    assertThat(SnapshotUtil.isAncestorOf(table, snapshotFork2Id, snapshotFork0Id)).isFalse();

    assertThat(SnapshotUtil.isAncestorOf(table, snapshotMain1Id)).isTrue();
    assertThat(SnapshotUtil.isAncestorOf(table, snapshotBranchId)).isFalse();
  }

  @Test
  public void currentAncestors() {
    Iterable<Snapshot> snapshots = SnapshotUtil.currentAncestors(table);
    expectedSnapshots(new long[] {snapshotMain2Id, snapshotMain1Id, snapshotBaseId}, snapshots);

    List<Long> snapshotList = SnapshotUtil.currentAncestorIds(table);
    assertThat(snapshotList.toArray(new Long[0]))
        .isEqualTo(new Long[] {snapshotMain2Id, snapshotMain1Id, snapshotBaseId});
  }

  @Test
  public void oldestAncestor() {
    Snapshot snapshot = SnapshotUtil.oldestAncestor(table);
    assertThat(snapshot.snapshotId()).isEqualTo(snapshotBaseId);

    snapshot = SnapshotUtil.oldestAncestorOf(table, snapshotMain2Id);
    assertThat(snapshot.snapshotId()).isEqualTo(snapshotBaseId);

    snapshot = SnapshotUtil.oldestAncestorAfter(table, snapshotBaseTimestamp + 1);
    assertThat(snapshot.snapshotId()).isEqualTo(snapshotMain1Id);
  }

  @Test
  public void snapshotsBetween() {
    List<Long> snapshotIdsBetween =
        SnapshotUtil.snapshotIdsBetween(table, snapshotBaseId, snapshotMain2Id);
    assertThat(snapshotIdsBetween.toArray(new Long[0]))
        .isEqualTo(new Long[] {snapshotMain2Id, snapshotMain1Id});

    Iterable<Snapshot> ancestorsBetween =
        SnapshotUtil.ancestorsBetween(table, snapshotMain2Id, snapshotMain1Id);
    expectedSnapshots(new long[] {snapshotMain2Id}, ancestorsBetween);

    ancestorsBetween = SnapshotUtil.ancestorsBetween(table, snapshotMain2Id, snapshotBranchId);
    expectedSnapshots(
        new long[] {snapshotMain2Id, snapshotMain1Id, snapshotBaseId}, ancestorsBetween);
  }

  @Test
  public void ancestorsOf() {
    Iterable<Snapshot> snapshots = SnapshotUtil.ancestorsOf(snapshotFork2Id, table::snapshot);
    expectedSnapshots(new long[] {snapshotFork2Id, snapshotFork1Id}, snapshots);

    Iterator<Snapshot> snapshotIter = snapshots.iterator();
    while (snapshotIter.hasNext()) {
      snapshotIter.next();
    }

    // Once snapshot iterator has been exhausted, call hasNext again to make sure it is stable.
    assertThat(snapshotIter).isExhausted();
  }

  private void expectedSnapshots(long[] snapshotIdExpected, Iterable<Snapshot> snapshotsActual) {
    long[] actualSnapshots =
        StreamSupport.stream(snapshotsActual.spliterator(), false)
            .mapToLong(Snapshot::snapshotId)
            .toArray();
    assertThat(actualSnapshots).isEqualTo(snapshotIdExpected);
  }

  @Test
  public void schemaForRef() {
    Schema initialSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct()).isEqualTo(initialSchema.asStruct());

    assertThat(SnapshotUtil.schemaFor(table, null).asStruct()).isEqualTo(initialSchema.asStruct());
    assertThat(SnapshotUtil.schemaFor(table, "non-existing-ref").asStruct())
        .isEqualTo(initialSchema.asStruct());
    assertThat(SnapshotUtil.schemaFor(table, SnapshotRef.MAIN_BRANCH).asStruct())
        .isEqualTo(initialSchema.asStruct());
  }

  @Test
  public void schemaForBranch() {
    Schema initialSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct()).isEqualTo(initialSchema.asStruct());

    String branch = "branch";
    table.manageSnapshots().createBranch(branch).commit();

    assertThat(SnapshotUtil.schemaFor(table, branch).asStruct())
        .isEqualTo(initialSchema.asStruct());

    table.updateSchema().addColumn("zip", Types.IntegerType.get()).commit();
    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()),
            optional(3, "zip", Types.IntegerType.get()));

    assertThat(table.schema().asStruct()).isEqualTo(expected.asStruct());
    assertThat(SnapshotUtil.schemaFor(table, branch).asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  public void schemaForTag() {
    Schema initialSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct()).isEqualTo(initialSchema.asStruct());

    String tag = "tag";
    table.manageSnapshots().createTag(tag, table.currentSnapshot().snapshotId()).commit();

    assertThat(SnapshotUtil.schemaFor(table, tag).asStruct()).isEqualTo(initialSchema.asStruct());

    table.updateSchema().addColumn("zip", Types.IntegerType.get()).commit();
    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()),
            optional(3, "zip", Types.IntegerType.get()));

    assertThat(table.schema().asStruct()).isEqualTo(expected.asStruct());
    assertThat(SnapshotUtil.schemaFor(table, tag).asStruct()).isEqualTo(initialSchema.asStruct());
  }
}
