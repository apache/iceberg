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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper.FilePositions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Covers the fallback path of the DV-only committer: merging the deletion vectors of a checkpoint
 * again after a concurrent commit invalidated them.
 */
class TestDvOnlyDeletionVectorMerger {

  private static final Set<Integer> EQUALITY_FIELD_IDS = ImmutableSet.of(1);

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private Table table;
  private DeletionVectorHelper deletionVectorHelper;
  private OutputFileFactory fileFactory;
  private DvOnlyDeletionVectorMerger merger;

  @BeforeEach
  void before() {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    FileFormat.PARQUET.name(),
                    TableProperties.FORMAT_VERSION,
                    "3"));
    deletionVectorHelper = new DeletionVectorHelper(table);
    fileFactory = OutputFileFactory.builderFor(table, 1, 1L).format(FileFormat.PUFFIN).build();
    merger =
        new DvOnlyDeletionVectorMerger(
            table, SnapshotRef.MAIN_BRANCH, EQUALITY_FIELD_IDS, fileFactory);
  }

  @Test
  void mergesWithAVectorCommittedInTheMeantime() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"), row(3, "c"));
    long baseline = table.currentSnapshot().snapshotId();
    WriteResult checkpoint = checkpoint(writeVector(file, null, 1L));

    // What the commit of the previous checkpoint landing after the barrier looks like.
    DeleteFile committed = vector(writeVector(file, null, 0L));
    commit(committed, null);

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .singleElement()
        .satisfies(dv -> assertVector(dv, file, 0L, 1L));
    assertThat(merged.replacedDeletionVectors())
        .singleElement()
        .extracting(DeleteFile::location)
        .isEqualTo(committed.location());

    commit(merged);
    assertRows(row(3, "c"));
  }

  @Test
  void resolvesDeletesAgainstTheFilesThatReplacedARemovedFile() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"), row(3, "c"));
    long baseline = table.currentSnapshot().snapshotId();
    // Deletes id 2, which sits at position 1.
    WriteResult checkpoint = checkpoint(writeVector(file, null, 1L));

    // A compaction reorders the rows, so id 2 ends up at position 2 of the replacement.
    DataFile compacted = writeFile("compacted", row(3, "c"), row(1, "a"), row(2, "b"));
    rewrite(ImmutableSet.of(file), compacted);

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .singleElement()
        .satisfies(dv -> assertVector(dv, compacted, 2L));
    assertThat(merged.replacedDeletionVectors()).isEmpty();

    commit(merged);
    assertRows(row(1, "a"), row(3, "c"));
  }

  @Test
  void onlyResolvesAgainThePositionsTheCheckpointAdded() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"), row(3, "c"));
    DeleteFile earlier = vector(writeVector(file, null, 0L));
    commit(earlier, null);
    // Id 1 was deleted earlier and inserted again since.
    DataFile reinserted = appendFile("reinserted", row(1, "x"));
    long baseline = table.currentSnapshot().snapshotId();

    // Deletes id 2 and, as the writer operator does, carries over the earlier vector.
    WriteResult checkpoint = checkpoint(writeVector(file, earlier, 1L));

    DataFile compacted = writeFile("compacted", row(1, "x"), row(2, "b"), row(3, "c"));
    rewrite(ImmutableSet.of(file, reinserted), ImmutableSet.of(earlier), compacted);

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .as("The earlier delete of id 1 must not remove the row inserted after it")
        .singleElement()
        .satisfies(dv -> assertVector(dv, compacted, 1L));

    commit(merged);
    assertRows(row(1, "x"), row(3, "c"));
  }

  @Test
  void keepsTheVectorAReplacementFileAlreadyCarries() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"), row(3, "c"));
    long baseline = table.currentSnapshot().snapshotId();
    // Deletes id 1.
    WriteResult checkpoint = checkpoint(writeVector(file, null, 0L));

    DataFile compacted = writeFile("compacted", row(2, "b"), row(3, "c"), row(1, "a"));
    rewrite(ImmutableSet.of(file), compacted);
    // Id 2 is deleted from the replacement by another commit.
    DeleteFile existing = vector(writeVector(compacted, null, 0L));
    commit(existing, null);

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .singleElement()
        .satisfies(dv -> assertVector(dv, compacted, 0L, 2L));
    assertThat(merged.replacedDeletionVectors())
        .singleElement()
        .extracting(DeleteFile::location)
        .isEqualTo(existing.location());

    commit(merged);
    assertRows(row(3, "c"));
  }

  @Test
  void leavesVectorsOfFilesThatStayedAlone() throws IOException {
    DataFile kept = appendFile("kept", row(1, "a"), row(2, "b"));
    DataFile rewritten = appendFile("rewritten", row(3, "c"), row(4, "d"));
    long baseline = table.currentSnapshot().snapshotId();
    WriteResult checkpoint =
        checkpoint(writeVector(kept, null, 1L), writeVector(rewritten, null, 0L));

    DataFile compacted = writeFile("compacted", row(4, "d"), row(3, "c"));
    rewrite(ImmutableSet.of(rewritten), compacted);

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors()).hasSize(2);
    Map<String, DeleteFile> byDataFile = Maps.newHashMap();
    merged.deletionVectors().forEach(dv -> byDataFile.put(dv.referencedDataFile(), dv));
    assertVector(byDataFile.get(kept.location()), kept, 1L);
    assertVector(byDataFile.get(compacted.location()), compacted, 1L);

    commit(merged);
    assertRows(row(1, "a"), row(4, "d"));
  }

  @Test
  void followsAFileThroughRepeatedRewrites() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"));
    long baseline = table.currentSnapshot().snapshotId();
    WriteResult checkpoint = checkpoint(writeVector(file, null, 1L));

    DataFile first = writeFile("first", row(2, "b"), row(1, "a"));
    rewrite(ImmutableSet.of(file), first);
    DataFile second = writeFile("second", row(1, "a"), row(2, "b"));
    rewrite(ImmutableSet.of(first), second);

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .singleElement()
        .satisfies(dv -> assertVector(dv, second, 1L));
    commit(merged);
    assertRows(row(1, "a"));
  }

  @Test
  void onlyReadsTheFilesThatReplacedARemovedFile() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"));
    DataFile other = appendFile("other", row(3, "c"));
    long baseline = table.currentSnapshot().snapshotId();
    WriteResult checkpoint = checkpoint(writeVector(file, null, 0L));

    DataFile compacted = writeFile("compacted", row(2, "b"), row(1, "a"));
    rewrite(ImmutableSet.of(file), compacted);
    // A rewrite of an unrelated file whose replacement cannot be read: reading it would fail.
    DataFile unrelated = writeFile("unrelated", row(3, "c"));
    rewrite(ImmutableSet.of(other), unrelated);
    table.io().deleteFile(unrelated.location());

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .singleElement()
        .satisfies(dv -> assertVector(dv, compacted, 1L));
  }

  @Test
  void skipsReplacementsInOtherPartitions() throws IOException {
    usePartitionedTable();
    DataFile fileA = appendFile("file-a", "a", row(1, "a"), row(2, "a"));
    DataFile fileB = appendFile("file-b", "b", row(3, "b"));
    long baseline = table.currentSnapshot().snapshotId();
    WriteResult checkpoint = checkpoint(writeVector(fileA, null, 0L));

    // One rewrite covering both partitions; the replacement in the other partition cannot be read.
    DataFile compactedA = writeFile("compacted-a", "a", row(2, "a"), row(1, "a"));
    DataFile compactedB = writeFile("compacted-b", "b", row(3, "b"));
    table
        .newRewrite()
        .deleteFile(fileA)
        .deleteFile(fileB)
        .addFile(compactedA)
        .addFile(compactedB)
        .commit();
    table.io().deleteFile(compactedB.location());

    table.refresh();
    DvOnlyDeletionVectorMerger.Merged merged = merger.merge(checkpoint, baseline);

    assertThat(merged.deletionVectors())
        .singleElement()
        .satisfies(dv -> assertVector(dv, compactedA, 1L));
  }

  @Test
  void refusesToMergeAgainstAnExpiredBaseline() throws IOException {
    DataFile file = appendFile("file", row(1, "a"), row(2, "b"));
    long baseline = table.currentSnapshot().snapshotId();
    WriteResult checkpoint = checkpoint(writeVector(file, null, 1L));
    rewrite(ImmutableSet.of(file), writeFile("compacted", row(1, "a"), row(2, "b")));
    expire(baseline);

    table.refresh();
    assertThat(merger.isTraceable(baseline)).isFalse();
    assertThatThrownBy(() -> merger.merge(checkpoint, baseline))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("is no longer an ancestor of branch");
  }

  private void expire(long snapshotId) {
    table.expireSnapshots().expireSnapshotId(snapshotId).cleanExpiredFiles(false).commit();
  }

  /** Replaces the table with one partitioned by {@code data}. */
  private void usePartitionedTable() {
    CATALOG_EXTENSION.catalog().dropTable(TestFixtures.TABLE_IDENTIFIER);
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    FileFormat.PARQUET.name(),
                    TableProperties.FORMAT_VERSION,
                    "3"));
    deletionVectorHelper = new DeletionVectorHelper(table);
    fileFactory = OutputFileFactory.builderFor(table, 1, 1L).format(FileFormat.PUFFIN).build();
    merger =
        new DvOnlyDeletionVectorMerger(
            table, SnapshotRef.MAIN_BRANCH, EQUALITY_FIELD_IDS, fileFactory);
  }

  /**
   * Writes a deletion vector for the positions of a file, merged with {@code previous} the way
   * {@link DvOnlyDeletionVectorWriterOperator} merges the vector a file carries.
   */
  private DeleteWriteResult writeVector(DataFile file, DeleteFile previous, long... positions) {
    FilePositions filePositions = FilePositions.forPartition(file.specId(), file.partition());
    for (long position : positions) {
      filePositions.add(position);
    }

    Map<String, DeleteFile> existing =
        previous != null ? ImmutableMap.of(file.location(), previous) : ImmutableMap.of();
    DeleteWriteResult result =
        deletionVectorHelper.write(
            fileFactory, ImmutableMap.of(file.location(), filePositions), existing);
    assertThat(result.deleteFiles()).hasSize(1);
    return result;
  }

  private static DeleteFile vector(DeleteWriteResult result) {
    return result.deleteFiles().get(0);
  }

  /** What {@link DvOnlyDeletionVectorWriterOperator} emits for a checkpoint. */
  private static WriteResult checkpoint(DeleteWriteResult... results) {
    WriteResult.Builder builder = WriteResult.builder();
    for (DeleteWriteResult result : results) {
      builder
          .addDeleteFiles(result.deleteFiles())
          .addRewrittenDeleteFiles(result.rewrittenDeleteFiles())
          .addReferencedDataFiles(result.referencedDataFiles());
    }

    return builder.build();
  }

  private void commit(DeleteFile vector, DeleteFile replaced) {
    RowDelta rowDelta = table.newRowDelta().addDeletes(vector);
    if (replaced != null) {
      rowDelta.removeDeletes(replaced);
    }

    rowDelta.commit();
  }

  /** Commits the merged vectors the way {@link IcebergCommitter} does. */
  private void commit(DvOnlyDeletionVectorMerger.Merged merged) {
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(merged.head().snapshotId())
            .validateDataFilesExist(merged.referencedDataFiles())
            .validateDeletedFiles();
    merged.deletionVectors().forEach(rowDelta::addDeletes);
    merged.replacedDeletionVectors().forEach(rowDelta::removeDeletes);
    rowDelta.commit();
  }

  private void rewrite(Set<DataFile> removed, DataFile added) {
    rewrite(removed, ImmutableSet.of(), added);
  }

  /**
   * Commits a compaction the way the rewrite action does: the deletes of the removed files are
   * applied to the replacement, so their deletion vectors are removed along with them.
   */
  private void rewrite(Set<DataFile> removed, Set<DeleteFile> removedDeletes, DataFile added) {
    RewriteFiles rewrite =
        table.newRewrite().validateFromSnapshot(table.currentSnapshot().snapshotId());
    removed.forEach(rewrite::deleteFile);
    removedDeletes.forEach(rewrite::deleteFile);
    rewrite.addFile(added).commit();
  }

  private void assertVector(DeleteFile vector, DataFile file, long... positions) {
    assertThat(vector.referencedDataFile()).isEqualTo(file.location());
    Set<Long> actual = Sets.newHashSet();
    deletionVectorHelper.load(vector).forEach(actual::add);
    Set<Long> expected = Sets.newHashSet();
    for (long position : positions) {
      expected.add(position);
    }

    assertThat(actual).isEqualTo(expected);
  }

  /**
   * Compares the id and data columns only, since reading a table with deletion vectors also
   * projects the row position.
   */
  private void assertRows(RowData... rows) throws IOException {
    List<String> expected = Lists.newArrayList();
    for (RowData row : rows) {
      expected.add(row.getInt(0) + ":" + row.getString(1));
    }

    List<String> actual = Lists.newArrayList();
    for (Record record : SimpleDataUtil.tableRecords(table)) {
      actual.add(record.getField("id") + ":" + record.getField("data"));
    }

    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
  }

  private DataFile appendFile(String name, RowData... rows) throws IOException {
    DataFile file = writeFile(name, rows);
    table.newAppend().appendFile(file).commit();
    return file;
  }

  private DataFile writeFile(String name, RowData... rows) throws IOException {
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        table.spec(),
        new Configuration(),
        table.location(),
        FileFormat.PARQUET.addExtension(name),
        Lists.newArrayList(rows));
  }

  /** Writes and appends a file to the partition {@code data=partitionValue}. */
  private DataFile appendFile(String name, String partitionValue, RowData... rows)
      throws IOException {
    DataFile file = writeFile(name, partitionValue, rows);
    table.newAppend().appendFile(file).commit();
    return file;
  }

  /** Writes a file to the partition {@code data=partitionValue}. */
  private DataFile writeFile(String name, String partitionValue, RowData... rows)
      throws IOException {
    PartitionKey partition = new PartitionKey(table.spec(), table.schema());
    partition.partition(SimpleDataUtil.createRecord(0, partitionValue));
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        table.spec(),
        new Configuration(),
        table.location(),
        FileFormat.PARQUET.addExtension(name),
        Lists.newArrayList(rows),
        partition);
  }

  private static RowData row(int id, String data) {
    return SimpleDataUtil.createInsert(id, data);
  }
}
