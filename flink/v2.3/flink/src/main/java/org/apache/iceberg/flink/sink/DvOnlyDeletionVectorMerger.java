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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper.FilePositions;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DvOnlyDeletionVectorMerger {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyDeletionVectorMerger.class);

  private final Table table;
  private final String branch;
  private final PkIndexFileReader reader;
  private final DeletionVectorHelper deletionVectorHelper;
  private final OutputFileFactory fileFactory;

  DvOnlyDeletionVectorMerger(
      Table table, String branch, Set<Integer> equalityFieldIds, OutputFileFactory fileFactory) {
    this.table = table;
    this.branch = branch;
    this.reader = new PkIndexFileReader(table, equalityFieldIds);
    this.deletionVectorHelper = new DeletionVectorHelper(table);
    this.fileFactory = fileFactory;
  }

  boolean isTraceable(Long baselineSnapshotId) {
    return SinkUtil.isTraceable(table, table.snapshot(branch), baselineSnapshotId);
  }

  /**
   * Computes the deletion vectors to commit for one checkpoint. The table has to be refreshed by
   * the caller, since the result is only valid against the state it was computed from.
   *
   * @param result the files of the checkpoint, as written by {@link
   *     DvOnlyDeletionVectorWriterOperator}
   * @param baselineSnapshotId snapshot the deletes were resolved against, or null when the branch
   *     had none; has to be {@link #isTraceable traceable}
   */
  Merged merge(WriteResult result, Long baselineSnapshotId) {
    Snapshot head = table.snapshot(branch);
    Map<String, FilePositions> positionsByFile = addedPositions(result);
    Rewrites rewrites = rewritesSince(baselineSnapshotId, head);

    Set<String> removedTargets = Sets.newHashSet(positionsByFile.keySet());
    removedTargets.retainAll(rewrites.removedPaths());
    if (!removedTargets.isEmpty()) {
      resolveAgain(removedTargets, positionsByFile, rewrites);
    }

    Map<String, DeleteFile> attachedVectors =
        deletionVectorHelper.collectExistingDVs(head, positionsByFile);
    return new Merged(
        head, deletionVectorHelper.write(fileFactory, positionsByFile, attachedVectors));
  }

  // The positions the checkpoint adds: the vector it wrote minus the one it replaces.
  private Map<String, FilePositions> addedPositions(WriteResult result) {
    Map<String, DeleteFile> replaced = Maps.newHashMap();
    for (DeleteFile deleteFile : result.rewrittenDeleteFiles()) {
      replaced.put(deleteFile.referencedDataFile(), deleteFile);
    }

    Map<String, FilePositions> positionsByFile = Maps.newLinkedHashMap();
    for (DeleteFile vector : result.deleteFiles()) {
      Preconditions.checkState(
          ContentFileUtil.isDV(vector),
          "Expected a deletion vector, but found delete file %s",
          vector.location());
      String dataFilePath = vector.referencedDataFile();
      Preconditions.checkState(
          !positionsByFile.containsKey(dataFilePath),
          "Found more than one deletion vector for data file %s",
          dataFilePath);
      DeleteFile previous = replaced.get(dataFilePath);
      PositionDeleteIndex previousPositions =
          previous != null ? deletionVectorHelper.load(previous) : null;
      FilePositions file = FilePositions.forPartition(vector.specId(), vector.partition());
      deletionVectorHelper
          .load(vector)
          .forEach(
              position -> {
                if (previousPositions == null || !previousPositions.isDeleted(position)) {
                  file.add(position);
                }
              });
      positionsByFile.put(dataFilePath, file);
    }

    return positionsByFile;
  }

  private Rewrites rewritesSince(Long baselineSnapshotId, Snapshot head) {
    Rewrites rewrites = new Rewrites();
    if (head == null) {
      return rewrites;
    }

    Preconditions.checkState(
        isTraceable(baselineSnapshotId),
        "Snapshot %s the deletes were resolved against is no longer an ancestor of branch '%s' of "
            + "table %s",
        baselineSnapshotId,
        branch,
        table.name());

    List<Snapshot> committedSince =
        Lists.reverse(
            Lists.newArrayList(
                SnapshotUtil.ancestorsBetween(table, head.snapshotId(), baselineSnapshotId)));
    for (Snapshot snapshot : committedSince) {
      if (PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.DELETED_FILES_PROP, 0)
          == 0) {
        continue;
      }

      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      for (DataFile file : changes.removedDataFiles()) {
        rewrites.recordRemoval(file, snapshot.snapshotId());
      }

      rewrites.recordAdded(snapshot.snapshotId(), Lists.newArrayList(changes.addedDataFiles()));
    }

    return rewrites;
  }

  private void resolveAgain(
      Set<String> removedTargets, Map<String, FilePositions> positionsByFile, Rewrites rewrites) {
    Map<String, DataFile> replacements = Maps.newLinkedHashMap();
    Map<String, Set<SerializedEqualityValues>> keysByReplacement = Maps.newHashMap();
    long keyCount = 0;
    for (String path : removedTargets) {
      Set<SerializedEqualityValues> keys = Sets.newHashSet();
      collectKeys(rewrites.removedFile(path), positionsByFile.remove(path), keys);
      keyCount += keys.size();
      if (keys.isEmpty()) {
        continue;
      }

      for (DataFile replacement : replacementsOf(rewrites.removedFile(path), rewrites)) {
        replacements.put(replacement.location(), replacement);
        keysByReplacement
            .computeIfAbsent(replacement.location(), location -> Sets.newHashSet())
            .addAll(keys);
      }
    }

    long resolved = 0;
    for (DataFile replacement : replacements.values()) {
      Set<SerializedEqualityValues> keys = keysByReplacement.get(replacement.location());
      FilePositions file =
          FilePositions.forPartition(replacement.specId(), replacement.partition());
      reader.read(
          new PkIndexReadTask(replacement, ImmutableList.of()),
          entry -> {
            if (keys.contains(entry.key())) {
              file.add(entry.position().position());
            }
          });

      if (!file.positions().isEmpty()) {
        resolved += file.positions().getLongCardinality();
        positionsByFile.merge(
            replacement.location(),
            file,
            (current, added) -> {
              current.positions().or(added.positions());
              return current;
            });
      }
    }

    LOG.info(
        "Resolved again the deletes of {} key(s) aimed at {} data file(s) that left branch '{}' of "
            + "table {}: read {} replacement(s), which removed {} row(s)",
        keyCount,
        removedTargets.size(),
        branch,
        table.name(),
        replacements.size(),
        resolved);
  }

  private List<DataFile> replacementsOf(DataFile removedFile, Rewrites rewrites) {
    List<DataFile> replacements = Lists.newArrayList();
    Set<String> visited = Sets.newHashSet(removedFile.location());
    Deque<DataFile> toTrace = new ArrayDeque<>();
    toTrace.add(removedFile);
    while (!toTrace.isEmpty()) {
      DataFile traced = toTrace.poll();
      for (DataFile added : rewrites.addedInRemovalOf(traced.location())) {
        if (!mayHoldRowsOf(added, traced) || !visited.add(added.location())) {
          continue;
        }

        if (rewrites.wasRemoved(added.location())) {
          toTrace.add(added);
        } else {
          replacements.add(added);
        }
      }
    }

    return replacements;
  }

  private boolean mayHoldRowsOf(DataFile added, DataFile removedFile) {
    if (added.specId() != removedFile.specId()) {
      return true;
    }

    StructLikeWrapper partition =
        StructLikeWrapper.forType(table.specs().get(added.specId()).partitionType());
    return partition.copyFor(added.partition()).equals(partition.copyFor(removedFile.partition()));
  }

  private void collectKeys(
      DataFile removedFile, FilePositions positions, Set<SerializedEqualityValues> keys) {
    try {
      reader.read(
          new PkIndexReadTask(removedFile, ImmutableList.of()),
          entry -> {
            if (positions.positions().contains(entry.position().position())) {
              keys.add(entry.key());
            }
          });
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          String.format(
              "Cannot resolve again the deletes aimed at data file %s, which left branch '%s' of "
                  + "table %s after the deletes were resolved: the file is no longer readable",
              removedFile.location(), branch, table.name()),
          e);
    }
  }

  private static class Rewrites {
    private final Map<String, DataFile> removed = Maps.newHashMap();
    private final Map<String, Long> removedBy = Maps.newHashMap();
    private final Map<Long, List<DataFile>> addedBy = Maps.newHashMap();

    void recordRemoval(DataFile file, long snapshotId) {
      removed.put(file.location(), file);
      removedBy.put(file.location(), snapshotId);
    }

    void recordAdded(long snapshotId, List<DataFile> files) {
      addedBy.put(snapshotId, files);
    }

    Set<String> removedPaths() {
      return removedBy.keySet();
    }

    boolean wasRemoved(String path) {
      return removedBy.containsKey(path);
    }

    DataFile removedFile(String path) {
      return removed.get(path);
    }

    List<DataFile> addedInRemovalOf(String path) {
      return addedBy.getOrDefault(removedBy.get(path), ImmutableList.of());
    }
  }

  /** The deletion vectors to commit for one checkpoint and the snapshot they were computed from. */
  static class Merged {
    private final Snapshot head;
    private final DeleteWriteResult result;

    private Merged(Snapshot head, DeleteWriteResult result) {
      this.head = head;
      this.result = result;
    }

    Snapshot head() {
      return head;
    }

    List<DeleteFile> deletionVectors() {
      return result.deleteFiles();
    }

    List<DeleteFile> replacedDeletionVectors() {
      return result.rewrittenDeleteFiles();
    }

    Iterable<CharSequence> referencedDataFiles() {
      return result.referencedDataFiles();
    }
  }
}
