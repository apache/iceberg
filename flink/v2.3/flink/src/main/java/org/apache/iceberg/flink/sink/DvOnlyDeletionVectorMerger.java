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
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DvOnlyDeletionVectorMerger {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyDeletionVectorMerger.class);

  /** Data files whose vectors end up in the same Puffin file when they are written again. */
  private static final int WRITE_BATCH_SIZE = 1000;

  private final Table table;
  private final String branch;
  private final Set<Integer> equalityFieldIds;
  private final DeletionVectorHelper deletionVectorHelper;
  private final OutputFileFactory fileFactory;
  private final ExecutorService workerPool;

  DvOnlyDeletionVectorMerger(
      Table table,
      String branch,
      Set<Integer> equalityFieldIds,
      OutputFileFactory fileFactory,
      ExecutorService workerPool) {
    this.table = table;
    this.branch = branch;
    this.equalityFieldIds = equalityFieldIds;
    this.deletionVectorHelper = new DeletionVectorHelper(table);
    this.fileFactory = fileFactory;
    this.workerPool = workerPool;
  }

  /**
   * Computes the deletion vectors to commit for one checkpoint. The table has to be refreshed by
   * the caller, since the result is only valid against the state it was computed from.
   *
   * @param result the files of the checkpoint, as written by {@link
   *     DvOnlyDeletionVectorWriterOperator}
   * @param baselineSnapshotId snapshot the deletes were resolved against, or null when the branch
   *     had none; has to be an ancestor of the branch head
   */
  Merged merge(WriteResult result, Long baselineSnapshotId) {
    Snapshot head = table.snapshot(branch);
    Rewrites rewrites = rewritesSince(baselineSnapshotId, head);
    Preconditions.checkState(
        Arrays.stream(result.deleteFiles()).allMatch(ContentFileUtil::isDV),
        "Expected deletion vectors only, but found other delete files");
    Map<String, DeleteFile> vectors =
        Maps.uniqueIndex(Arrays.asList(result.deleteFiles()), DeleteFile::referencedDataFile);
    Map<String, DeleteFile> replaced =
        Maps.uniqueIndex(
            Arrays.asList(result.rewrittenDeleteFiles()), DeleteFile::referencedDataFile);

    Map<String, DeleteFile> attached =
        deletionVectorHelper.collectExistingDVs(
            head,
            Maps.transformValues(
                vectors, vector -> FilePositions.forPartition(vector.specId(), vector.partition())),
            workerPool);
    Set<String> affected =
        vectors.keySet().stream()
            .filter(
                path ->
                    rewrites.wasRemoved(path)
                        || !isSameVector(replaced.get(path), attached.get(path)))
            .collect(Collectors.toSet());

    Map<String, FilePositions> positionsByFile = Maps.newConcurrentMap();
    Tasks.foreach(affected)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool)
        .run(
            path ->
                positionsByFile.put(path, addedPositions(vectors.get(path), replaced.get(path))));

    Set<String> removedTargets =
        Sets.intersection(affected, rewrites.removedPaths()).immutableCopy();
    if (!removedTargets.isEmpty()) {
      resolveAgain(removedTargets, positionsByFile, rewrites);
    }

    attached.putAll(
        deletionVectorHelper.collectExistingDVs(
            head,
            Maps.filterKeys(positionsByFile, path -> !vectors.containsKey(path)),
            workerPool));

    Queue<DeleteWriteResult> written = new ConcurrentLinkedQueue<>();
    Tasks.foreach(Iterables.partition(positionsByFile.keySet(), WRITE_BATCH_SIZE))
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool)
        .run(
            batch ->
                written.add(
                    deletionVectorHelper.write(
                        fileFactory,
                        Maps.filterKeys(positionsByFile, Sets.newHashSet(batch)::contains),
                        attached)));

    return new Merged(head, result, vectors, replaced, affected, written);
  }

  private static boolean isSameVector(DeleteFile expected, DeleteFile actual) {
    if (expected == null || actual == null) {
      return expected == actual;
    }

    return expected.location().equals(actual.location())
        && Objects.equals(expected.contentOffset(), actual.contentOffset());
  }

  // The positions the checkpoint adds: the vector it wrote minus the one it replaces.
  private FilePositions addedPositions(DeleteFile vector, DeleteFile previous) {
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
    return file;
  }

  private Rewrites rewritesSince(Long baselineSnapshotId, Snapshot head) {
    Rewrites rewrites = new Rewrites();
    if (head == null) {
      return rewrites;
    }

    Preconditions.checkState(
        SinkUtil.isTraceable(table, head, baselineSnapshotId),
        "Snapshot %s the deletes were resolved against is no longer an ancestor of branch '%s' of "
            + "table %s",
        baselineSnapshotId,
        branch,
        table.name());

    for (Snapshot snapshot : SinkUtil.snapshotsSince(table, head, baselineSnapshotId)) {
      if (SinkUtil.summaryCount(snapshot, SnapshotSummary.DELETED_FILES_PROP) == 0) {
        continue;
      }

      SnapshotChanges changes =
          SnapshotChanges.builderFor(table).snapshot(snapshot).executeWith(workerPool).build();
      for (DataFile file : changes.removedDataFiles()) {
        rewrites.recordRemoval(file, snapshot.snapshotId());
      }

      rewrites.recordAdded(snapshot.snapshotId(), Lists.newArrayList(changes.addedDataFiles()));
    }

    return rewrites;
  }

  private void resolveAgain(
      Set<String> removedTargets, Map<String, FilePositions> positionsByFile, Rewrites rewrites) {
    Map<String, FilePositions> positionsByRemoved = Maps.newHashMap();
    for (String path : removedTargets) {
      positionsByRemoved.put(path, positionsByFile.remove(path));
    }

    Map<String, Set<SerializedEqualityValues>> keysByRemoved = Maps.newConcurrentMap();
    Tasks.foreach(removedTargets)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool)
        .run(
            path ->
                keysByRemoved.put(
                    path, collectKeys(rewrites.removedFile(path), positionsByRemoved.get(path))));

    Map<String, DataFile> replacements = Maps.newLinkedHashMap();
    Map<String, Set<SerializedEqualityValues>> keysByReplacement = Maps.newHashMap();
    long keyCount = 0;
    for (String path : removedTargets) {
      Set<SerializedEqualityValues> keys = keysByRemoved.get(path);
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

    Map<String, FilePositions> matchedByReplacement = Maps.newConcurrentMap();
    Tasks.foreach(replacements.values())
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool)
        .run(
            replacement -> {
              Set<SerializedEqualityValues> keys = keysByReplacement.get(replacement.location());
              FilePositions file =
                  FilePositions.forPartition(replacement.specId(), replacement.partition());
              newReader()
                  .read(
                      new PkIndexReadTask(replacement, ImmutableList.of()),
                      entry -> {
                        if (keys.contains(entry.key())) {
                          file.add(entry.position().position());
                        }
                      });

              if (!file.positions().isEmpty()) {
                matchedByReplacement.put(replacement.location(), file);
              }
            });

    long resolved = 0;
    for (Map.Entry<String, FilePositions> matched : matchedByReplacement.entrySet()) {
      resolved += matched.getValue().positions().getLongCardinality();
      positionsByFile.merge(
          matched.getKey(),
          matched.getValue(),
          (current, added) -> {
            current.positions().or(added.positions());
            return current;
          });
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

  private PkIndexFileReader newReader() {
    // The reader serializes keys through a shared buffer, so each task needs one of its own.
    return new PkIndexFileReader(table, equalityFieldIds);
  }

  private Set<SerializedEqualityValues> collectKeys(DataFile removedFile, FilePositions positions) {
    Set<SerializedEqualityValues> keys = Sets.newHashSet();
    try {
      newReader()
          .read(
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

    return keys;
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

  /**
   * The deletion vectors to commit for one checkpoint and the snapshot they were computed from.
   * Vectors of the checkpoint that were still valid are committed as they were written; the others
   * were merged again and are listed in {@link #writtenFiles()}.
   */
  static class Merged {
    private final Snapshot head;
    private final List<DeleteFile> deletionVectors;
    private final List<DeleteFile> replacedDeletionVectors;
    private final List<DeleteFile> newDeletionVectors;
    private final Set<String> writtenFiles;
    private final Set<String> obsoleteFiles;

    private Merged(
        Snapshot head,
        WriteResult checkpoint,
        Map<String, DeleteFile> vectors,
        Map<String, DeleteFile> replaced,
        Set<String> affected,
        Collection<DeleteWriteResult> written) {
      Map<String, DeleteFile> reused = Maps.filterKeys(vectors, path -> !affected.contains(path));
      this.head = head;
      this.newDeletionVectors =
          written.stream()
              .flatMap(result -> result.deleteFiles().stream())
              .collect(Collectors.toList());
      this.deletionVectors = Lists.newArrayList(reused.values());
      this.deletionVectors.addAll(newDeletionVectors);
      this.replacedDeletionVectors =
          reused.keySet().stream()
              .map(replaced::get)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      written.forEach(result -> replacedDeletionVectors.addAll(result.rewrittenDeleteFiles()));
      this.writtenFiles =
          newDeletionVectors.stream().map(DeleteFile::location).collect(Collectors.toSet());

      Set<String> keptFiles =
          reused.values().stream().map(DeleteFile::location).collect(Collectors.toSet());
      this.obsoleteFiles =
          Arrays.stream(checkpoint.deleteFiles())
              .map(DeleteFile::location)
              .filter(location -> !keptFiles.contains(location))
              .collect(Collectors.toSet());
    }

    Snapshot head() {
      return head;
    }

    List<DeleteFile> deletionVectors() {
      return deletionVectors;
    }

    List<DeleteFile> replacedDeletionVectors() {
      return replacedDeletionVectors;
    }

    List<String> referencedDataFiles() {
      return Lists.transform(deletionVectors, DeleteFile::referencedDataFile);
    }

    Set<String> writtenFiles() {
      return writtenFiles;
    }

    Set<String> obsoleteFiles() {
      return obsoleteFiles;
    }
  }
}
