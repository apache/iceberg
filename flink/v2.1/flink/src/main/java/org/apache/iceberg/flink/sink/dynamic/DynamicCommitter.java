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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.sink.CommitSummary;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Flink SinkV2 {@link Committer} interface to implement the Iceberg
 * commits. The implementation builds on the following assumptions:
 *
 * <ul>
 *   <li>There is a single {@link DynamicCommittable} for every table / branch / checkpoint
 *   <li>There is no late checkpoint - if checkpoint 'x' has received in one call, then after a
 *       successful run only checkpoints &gt; x will arrive
 *   <li>There is no other writer which would generate another commit to the same branch with the
 *       same jobId-operatorId-checkpointId triplet
 * </ul>
 */
@Internal
class DynamicCommitter implements Committer<DynamicCommittable> {

  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  private static final Logger LOG = LoggerFactory.getLogger(DynamicCommitter.class);

  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final String FLINK_JOB_ID = "flink.job-id";
  private static final String OPERATOR_ID = "flink.operator-id";
  private final Map<String, String> snapshotProperties;
  private final boolean replacePartitions;
  private final DynamicCommitterMetrics committerMetrics;
  private final Catalog catalog;
  private final ExecutorService workerPool;

  DynamicCommitter(
      Catalog catalog,
      Map<String, String> snapshotProperties,
      boolean replacePartitions,
      int workerPoolSize,
      String sinkId,
      DynamicCommitterMetrics committerMetrics) {
    this.snapshotProperties = snapshotProperties;
    this.replacePartitions = replacePartitions;
    this.committerMetrics = committerMetrics;
    this.catalog = catalog;
    this.workerPool =
        ThreadPools.newFixedThreadPool("iceberg-committer-pool-" + sinkId, workerPoolSize);
  }

  @Override
  public void commit(Collection<CommitRequest<DynamicCommittable>> commitRequests)
      throws IOException, InterruptedException {
    if (commitRequests.isEmpty()) {
      return;
    }

    /*
      Each (table, branch, checkpoint) triplet must have only one commit request.
      There may be commit requests from previous checkpoints which have not been committed yet.

      We currently keep a List of commit requests per checkpoint instead of a single CommitRequest<DynamicCommittable>
      to process the Flink state from previous releases, which had multiple commit requests created by the upstream
      DynamicWriteResultAggregator. Iceberg 1.12 will remove this, and users should upgrade to the 1.11 release first
      to migrate their state to a single commit request per checkpoint.
    */
    Map<TableKey, NavigableMap<Long, List<CommitRequest<DynamicCommittable>>>> commitRequestMap =
        Maps.newHashMap();
    for (CommitRequest<DynamicCommittable> request : commitRequests) {
      NavigableMap<Long, List<CommitRequest<DynamicCommittable>>> committables =
          commitRequestMap.computeIfAbsent(
              new TableKey(request.getCommittable()), unused -> Maps.newTreeMap());
      committables
          .computeIfAbsent(request.getCommittable().checkpointId(), unused -> Lists.newArrayList())
          .add(request);
    }

    for (Map.Entry<TableKey, NavigableMap<Long, List<CommitRequest<DynamicCommittable>>>> entry :
        commitRequestMap.entrySet()) {
      Table table = catalog.loadTable(TableIdentifier.parse(entry.getKey().tableName()));
      DynamicCommittable last = entry.getValue().lastEntry().getValue().get(0).getCommittable();
      Snapshot latestSnapshot = table.snapshot(entry.getKey().branch());
      Iterable<Snapshot> ancestors =
          latestSnapshot != null
              ? SnapshotUtil.ancestorsOf(latestSnapshot.snapshotId(), table::snapshot)
              : List.of();
      long maxCommittedCheckpointId =
          getMaxCommittedCheckpointId(ancestors, last.jobId(), last.operatorId());

      NavigableMap<Long, List<CommitRequest<DynamicCommittable>>> skippedCommitRequests =
          entry.getValue().headMap(maxCommittedCheckpointId, true);
      LOG.debug(
          "Skipping {} commit requests: {}", skippedCommitRequests.size(), skippedCommitRequests);
      // Mark the already committed FilesCommittable(s) as finished
      skippedCommitRequests
          .values()
          .forEach(list -> list.forEach(CommitRequest::signalAlreadyCommitted));

      NavigableMap<Long, List<CommitRequest<DynamicCommittable>>> uncommitted =
          entry.getValue().tailMap(maxCommittedCheckpointId, false);
      if (!uncommitted.isEmpty()) {
        commitPendingRequests(
            table, entry.getKey().branch(), uncommitted, last.jobId(), last.operatorId());
      }
    }
  }

  private static long getMaxCommittedCheckpointId(
      Iterable<Snapshot> ancestors, String flinkJobId, String operatorId) {
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    for (Snapshot ancestor : ancestors) {
      Map<String, String> summary = ancestor.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      String snapshotOperatorId = summary.get(OPERATOR_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)
          && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
        String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          lastCommittedCheckpointId = Long.parseLong(value);
          break;
        }
      }
    }

    return lastCommittedCheckpointId;
  }

  /**
   * Commits the data to the Iceberg table by reading the file data from the {@link DeltaManifests}
   * ordered by the checkpointId, and writing the new snapshot to the Iceberg table. The {@link
   * SnapshotSummary} will contain the jobId, snapshotId, checkpointId so in case of job restart we
   * can identify which changes are committed, and which are still waiting for the commit.
   *
   * @param commitRequestMap The checkpointId to {@link CommitRequest} map of the changes to commit
   * @param newFlinkJobId The jobId to store in the {@link SnapshotSummary}
   * @param operatorId The operatorId to store in the {@link SnapshotSummary}
   * @throws IOException On commit failure
   */
  private void commitPendingRequests(
      Table table,
      String branch,
      NavigableMap<Long, List<CommitRequest<DynamicCommittable>>> commitRequestMap,
      String newFlinkJobId,
      String operatorId)
      throws IOException {
    long checkpointId = commitRequestMap.lastKey();
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, List<WriteResult>> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, List<CommitRequest<DynamicCommittable>>> e : commitRequestMap.entrySet()) {
      for (CommitRequest<DynamicCommittable> committable : e.getValue()) {
        for (byte[] manifest : committable.getCommittable().manifests()) {
          DeltaManifests deltaManifests =
              SimpleVersionedSerialization.readVersionAndDeSerialize(
                  DeltaManifestsSerializer.INSTANCE, manifest);
          pendingResults
              .computeIfAbsent(e.getKey(), unused -> Lists.newArrayList())
              .add(FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
          manifests.addAll(deltaManifests.manifests());
        }
      }
    }

    if (TableUtil.formatVersion(table) > 2) {
      Optional<DeleteFile> positionalDelete =
          pendingResults.values().stream()
              .flatMap(List::stream)
              .flatMap(writeResult -> Arrays.stream(writeResult.deleteFiles()))
              .filter(deleteFile -> deleteFile.content() == FileContent.POSITION_DELETES)
              .filter(Predicate.not(ContentFileUtil::isDV))
              .findAny();
      Preconditions.checkArgument(
          positionalDelete.isEmpty(),
          "Can't add position delete file to the %s table. Concurrent table upgrade to V3 is not supported.",
          table.name());
    }

    if (replacePartitions) {
      replacePartitions(table, branch, pendingResults, newFlinkJobId, operatorId);
    } else {
      commitDeltaTxn(table, branch, pendingResults, newFlinkJobId, operatorId);
    }

    FlinkManifestUtil.deleteCommittedManifests(table, manifests, newFlinkJobId, checkpointId);
  }

  private void replacePartitions(
      Table table,
      String branch,
      NavigableMap<Long, List<WriteResult>> pendingResults,
      String newFlinkJobId,
      String operatorId) {
    // Iceberg tables are unsorted. So the order of the append data does not matter.
    // Hence, we commit everything in one snapshot.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);

    for (List<WriteResult> writeResults : pendingResults.values()) {
      for (WriteResult result : writeResults) {
        Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
      }
    }

    CommitSummary summary = new CommitSummary();
    summary.addAll(pendingResults);

    commitOperation(
        table,
        branch,
        dynamicOverwrite,
        summary,
        "dynamic partition overwrite",
        newFlinkJobId,
        operatorId,
        pendingResults.lastKey());
  }

  private void commitDeltaTxn(
      Table table,
      String branch,
      NavigableMap<Long, List<WriteResult>> pendingResults,
      String newFlinkJobId,
      String operatorId) {
    for (Map.Entry<Long, List<WriteResult>> e : pendingResults.entrySet()) {
      long checkpointId = e.getKey();
      List<WriteResult> writeResults = e.getValue();

      RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
      for (WriteResult result : writeResults) {
        // Row delta validations are not needed for streaming changes that write equality deletes.
        // Equality deletes are applied to data in all previous sequence numbers, so retries may
        // push deletes further in the future, but do not affect correctness. Position deletes
        // committed to the table in this path are used only to delete rows from data files that are
        // being added in this commit. There is no way for data files added along with the delete
        // files to be concurrently removed, so there is no need to validate the files referenced by
        // the position delete files that are being committed.
        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
      }

      CommitSummary summary = new CommitSummary();
      summary.addAll(writeResults);

      // Every Flink checkpoint contains a set of independent changes which can be committed
      // together. While it is technically feasible to combine append-only data across checkpoints,
      // for the sake of simplicity, we do not implement this (premature) optimization. Multiple
      // pending checkpoints here are very rare to occur, i.e. only with very short checkpoint
      // intervals or when concurrent checkpointing is enabled.
      commitOperation(
          table, branch, rowDelta, summary, "rowDelta", newFlinkJobId, operatorId, checkpointId);
    }
  }

  private static class MaxCommittedCheckpointMismatchException extends ValidationException {
    private MaxCommittedCheckpointMismatchException() {
      super("Table already contains staged changes.");
    }
  }

  private static class MaxCommittedCheckpointIdValidator implements SnapshotAncestryValidator {
    private final long stagedCheckpointId;
    private final String flinkJobId;
    private final String flinkOperatorId;

    private MaxCommittedCheckpointIdValidator(
        long stagedCheckpointId, String flinkJobId, String flinkOperatorId) {
      this.stagedCheckpointId = stagedCheckpointId;
      this.flinkJobId = flinkJobId;
      this.flinkOperatorId = flinkOperatorId;
    }

    @Override
    public boolean validate(Iterable<Snapshot> baseSnapshots) {
      long maxCommittedCheckpointId =
          getMaxCommittedCheckpointId(baseSnapshots, flinkJobId, flinkOperatorId);
      if (maxCommittedCheckpointId >= stagedCheckpointId) {
        throw new MaxCommittedCheckpointMismatchException();
      }

      return true;
    }
  }

  @VisibleForTesting
  void commitOperation(
      Table table,
      String branch,
      SnapshotUpdate<?> operation,
      CommitSummary summary,
      String description,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {

    LOG.info(
        "Committing {} for checkpoint {} to table {} branch {} with summary: {}",
        description,
        checkpointId,
        table.name(),
        branch,
        summary);
    snapshotProperties.forEach(operation::set);
    // custom snapshot metadata properties will be overridden if they conflict with internal ones
    // used by the sink.
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);
    operation.set(OPERATOR_ID, operatorId);
    operation.toBranch(branch);
    operation.validateWith(
        new MaxCommittedCheckpointIdValidator(checkpointId, newFlinkJobId, operatorId));

    long startNano = System.nanoTime();
    try {
      operation.commit(); // abort is automatically called if this fails.
    } catch (MaxCommittedCheckpointMismatchException e) {
      LOG.info(
          "Skipping commit operation {} because the {} branch of the {} table already contains changes for checkpoint {}."
              + " This can occur when a failure prevents the committer from receiving confirmation of a"
              + " successful commit, causing the Flink job to retry committing the same set of changes.",
          description,
          branch,
          table.name(),
          checkpointId,
          e);
      return;
    }

    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    LOG.info(
        "Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
        description,
        table.name(),
        branch,
        checkpointId,
        durationMs);
    if (committerMetrics != null) {
      committerMetrics.commitDuration(table.name(), durationMs);
      committerMetrics.updateCommitSummary(table.name(), summary);
    }
  }

  @Override
  public void close() throws IOException {
    workerPool.shutdown();
  }
}
