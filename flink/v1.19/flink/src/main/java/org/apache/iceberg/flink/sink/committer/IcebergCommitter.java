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
package org.apache.iceberg.flink.sink.committer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.CommitSummary;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.IcebergFilesCommitterMetrics;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Flink SinkV2 {@link Committer} interface to implement the Iceberg
 * commits. The implementation builds on the following assumptions:
 *
 * <ul>
 *   <li>There is a single {@link IcebergCommittable} for every checkpoint
 *   <li>There is no late checkpoint - if checkpoint 'x' has received in one call, then after a
 *       successful run only checkpoints &gt; x will arrive
 *   <li>There is no other writer which would generate another commit to the same branch with the
 *       same jobId-operatorId-checkpointId triplet
 * </ul>
 */
@Internal
public class IcebergCommitter implements Committer<IcebergCommittable>, Serializable {
  private static final long serialVersionUID = 1L;
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitter.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];
  public static final WriteResult EMPTY_WRITE_RESULT =
      WriteResult.builder()
          .addDataFiles(Lists.newArrayList())
          .addDeleteFiles(Lists.newArrayList())
          .build();

  private static final long INITIAL_CHECKPOINT_ID = -1L;

  @VisibleForTesting
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  public static final String FLINK_JOB_ID = "flink.job-id";
  public static final String OPERATOR_ID = "flink.operator-id";
  private final String branch;
  private final Map<String, String> snapshotProperties;
  private final boolean replacePartitions;
  private transient IcebergFilesCommitterMetrics committerMetrics;
  private transient Table table;
  private final TableLoader tableLoader;
  private transient int maxContinuousEmptyCommits;
  private transient ExecutorService workerPool;
  private transient int continuousEmptyCheckpoints = 0;

  public IcebergCommitter(
      TableLoader tableLoader,
      String branch,
      Map<String, String> snapshotProperties,
      boolean replacePartitions,
      int workerPoolSize,
      String sinkId,
      IcebergFilesCommitterMetrics committerMetrics) {
    this.branch = branch;
    this.snapshotProperties = snapshotProperties;
    this.replacePartitions = replacePartitions;
    this.committerMetrics = committerMetrics;
    this.tableLoader = tableLoader;
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.maxContinuousEmptyCommits =
        PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(
        maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
    this.workerPool =
        ThreadPools.newWorkerPool(
            "iceberg-committer-pool-" + table.name() + "-" + sinkId, workerPoolSize);
    this.continuousEmptyCheckpoints = 0;
  }

  @Override
  public void commit(Collection<CommitRequest<IcebergCommittable>> commitRequests)
      throws IOException, InterruptedException {
    if (commitRequests.isEmpty()) {
      return;
    }

    NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap = Maps.newTreeMap();
    for (CommitRequest<IcebergCommittable> request : commitRequests) {
      commitRequestMap.put(request.getCommittable().checkpointId(), request);
    }

    IcebergCommittable last = commitRequestMap.lastEntry().getValue().getCommittable();
    long maxCommittedCheckpointId =
        getMaxCommittedCheckpointId(table, last.jobId(), last.operatorId(), branch);
    // Mark the already committed FilesCommittable(s) as finished
    commitRequestMap
        .headMap(maxCommittedCheckpointId, true)
        .values()
        .forEach(CommitRequest::signalAlreadyCommitted);
    NavigableMap<Long, CommitRequest<IcebergCommittable>> uncommitted =
        commitRequestMap.tailMap(maxCommittedCheckpointId, false);
    if (!uncommitted.isEmpty()) {
      commitPendingRequests(uncommitted, last.jobId(), last.operatorId());
    }
  }

  public static long getMaxCommittedCheckpointId(
      Table table, String flinkJobId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
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
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
  }

  /**
   * Commits the data to the Iceberg table by reading the file data from the {@link DeltaManifests}
   * ordered by the checkpointId, and writing the new snapshot to the Iceberg table. The {@link
   * org.apache.iceberg.SnapshotSummary} will contain the jobId, snapshotId, checkpointId so in case
   * of job restart we can identify which changes are committed, and which are still waiting for the
   * commit.
   *
   * @param commitRequestMap The checkpointId to {@link CommitRequest} map of the changes to commit
   * @param newFlinkJobId The jobId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @param operatorId The operatorId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @throws IOException On commit failure
   */
  private void commitPendingRequests(
      NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap,
      String newFlinkJobId,
      String operatorId)
      throws IOException {
    long checkpointId = commitRequestMap.lastKey();
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, CommitRequest<IcebergCommittable>> e : commitRequestMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue().getCommittable().manifest())) {
        pendingResults.put(e.getKey(), EMPTY_WRITE_RESULT);
      } else {
        DeltaManifests deltaManifests =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                DeltaManifestsSerializer.INSTANCE, e.getValue().getCommittable().manifest());
        pendingResults.put(
            e.getKey(),
            FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
        manifests.addAll(deltaManifests.manifests());
      }
    }

    CommitSummary summary = new CommitSummary(pendingResults);
    commitPendingResult(pendingResults, summary, newFlinkJobId, operatorId);
    if (committerMetrics != null) {
      committerMetrics.updateCommitSummary(summary);
    }

    FlinkManifestUtil.deleteCommittedManifests(table, manifests, newFlinkJobId, checkpointId);
  }

  private void commitPendingResult(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId) {
    long totalFiles = summary.dataFilesCount() + summary.deleteFilesCount();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(pendingResults, summary, newFlinkJobId, operatorId);
      } else {
        commitDeltaTxn(pendingResults, summary, newFlinkJobId, operatorId);
      }
      continuousEmptyCheckpoints = 0;
    } else {
      long checkpointId = pendingResults.lastKey();
      LOG.info("Skip commit for checkpoint {} due to no data files or delete files.", checkpointId);
    }
  }

  private void replacePartitions(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId) {
    long checkpointId = pendingResults.lastKey();
    Preconditions.checkState(
        summary.deleteFilesCount() == 0, "Cannot overwrite partitions with delete files.");
    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(
          result.referencedDataFiles().length == 0, "Should have no referenced data files.");
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(
        dynamicOverwrite,
        summary,
        "dynamic partition overwrite",
        newFlinkJobId,
        operatorId,
        checkpointId);
  }

  private void commitDeltaTxn(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId) {
    long checkpointId = pendingResults.lastKey();
    if (summary.deleteFilesCount() == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(
            result.referencedDataFiles().length == 0,
            "Should have no referenced data files for append.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }
      // fail all commits as really its only one
      commitOperation(appendFiles, summary, "append", newFlinkJobId, operatorId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential
        // transaction txn1 and txn2, the equality-delete files of txn2 are required to be applied
        // to data files from txn1. Committing the merged one will lead to the incorrect delete
        // semantic.
        WriteResult result = e.getValue();

        // Row delta validations are not needed for streaming changes that write equality deletes.
        // Equality deletes are applied to data in all previous sequence numbers, so retries may
        // push deletes further in the future, but do not affect correctness. Position deletes
        // committed to the table in this path are used only to delete rows from data files that are
        // being added in this commit. There is no way for data files added along with the delete
        // files to be concurrently removed, so there is no need to validate the files referenced by
        // the position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);

        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
        commitOperation(rowDelta, summary, "rowDelta", newFlinkJobId, operatorId, e.getKey());
      }
    }
  }

  private void commitOperation(
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

    long startNano = System.nanoTime();
    operation.commit(); // abort is automatically called if this fails.
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    LOG.info(
        "Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
        description,
        table.name(),
        branch,
        checkpointId,
        durationMs);
    if (committerMetrics != null) {
      committerMetrics.commitDuration(durationMs);
    }
  }

  @Override
  public void close() throws IOException {
    tableLoader.close();
  }
}
