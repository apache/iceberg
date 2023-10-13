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
import javax.annotation.Nullable;
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
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the common commit related tasks which are needed for both the {@link
 * org.apache.iceberg.flink.sink.FlinkSink} (SinkV1) and the {@link
 * org.apache.iceberg.flink.sink.IcebergSink} (SinkV2).
 *
 * <p>The object is serialized and sent to the {@link IcebergFilesCommitter} (SinkV1) and to the
 * {@link SinkV2Aggregator}, {@link SinkV2Committer} (SinkV2) to execute the related tasks. Before
 * calling any of the public methods, the {@link #init(IcebergFilesCommitterMetrics)} method should
 * be called, so the non-serializable attributes are initialized. The {@link
 * #init(IcebergFilesCommitterMetrics)} is idempotent, so it could be called multiple times to
 * accommodate the missing init feature in the {@link Committer}.
 */
public class CommonCommitter implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CommonCommitter.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];
  public static final long INITIAL_CHECKPOINT_ID = -1L;
  public static final String FLINK_JOB_ID = "flink.job-id";
  public static final String OPERATOR_ID = "flink.operator-id";
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";

  @VisibleForTesting
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  private final TableLoader tableLoader;
  private final String branch;
  private final Map<String, String> snapshotProperties;
  private final boolean replacePartitions;
  private final int workerPoolSize;
  private final String prefix;
  private transient IcebergFilesCommitterMetrics committerMetrics;
  private transient Table table;
  private transient int maxContinuousEmptyCommits;
  private transient ExecutorService workerPool;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient int continuousEmptyCheckpoints = 0;
  private transient boolean initialized = false;

  /**
   * Creates the object with using only serializable parameters.
   *
   * @param tableLoader The tableSupplier which will be used to access the table
   * @param branch The branch name which the committer commits to
   * @param snapshotProperties The extra properties to put into the new snapshot
   * @param replacePartitions Whether to overwrite existing partition if new data arrives, or add
   *     the new data only
   * @param workerPoolSize The pool size for the worker thread pool for reading manifest files
   * @param prefix Unique id for the committer. Used for creating unique name for manifests, and the
   *     worker thread pool.
   */
  public CommonCommitter(
      TableLoader tableLoader,
      String branch,
      Map<String, String> snapshotProperties,
      boolean replacePartitions,
      int workerPoolSize,
      String prefix) {
    this.tableLoader = tableLoader;
    this.branch = branch;
    this.snapshotProperties = snapshotProperties;
    this.replacePartitions = replacePartitions;
    this.workerPoolSize = workerPoolSize;
    this.prefix = prefix;
  }

  /**
   * Initializes the committer. Idempotent, as it might be called multiple times. For example, from
   * the {@link SinkV2Committer}. Loads the table, initializes the {@link ManifestOutputFileFactory}
   * and the manifest reader worker pool.
   *
   * @param newCommitterMetrics The metrics used to store the commit related metrics
   */
  public void init(@Nullable IcebergFilesCommitterMetrics newCommitterMetrics) {
    this.committerMetrics = newCommitterMetrics;

    if (initialized) {
      // If already initialized, then we do not need to do it again
      return;
    }

    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }
    this.table = tableLoader.loadTable();
    this.manifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(() -> table, table.properties(), prefix);
    this.maxContinuousEmptyCommits =
        PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(
        maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
    this.workerPool =
        ThreadPools.newWorkerPool("iceberg-worker-pool-" + table + "-" + prefix, workerPoolSize);
    this.continuousEmptyCheckpoints = 0;
    this.initialized = true;
  }

  /**
   * Write all the completed data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  public byte[] writeToManifest(Collection<WriteResult> writeResults, long checkpointId)
      throws IOException {
    if (writeResults.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResults).build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result, () -> manifestOutputFileFactory.create(checkpointId), table.spec());

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  /**
   * Gets the last checkpointId which is committed to this branch of the table. The commits are
   * identified by the {@link SinkV2Committable} (jobId/operatorId/checkpointId). Only used by
   * {@link SinkV2Committer} (SinkV2). There is a similar method for {@link IcebergFilesCommitter}
   * (SinkV1). see: {@link #getMaxCommittedCheckpointId(String, String)}.
   *
   * @param requests The {@link SinkV2Committable}s ordered by checkpointId
   * @return The checkpointId for the first commit (historically backward)
   */
  public long getMaxCommittedCheckpointId(
      NavigableMap<Long, Committer.CommitRequest<SinkV2Committable>> requests) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      String snapshotOperatorId = summary.get(OPERATOR_ID);
      String snapshotCheckpointId = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
      if (snapshotCheckpointId != null) {
        long checkpointId = Long.parseLong(snapshotCheckpointId);
        Committer.CommitRequest<SinkV2Committable> request = requests.get(checkpointId);
        if (request != null
            && request.getCommittable().jobId().equals(snapshotFlinkJobId)
            && request.getCommittable().operatorId().equals(snapshotOperatorId)) {
          lastCommittedCheckpointId = checkpointId;
          break;
        }
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
  }

  /**
   * Gets the last checkpointId which is committed to this branch of the table by this job/operator.
   * Only used by {@link IcebergFilesCommitter} (SinkV1). There is a similar method for {@link
   * SinkV2Committer} (SinkV2), see: {@link #getMaxCommittedCheckpointId(NavigableMap)}.
   *
   * @param flinkJobId The jobs jobId (or at recovery time the previous jobId)
   * @param operatorId The committer operatorId (this is the same on recovery)
   * @return The checkpointId for the first commit (historically backward) with the given jobId and
   *     operatorId
   */
  public long getMaxCommittedCheckpointId(String flinkJobId, String operatorId) {
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
   * @param deltaManifestsMap The checkpointId to {@link DeltaManifests} map of the changes to
   *     commit
   * @param newFlinkJobId The jobId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @param operatorId The operatorId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @param checkpointId The checkpointId to store in the {@link org.apache.iceberg.SnapshotSummary}
   * @throws IOException On commit failure
   */
  public void commitUpToCheckpoint(
      NavigableMap<Long, byte[]> deltaManifestsMap,
      String newFlinkJobId,
      String operatorId,
      long checkpointId)
      throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

      DeltaManifests deltaManifests =
          SimpleVersionedSerialization.readVersionAndDeSerialize(
              DeltaManifestsSerializer.INSTANCE, e.getValue());
      pendingResults.put(
          e.getKey(),
          FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
      manifests.addAll(deltaManifests.manifests());
    }

    CommitSummary summary = new CommitSummary(pendingResults);
    commitPendingResult(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
    if (committerMetrics != null) {
      committerMetrics.updateCommitSummary(summary);
    }
    pendingMap.clear();
    deleteCommittedManifests(manifests, newFlinkJobId, checkpointId);
  }

  private void commitPendingResult(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
    long totalFiles = summary.dataFilesCount() + summary.deleteFilesCount();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
      } else {
        commitDeltaTxn(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
      }
      continuousEmptyCheckpoints = 0;
    } else {
      LOG.info("Skip commit for checkpoint {} due to no data files or delete files.", checkpointId);
    }
  }

  private void replacePartitions(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId) {
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
      String operatorId,
      long checkpointId) {
    if (summary.deleteFilesCount() == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(
            result.referencedDataFiles().length == 0,
            "Should have no referenced data files for append.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }
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

  private void deleteCommittedManifests(
      List<ManifestFile> manifests, String newFlinkJobId, long checkpointId) {
    for (ManifestFile manifest : manifests) {
      try {
        table.io().deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details =
            MoreObjects.toStringHelper(this)
                .add("flinkJobId", newFlinkJobId)
                .add("checkpointId", checkpointId)
                .add("manifestPath", manifest.path())
                .toString();
        LOG.warn(
            "The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details,
            e);
      }
    }
  }
}
