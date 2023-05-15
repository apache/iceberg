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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class IcebergFilesCommitter<T extends WriteResult> extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<T, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";
  private static final String OPERATOR_ID = "flink.operator-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always
  // increasing, so we could correctly commit all the data files whose checkpoint id is greater than
  // the max committed one to iceberg table, for avoiding committing the same data files twice. This
  // id will be attached to iceberg's meta when committing the iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;
  private final Map<String, String> snapshotProperties;
  private final String branch;

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient String operatorUniqueId;
  private transient Table table;
  private transient IcebergFilesCommitterMetrics committerMetrics;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient long maxCommittedCheckpointId;
  private transient int continuousEmptyCheckpoints;
  private transient int maxContinuousEmptyCommits;
  // There're two cases that we restore from flink checkpoints: the first case is restoring from
  // snapshot created by the same flink job; another case is restoring from snapshot created by
  // another different job. For the second case, we need to maintain the old flink job's id in flink
  // state backend to find the max-committed-checkpoint-id when traversing iceberg table's
  // snapshots.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR =
      new ListStateDescriptor<>("iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;

  private final Integer workerPoolSize;
  private transient ExecutorService workerPool;

  IcebergFilesCommitter(
      TableLoader tableLoader,
      boolean replacePartitions,
      Map<String, String> snapshotProperties,
      Integer workerPoolSize,
      String branch) {
    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
    this.snapshotProperties = snapshotProperties;
    this.workerPoolSize = workerPoolSize;
    this.branch = branch;
  }

  protected Table table() {
    return table;
  }

  protected String branch() {
    return branch;
  }

  protected String flinkJobId() {
    return flinkJobId;
  }

  protected String operatorId() {
    return operatorUniqueId;
  }

  protected ManifestOutputFileFactory manifestOutputFileFactory() {
    return manifestOutputFileFactory;
  }

  protected IcebergFilesCommitterMetrics committerMetrics() {
    return committerMetrics;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorUniqueId = getRuntimeContext().getOperatorUniqueID();

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.committerMetrics = new IcebergFilesCommitterMetrics(super.metrics, table.name());

    maxContinuousEmptyCommits =
        PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(
        maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");

    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();
    this.manifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(
            table, flinkJobId, operatorUniqueId, subTaskId, attemptId);
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    initCheckpointState(context);

    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    if (context.isRestored()) {
      Iterable<String> jobIdIterable = jobIdState.get();
      if (jobIdIterable == null || !jobIdIterable.iterator().hasNext()) {
        LOG.warn(
            "Failed to restore committer state. This can happen when operator uid changed and Flink "
                + "allowNonRestoredState is enabled. Best practice is to explicitly set the operator id "
                + "via FlinkSink#Builder#uidPrefix() so that the committer operator uid is stable. "
                + "Otherwise, Flink auto generate an operator uid based on job topology."
                + "With that, operator uid is subjective to change upon topology change.");
        return;
      }

      String restoredFlinkJobId = jobIdIterable.iterator().next();
      Preconditions.checkState(
          !Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new
      // flink job even if it's restored from a snapshot created by another different flink job, so
      // it's safe to assign the max committed checkpoint id from restored flink job to the current
      // flink job.
      this.maxCommittedCheckpointId =
          getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch);

      commitUncommittedDataFiles(restoredFlinkJobId, maxCommittedCheckpointId);
    }
  }

  abstract void initCheckpointState(StateInitializationContext context) throws Exception;

  abstract void commitUncommittedDataFiles(String jobId, long checkpointId) throws Exception;

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info(
        "Start to flush snapshot state to state backend, table: {}, checkpointId: {}",
        table,
        checkpointId);

    // Update the checkpoint state.
    long startNano = System.nanoTime();
    snapshotState(checkpointId);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    committerMetrics.checkpointDuration(
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
  }

  abstract void snapshotState(long checkpointId) throws Exception;

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    // It's possible that we have the following events:
    //   1. snapshotState(ckpId);
    //   2. snapshotState(ckpId+1);
    //   3. notifyCheckpointComplete(ckpId+1);
    //   4. notifyCheckpointComplete(ckpId);
    // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all
    // the files,
    // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.
    if (checkpointId > maxCommittedCheckpointId) {
      LOG.info("Checkpoint {} completed. Attempting commit.", checkpointId);
      commitUpToCheckpoint(checkpointId);
      this.maxCommittedCheckpointId = checkpointId;
    } else {
      LOG.info(
          "Skipping committing checkpoint {}. {} is already committed.",
          checkpointId,
          maxCommittedCheckpointId);
    }
  }

  abstract void commitUpToCheckpoint(long checkpointId) throws IOException;

  protected void commitPendingResult(
      NavigableMap<Long, ? extends WriteResult> pendingResults,
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

  protected void deleteCommittedManifests(
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

  private void replacePartitions(
      NavigableMap<Long, ? extends WriteResult> pendingResults,
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

  protected void commitDeltaTxn(
      NavigableMap<Long, ? extends WriteResult> pendingResults,
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
      for (Map.Entry<Long, ? extends WriteResult> e : pendingResults.entrySet()) {
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

    prepareOperation(operation);

    long startNano = System.nanoTime();

    committerOperation(operation);

    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    LOG.info(
        "Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
        description,
        table.name(),
        branch,
        checkpointId,
        durationMs);
    committerMetrics.commitDuration(durationMs);
  }

  abstract void prepareOperation(SnapshotUpdate<?> operation);

  abstract void committerOperation(SnapshotUpdate<?> operation);

  @Override
  public void endInput() throws IOException {
    // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
    long currentCheckpointId = Long.MAX_VALUE;
    endInput(currentCheckpointId);
  }

  abstract void endInput(long checkpointId) throws IOException;

  @Override
  public void open() throws Exception {
    super.open();

    final String operatorID = getRuntimeContext().getOperatorUniqueID();
    this.workerPool =
        ThreadPools.newWorkerPool("iceberg-worker-pool-" + operatorID, workerPoolSize);
  }

  @Override
  public void close() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }

  static long getMaxCommittedCheckpointId(
      Table table, String flinkJobId, String operatorId, String branch) {
    return getMaxCommittedSummaryValue(
        table, flinkJobId, operatorId, branch, MAX_COMMITTED_CHECKPOINT_ID);
  }

  static long getMaxCommittedSummaryValue(
      Table table, String flinkJobId, String operatorId, String branch, String summaryKey) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      String snapshotOperatorId = summary.get(OPERATOR_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)
          && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
        String value = summary.get(summaryKey);
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
}
