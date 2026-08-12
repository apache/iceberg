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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits data files and DVs to the target branch. Receives {@link DVWriteResult}s from parallel
 * {@link EqualityConvertDVWriter} instances (input 1) and an {@link EqualityConvertPlan} from the
 * planner (input 2). Assembles the final file lists and commits using a {@link RowDelta} operation
 * once the plan result and done-timestamp watermark have both arrived.
 *
 * <p>The commit is gated on the plan's done-timestamp watermark.
 *
 * <p>Watermarks are forwarded only after the cycle commits, never mid-cycle. The {@link
 * LockRemover} releases the maintenance lock once a watermark past the trigger's start epoch
 * reaches it. The planner emits phase watermarks in the middle of a cycle; forwarding those would
 * release the lock before this commit, letting the TriggerManager start a concurrent cycle that
 * re-processes the same uncommitted staging snapshot.
 *
 * <p>Emits a {@link Trigger} after each cycle (commit, no-op, or error) so the downstream {@link
 * TaskResultAggregator} can track task completion. This is the sole source of Trigger records for
 * the Aggregator.
 *
 * <p>No-op vs error: a no-op cycle (empty plan result from {@code
 * EqualityConvertPlanner.emitNoOpResult}) returns early in {@code commitIfNeeded} without writing
 * anything; the Trigger emit in {@code processWatermark} still happens, so the maintenance task
 * completes cleanly. Errors are reported via {@link TaskResultAggregator#ERROR_STREAM} side output;
 * the Aggregator collects them and surfaces failure on its own watermark.
 *
 * <p>The committer is intentionally stateless: {@code bufferedResults} and {@code planResult} are
 * not checkpointed. The maintenance framework ensures mutual exclusive tasks, and on restart the
 * planner re-derives its position from {@link #COMMITTED_STAGING_SNAPSHOT_PROPERTY} on main, so the
 * committer never receives a plan for an already-committed staging snapshot.
 */
@Internal
public class EqualityConvertCommitter extends AbstractStreamOperator<Trigger>
    implements TwoInputStreamOperator<DVWriteResult, EqualityConvertPlan, Trigger> {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityConvertCommitter.class);

  public static final String COMMITTED_STAGING_SNAPSHOT_PROPERTY =
      "equality-convert-staging-snapshot";

  private static final String ADDED_DV_NUM_METRIC = "addedDvNum";
  private static final String REMOVED_EQ_DELETE_NUM_METRIC = "removedEqDeleteNum";
  private static final String COMMIT_DURATION_MS_METRIC = "commitDurationMs";

  private final String tableName;
  private final String taskName;
  private final TableLoader tableLoader;
  private final String targetBranch;
  private final boolean stagingOnTargetBranch;

  private transient Table table;
  private transient List<DVWriteResult> bufferedResults;
  private transient EqualityConvertPlan planResult;

  private transient Counter errorCounter;
  private transient Counter addedDataFileNumCounter;
  private transient Counter addedDataFileSizeCounter;
  private transient Counter addedDvNumCounter;
  private transient Counter removedEqDeleteNumCounter;
  private transient Counter commitDurationMsCounter;

  public EqualityConvertCommitter(
      String tableName,
      String taskName,
      TableLoader tableLoader,
      String stagingBranch,
      String targetBranch) {
    this.tableName = tableName;
    this.taskName = taskName;
    this.tableLoader = tableLoader;
    this.targetBranch = targetBranch;
    this.stagingOnTargetBranch = stagingBranch.equals(targetBranch);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.bufferedResults = Lists.newArrayList();

    MetricGroup taskMetricGroup =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, 0);
    this.errorCounter = taskMetricGroup.counter(TableMaintenanceMetrics.ERROR_COUNTER);
    this.addedDataFileNumCounter =
        taskMetricGroup.counter(TableMaintenanceMetrics.ADDED_DATA_FILE_NUM_METRIC);
    this.addedDataFileSizeCounter =
        taskMetricGroup.counter(TableMaintenanceMetrics.ADDED_DATA_FILE_SIZE_METRIC);
    this.addedDvNumCounter = taskMetricGroup.counter(ADDED_DV_NUM_METRIC);
    this.removedEqDeleteNumCounter = taskMetricGroup.counter(REMOVED_EQ_DELETE_NUM_METRIC);
    this.commitDurationMsCounter = taskMetricGroup.counter(COMMIT_DURATION_MS_METRIC);
  }

  @Override
  public void processElement1(StreamRecord<DVWriteResult> record) {
    bufferedResults.add(record.getValue());
  }

  @Override
  public void processElement2(StreamRecord<EqualityConvertPlan> record) {
    planResult = record.getValue();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (planResult == null || mark.getTimestamp() < planResult.doneTimestamp()) {
      // Hold back watermarks until the cycle commits so the LockRemover keeps the maintenance lock
      // for the whole cycle. Forwarding the planner's mid-cycle phase watermarks will release the
      // lock early and could let the next trigger run a concurrent cycle on the same staging
      // snapshot.
      return;
    }

    try {
      commitIfNeeded();
    } catch (Exception e) {
      LOG.error(
          "Failed to commit equality convert result for table {} task {}", tableName, taskName, e);
      output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e));
      errorCounter.inc();
    }

    // Emit Trigger for the Aggregator (even on error or no-op).
    output.collect(new StreamRecord<>(Trigger.create(planResult.triggerTimestamp(), 0)));

    bufferedResults.clear();
    planResult = null;

    super.processWatermark(mark);
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  private void commitIfNeeded() {
    for (DVWriteResult result : bufferedResults) {
      if (result.isAbort()) {
        LOG.warn(
            "Skipping commit for table {} task {}: a DV writer reported an error.",
            tableName,
            taskName);
        deleteUncommittedDVs();
        return;
      }
    }

    // No-op cycle: the planner emitted an empty plan result (see
    // EqualityConvertPlanner.emitNoOpResult) because the next staging snapshot was filtered by
    // shouldSkip or there was nothing new on staging. processWatermark still forwards a Trigger to
    // TaskResultAggregator, so the maintenance task completes cleanly.
    if (planResult.noOp()) {
      return;
    }

    table.refresh();

    List<DeleteFile> allDvFiles = Lists.newArrayList();
    List<DeleteFile> allRewrittenDvFiles = Lists.newArrayList();
    for (DVWriteResult result : bufferedResults) {
      allDvFiles.addAll(result.dvFiles());
      allRewrittenDvFiles.addAll(result.rewrittenDvFiles());
    }

    RowDelta rowDelta = buildRowDelta(planResult.dataFiles(), allDvFiles, allRewrittenDvFiles);

    long startNano = System.nanoTime();
    try {
      commit(rowDelta);
    } catch (CommitStateUnknownException e) {
      // Commit outcome unknown: the DVs may already be referenced by a committed snapshot. Leave
      // them for Remove Orphan Files rather than risk deleting live data.
      throw e;
    } catch (Exception e) {
      // Commit definitively failed: the DVs this cycle wrote are unreferenced. Delete them so a
      // failed cycle does not leak Puffin files. Rewritten DVs stay (still live on the target).
      deleteUncommittedDVs();
      throw e;
    }

    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    commitDurationMsCounter.inc(durationMs);

    int removedEqDeletes = stagingOnTargetBranch ? planResult.eqDeleteFiles().size() : 0;
    LOG.info(
        "Committed {} data files and {} DV files, removed {} equality delete files on branch '{}' "
            + "for table {} in {} ms. Processed staging snapshot {}.",
        planResult.dataFiles().size(),
        allDvFiles.size(),
        removedEqDeletes,
        targetBranch,
        tableName,
        durationMs,
        planResult.stagingSnapshotId());

    // Only count files actually added by this commit. When sameBranch, the writer already
    // committed the data files to target and buildRowDelta does not re-add them.
    if (!stagingOnTargetBranch) {
      for (DataFile dataFile : planResult.dataFiles()) {
        addedDataFileNumCounter.inc();
        addedDataFileSizeCounter.inc(dataFile.fileSizeInBytes());
      }
    }

    addedDvNumCounter.inc(allDvFiles.size());
    removedEqDeleteNumCounter.inc(removedEqDeletes);
  }

  @VisibleForTesting
  void commit(RowDelta rowDelta) {
    rowDelta.commit();
  }

  @VisibleForTesting
  long removedEqDeleteNum() {
    return removedEqDeleteNumCounter.getCount();
  }

  /**
   * Deletes the DVs this cycle wrote but did not commit (abort or definite commit failure). Only
   * the newly written DVs are removed; rewritten DVs remain referenced on the target branch. Best
   * effort: a delete failure is logged, not propagated, so it never masks the original error.
   */
  private void deleteUncommittedDVs() {
    for (DVWriteResult result : bufferedResults) {
      if (result.isAbort()) {
        continue;
      }

      for (DeleteFile dvFile : result.dvFiles()) {
        try {
          table.io().deleteFile(dvFile.location());
        } catch (RuntimeException e) {
          LOG.warn(
              "Failed to delete uncommitted DV {} for table {} task {}",
              dvFile.location(),
              tableName,
              taskName,
              e);
        }
      }
    }
  }

  private RowDelta buildRowDelta(
      List<DataFile> dataFiles, List<DeleteFile> allDvFiles, List<DeleteFile> allRewrittenDvFiles) {
    RowDelta rowDelta = table.newRowDelta();

    // Fail the commit on external target-branch activity since the planner's snapshot. The next
    // trigger detects the change and reindexes.
    if (planResult.mainSnapshotId() != null) {
      rowDelta.validateFromSnapshot(planResult.mainSnapshotId());
    }

    rowDelta.validateNoConflictingDataFiles();
    rowDelta.validateNoConflictingDeleteFiles();

    Set<String> referencedDataFiles = Sets.newHashSet();
    for (DeleteFile dvFile : allDvFiles) {
      if (dvFile.referencedDataFile() != null) {
        referencedDataFiles.add(dvFile.referencedDataFile());
      }
    }

    if (!referencedDataFiles.isEmpty()) {
      rowDelta.validateDataFilesExist(referencedDataFiles);
    }

    // When stagingBranch == targetBranch, the writer already committed data files and DVs to the
    // target branch. Re-adding them here would produce duplicate manifest entries. Skip those
    // paths; only the new DVs from the writer need to be added.
    if (!stagingOnTargetBranch) {
      for (DataFile dataFile : dataFiles) {
        rowDelta.addRows(dataFile);
      }
    }

    for (DeleteFile dvFile : allDvFiles) {
      rowDelta.addDeletes(dvFile);
    }

    if (stagingOnTargetBranch) {
      // In-place conversion: the equality deletes are already on the target branch. Their rows
      // are now covered by DVs, so remove them; readers then stop loading and applying equality
      // deletes.
      for (DeleteFile eqDeleteFile : planResult.eqDeleteFiles()) {
        rowDelta.removeDeletes(eqDeleteFile);
      }
    } else {
      // Separate target branch: the writer's DVs exist only on staging, so add them to the target
      // (addStagingDeletes skips any superseded DVs).
      addStagingDeletes(rowDelta, referencedDataFiles, planResult.stagingDVFiles());
    }

    removeRewrittenDVs(
        rowDelta, allRewrittenDvFiles, planResult.stagingDVFiles(), stagingOnTargetBranch);

    rowDelta.toBranch(targetBranch);
    rowDelta.set(
        COMMITTED_STAGING_SNAPSHOT_PROPERTY, String.valueOf(planResult.stagingSnapshotId()));
    return rowDelta;
  }

  /** Adds staging delete files, skipping DVs that overlap with conversion DVs (V3 rule). */
  private static void addStagingDeletes(
      RowDelta rowDelta, Set<String> dvCoveredDataFiles, List<DeleteFile> stagingDVFiles) {
    for (DeleteFile stagingDelete : stagingDVFiles) {
      // V3 allows one DV per data file. When a staging snapshot contains both a writer-committed
      // DV and an eq-delete that resolves to additional positions in the same data file, the
      // writer folds the staging DV into a new merged DV (via
      // EqualityConvertDVWriter.collectExistingDVs). Skip the superseded staging DV; adding both
      // would commit two DVs for the same data file.
      if (ContentFileUtil.isDV(stagingDelete)
          && stagingDelete.referencedDataFile() != null
          && dvCoveredDataFiles.contains(stagingDelete.referencedDataFile())) {
        continue;
      }

      rowDelta.addDeletes(stagingDelete);
    }
  }

  /**
   * Removes rewritten DVs. On a separate target branch, staging DVs are not yet on target, so they
   * are skipped: removeDeletes would fail. On a shared branch they are already on target and must
   * be removed like any other rewritten DV; the merged DV that supersedes them would otherwise be a
   * second DV for the same data file (V3 allows one).
   */
  private static void removeRewrittenDVs(
      RowDelta rowDelta,
      List<DeleteFile> allRewrittenDvFiles,
      List<DeleteFile> stagingDVFiles,
      boolean stagingOnTargetBranch) {
    Set<String> stagingDeleteLocations = Sets.newHashSet();
    for (DeleteFile sd : stagingDVFiles) {
      stagingDeleteLocations.add(sd.location());
    }

    for (DeleteFile rewrittenDv : allRewrittenDvFiles) {
      if (stagingOnTargetBranch || !stagingDeleteLocations.contains(rewrittenDv.location())) {
        rowDelta.removeDeletes(rewrittenDv);
      }
    }
  }
}
