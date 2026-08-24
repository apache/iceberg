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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner for the equality delete conversion pipeline. For each trigger, it picks the oldest
 * staging snapshot that hasn't been converted yet and emits {@link ReadCommand}s describing the
 * files its downstream readers and workers must process.
 *
 * <p>Each trigger runs three steps in order:
 *
 * <ol>
 *   <li>{@link #refreshStagingCursor}: updates {@link #lastStagingSnapshotId} from the most recent
 *       committer marker on the target branch.
 *   <li>{@link #ensureIndexCurrent}: rebuilds the worker index from main when there is no index
 *       yet, when external commits (e.g. compaction) have advanced main, or when the staging
 *       snapshot picked for this cycle is the one the previous cycle planned.
 *   <li>{@link #processStagingSnapshot}: resolve the chosen staging snapshot's eq deletes against
 *       the (now-current) index, pass through any DV files, and index the snapshot's new data files
 *       for the next cycle.
 * </ol>
 *
 * Watermarks separate phases that gate the worker's keyed state. The contract is documented on
 * {@link #advancePhase()}.
 *
 * <p>An {@link EqualityConvertPlan} with the current cycle's metadata is emitted via the {@link
 * #METADATA_STREAM} side output after the read commands.
 *
 * <p>Assumes a single equality-field set supplied via the builder; staging eq-deletes with a
 * different {@code equalityFieldIds} fail fast in {@link #retrieveStagingFiles}. Concurrent writes
 * on the target branch are handled by {@link #ensureIndexCurrent} reindexing from the new main
 * snapshot; commit-time conflicts are caught by {@code RowDelta.validateFromSnapshot}.
 */
@Internal
public class EqualityConvertPlanner extends AbstractStreamOperator<ReadCommand>
    implements OneInputStreamOperator<Trigger, ReadCommand> {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityConvertPlanner.class);

  public static final OutputTag<EqualityConvertPlan> METADATA_STREAM =
      new OutputTag<>("metadata-stream") {};

  public static final OutputTag<IndexCommand> CLEAR_BROADCAST_STREAM =
      new OutputTag<>("clear-broadcast-stream") {};

  private static final String PROCESSED_EQ_DELETE_FILE_NUM_METRIC = "processedEqDeleteFileNum";
  private static final String PROCESSED_STAGING_SNAPSHOT_NUM_METRIC = "processedStagingSnapshotNum";
  private static final String SKIPPED_NO_OP_CYCLES_METRIC = "skippedNoOpCycles";
  private static final String REINDEX_COUNT_METRIC = "reindexCount";

  private final String tableName;
  private final String taskName;
  private final TableLoader tableLoader;
  private final String stagingBranch;
  private final String targetBranch;
  private final boolean stagingOnTargetBranch;
  // Equality-field-id set the worker keys on. Supplied via the builder; every staging
  // eq-delete's equalityFieldIds() must match exactly.
  private final Set<Integer> eqFieldIds;

  // Main snapshot id the worker's index reflects.
  private transient ListState<Long> indexSnapshotState;
  // Generation of the worker's current index build.
  private transient ListState<Long> indexGenerationState;
  // Staging snapshot the last emitted plan covered.
  private transient ListState<Long> pendingStagingSnapshotState;
  // Equality field IDs the index was built with, allows to detect reconfiguration.
  private transient ListState<Integer> eqFieldIdsState;

  private transient Table table;

  private transient Long lastMainSnapshotId;
  private transient Long lastStagingSnapshotId;
  private transient Long indexSnapshotId;
  private transient long indexGeneration;
  // Staging snapshot the last emitted plan covered, checkpointed so it survives a restore taken
  // mid-cycle. Selecting it again means that cycle never committed.
  private transient Long pendingStagingSnapshotId;

  private transient long nextPhaseTs;

  private transient Counter processedEqDeleteFileNumCounter;
  private transient Counter processedStagingSnapshotNumCounter;
  private transient Counter skippedNoOpCyclesCounter;
  private transient Counter reindexCounter;
  private transient Counter errorCounter;

  public EqualityConvertPlanner(
      String tableName,
      String taskName,
      TableLoader tableLoader,
      String stagingBranch,
      String targetBranch,
      Set<Integer> eqFieldIds) {
    this.tableName = tableName;
    this.taskName = taskName;
    this.tableLoader = tableLoader;
    this.stagingBranch = stagingBranch;
    this.targetBranch = targetBranch;
    this.stagingOnTargetBranch = stagingBranch.equals(targetBranch);
    Preconditions.checkArgument(
        eqFieldIds != null && !eqFieldIds.isEmpty(), "eqFieldIds must not be null or empty");
    this.eqFieldIds = ImmutableSet.copyOf(eqFieldIds);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();

    MetricGroup taskMetricGroup =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, 0);
    this.processedEqDeleteFileNumCounter =
        taskMetricGroup.counter(PROCESSED_EQ_DELETE_FILE_NUM_METRIC);
    this.processedStagingSnapshotNumCounter =
        taskMetricGroup.counter(PROCESSED_STAGING_SNAPSHOT_NUM_METRIC);
    this.skippedNoOpCyclesCounter = taskMetricGroup.counter(SKIPPED_NO_OP_CYCLES_METRIC);
    this.reindexCounter = taskMetricGroup.counter(REINDEX_COUNT_METRIC);
    this.errorCounter = taskMetricGroup.counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    indexSnapshotState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("indexSnapshotId", Types.LONG));

    indexSnapshotId = null;
    for (Long stateValue : indexSnapshotState.get()) {
      Preconditions.checkState(
          indexSnapshotId == null, "indexSnapshotId state should hold at most one value");
      indexSnapshotId = stateValue;
    }

    indexGenerationState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("indexGeneration", Types.LONG));

    Long restoredGeneration = null;
    for (Long stateValue : indexGenerationState.get()) {
      Preconditions.checkState(
          restoredGeneration == null, "indexGeneration state should hold at most one value");
      restoredGeneration = stateValue;
    }

    indexGeneration = restoredGeneration != null ? restoredGeneration : 0L;

    eqFieldIdsState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("eqFieldIds", Types.INT));
    Set<Integer> restoredEqFieldIds = Sets.newHashSet(eqFieldIdsState.get());
    Preconditions.checkState(
        restoredEqFieldIds.isEmpty() || restoredEqFieldIds.equals(eqFieldIds),
        "Equality field IDs changed across restart: restored=%s, configured=%s. "
            + "Reconfiguring equality-field columns is not supported; "
            + "restart from a clean state (no savepoint).",
        restoredEqFieldIds,
        eqFieldIds);

    pendingStagingSnapshotState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("pendingStagingSnapshotId", Types.LONG));

    pendingStagingSnapshotId = null;
    for (Long stateValue : pendingStagingSnapshotState.get()) {
      Preconditions.checkState(
          pendingStagingSnapshotId == null,
          "pendingStagingSnapshotId state should hold at most one value");
      pendingStagingSnapshotId = stateValue;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    indexSnapshotState.clear();
    if (indexSnapshotId != null) {
      indexSnapshotState.add(indexSnapshotId);
    }

    indexGenerationState.clear();
    indexGenerationState.add(indexGeneration);

    eqFieldIdsState.clear();
    for (int id : eqFieldIds) {
      eqFieldIdsState.add(id);
    }

    pendingStagingSnapshotState.clear();
    if (pendingStagingSnapshotId != null) {
      pendingStagingSnapshotState.add(pendingStagingSnapshotId);
    }
  }

  @Override
  public void processElement(StreamRecord<Trigger> element) throws Exception {
    long triggerTs = element.getTimestamp();
    nextPhaseTs = Math.max(triggerTs, nextPhaseTs + 1);

    Long currentMainSnapshotId = lastMainSnapshotId;
    try {
      table.refresh();
      Snapshot mainSnapshot = table.snapshot(targetBranch);
      currentMainSnapshotId = mainSnapshot != null ? mainSnapshot.snapshotId() : null;

      LastCommittedWork committedWork = refreshStagingCursor(mainSnapshot);

      Snapshot nextToProcess =
          nextUnprocessedStagingSnapshot(table.snapshot(stagingBranch), mainSnapshot);

      ensureIndexCurrent(mainSnapshot, committedWork, nextToProcess);

      if (nextToProcess == null) {
        LOG.info("Nothing new to convert on staging branch '{}'.", stagingBranch);
        emitNoOpResult(triggerTs, currentMainSnapshotId);
        return;
      }

      processStagingSnapshot(nextToProcess, triggerTs, currentMainSnapshotId);
    } catch (Exception e) {
      LOG.error("Error processing equality deletes for table {} task {}", tableName, taskName, e);
      output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e));
      errorCounter.inc();
      emitDrainResult(triggerTs, currentMainSnapshotId);
    }
  }

  /**
   * Updates {@link #lastStagingSnapshotId} from the most recent committer marker on the target
   * branch. Returns the discovered work, or null when the target has not moved since the last
   * trigger and the cursor therefore cannot have changed.
   */
  private LastCommittedWork refreshStagingCursor(Snapshot mainSnapshot) {
    Long currentMainSnapshotId = mainSnapshot != null ? mainSnapshot.snapshotId() : null;

    if (Objects.equals(lastMainSnapshotId, currentMainSnapshotId)) {
      return null;
    }

    LastCommittedWork info = discoverLastCommittedWork(mainSnapshot);
    updateLastStagingSnapshotId(info);
    lastMainSnapshotId = currentMainSnapshotId;
    return info;
  }

  /**
   * Rebuilds the worker index when it is missing, when external commits have advanced the target
   * branch, or when {@code nextToProcess} is the staging snapshot the previous plan covered.
   * Resolving an eq delete consumes the index entries it matches, so a cycle that failed after its
   * delete phase left the index without them; the cursor only advances once the committer's marker
   * is on the target branch, so planning the same staging snapshot again means that cycle did not
   * commit and nothing else has rebuilt the index.
   */
  private void ensureIndexCurrent(
      Snapshot mainSnapshot, LastCommittedWork committedWork, Snapshot nextToProcess) {
    if (mainSnapshot == null) {
      return;
    }

    boolean bootstrap = indexSnapshotId == null;
    boolean reindex =
        !bootstrap && committedWork != null && committedWork.externalCommitCount() > 0;
    boolean replan =
        !bootstrap
            && nextToProcess != null
            && Objects.equals(pendingStagingSnapshotId, nextToProcess.snapshotId());

    if (bootstrap || reindex || replan) {
      LOG.info(
          "{} worker index from main snapshot {} for field IDs {}.",
          bootstrap ? "Bootstrapping" : "Reindexing",
          mainSnapshot.snapshotId(),
          eqFieldIds);
      rebuildIndex(mainSnapshot, !bootstrap);
    }
  }

  /**
   * Re-emits every data row on {@code mainSnapshot} so the worker's index holds all their positions
   * again, optionally preceded by a CLEAR_INDEX broadcast that evicts keyed entries the re-emission
   * will not re-add (e.g. a PK whose data file was removed by a CoW commit). A bootstrap has no
   * earlier index and so nothing to evict.
   *
   * <p>The worker detects stale state by comparing the generation stamped on the commands it
   * receives with the one it stored, so every rebuild hands out a higher generation, including a
   * rebuild while the target branch stands still.
   */
  private void rebuildIndex(Snapshot mainSnapshot, boolean evictStaleKeys) {
    long generation = indexGeneration + 1;

    if (evictStaleKeys) {
      output.collect(
          CLEAR_BROADCAST_STREAM,
          new StreamRecord<>(
              IndexCommand.clearBeforeReindex(mainSnapshot.snapshotId(), generation)));
      reindexCounter.inc();
    }

    indexSnapshotId = mainSnapshot.snapshotId();
    indexGeneration = generation;
    emitMainDataReadCommands(mainSnapshot);
  }

  private void updateLastStagingSnapshotId(LastCommittedWork info) {
    if (info.lastCommittedStaging() != null) {
      lastStagingSnapshotId = info.lastCommittedStaging();
      return;
    }

    Preconditions.checkState(
        lastMainSnapshotId == null || lastStagingSnapshotId == null || info.reachedLastInspected(),
        "No COMMITTED_STAGING_SNAPSHOT marker reachable on target branch '%s' for table %s, "
            + "but a prior marker was seen (lastStagingSnapshotId=%s). Target may have been "
            + "rewritten (rollback, replace_main, or snapshot expiration). "
            + "Manual intervention required.",
        targetBranch,
        tableName,
        lastStagingSnapshotId);
  }

  /**
   * Walks main back from head looking for the most recent snapshot tagged with {@link
   * EqualityConvertCommitter#COMMITTED_STAGING_SNAPSHOT_PROPERTY}. Returns the staging snapshot id
   * recorded there (or {@code null} if not reached) and the count of intervening external commits.
   *
   * <p>The walk stops at whichever comes first:
   *
   * <ul>
   *   <li>The first snapshot carrying the committer marker.
   *   <li>{@link #lastMainSnapshotId} — anything older was inspected on a previous trigger.
   * </ul>
   */
  private LastCommittedWork discoverLastCommittedWork(Snapshot mainSnapshot) {
    Long lastCommittedStaging = null;
    int externalCount = 0;
    boolean reachedLastInspected = false;
    Snapshot current = mainSnapshot;
    while (current != null) {
      if (lastMainSnapshotId != null && current.snapshotId() == lastMainSnapshotId) {
        reachedLastInspected = true;
        break;
      }

      String prop =
          current.summary().get(EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY);
      if (prop != null) {
        lastCommittedStaging = Long.parseLong(prop);
        break;
      }

      externalCount++;
      current = parentOf(current);
    }

    return new LastCommittedWork(lastCommittedStaging, externalCount, reachedLastInspected);
  }

  private record LastCommittedWork(
      Long lastCommittedStaging, int externalCommitCount, boolean reachedLastInspected) {}

  /**
   * Walks staging history from head back to the stop point and returns the oldest unprocessed
   * snapshot to convert this cycle, or {@code null} if there's nothing new.
   *
   * <p>The stop point is:
   *
   * <ul>
   *   <li>the last-processed staging snapshot, if known;
   *   <li>otherwise, on cold start with {@code stagingBranch != targetBranch}, the common ancestor
   *       with target;
   *   <li>otherwise, on cold start with {@code stagingBranch == targetBranch}, the root of history
   *       ({@code null}). {@link #shouldSkip} filters already-converted snapshots in O(1) via the
   *       {@code COMMITTED_STAGING_SNAPSHOT_PROPERTY} marker, and pure-insert snapshots via the
   *       same-branch eq-delete predicate.
   * </ul>
   *
   * <p>When {@code stagingBranch == targetBranch}, the writer commits eq-deletes and data files
   * directly to target; the converter performs in-place eq-delete-to-DV compaction. On cold start
   * we walk the full history so eq-deletes committed before the converter started are still picked
   * up.
   */
  private Snapshot nextUnprocessedStagingSnapshot(Snapshot stagingHead, Snapshot mainSnapshot) {
    if (stagingHead == null) {
      return null;
    }

    Long stopAt;
    if (lastStagingSnapshotId != null) {
      stopAt = lastStagingSnapshotId;
    } else if (stagingOnTargetBranch) {
      stopAt = null;
    } else {
      stopAt = findCommonAncestor(stagingHead, mainSnapshot);
    }

    Snapshot current = stagingHead;
    Snapshot oldestUnprocessed = null;
    while (current != null) {
      if (stopAt != null && current.snapshotId() == stopAt) {
        break;
      }

      if (!shouldSkip(current)) {
        oldestUnprocessed = current;
      }

      current = parentOf(current);
    }

    return oldestUnprocessed;
  }

  /**
   * Resolves the eq deletes in {@code stagingSnapshot} against the current index and emits the
   * cycle's metadata. Phase ordering (separated by watermarks):
   *
   * <ol>
   *   <li>Eq delete read commands. Eq deletes resolve in the worker.
   *   <li>Staging data files. Indexed for the NEXT cycle's eq-delete resolution.
   * </ol>
   *
   * Cold-start bootstrap of the index from main data is handled separately in {@link
   * #processElement}, which runs once before the first cycle.
   */
  private void processStagingSnapshot(
      Snapshot stagingSnapshot, long triggerTs, Long currentMainSnapshotId) {

    StagingInputs inputs = retrieveStagingFiles(stagingSnapshot);
    Preconditions.checkState(
        !inputs.isEmpty(),
        "Staging snapshot %s has no convertible inputs; shouldSkip should have filtered it.",
        stagingSnapshot.snapshotId());

    // Recorded only once the inputs are known to be convertible: a snapshot that fails validation
    // consumes no index entries, so it must not make later triggers rebuild the index.
    pendingStagingSnapshotId = stagingSnapshot.snapshotId();

    emitDeletePhase(inputs.eqDeleteFiles());
    emitSnapshotDataPhase(inputs.newDataFiles());

    LOG.info(
        "Emitted read commands for {} new data files from staging branch '{}'.",
        inputs.newDataFiles().size(),
        stagingBranch);

    processedStagingSnapshotNumCounter.inc();

    output.collect(
        METADATA_STREAM,
        new StreamRecord<>(
            new EqualityConvertPlan(
                inputs.newDataFiles(),
                inputs.stagingDVFiles(),
                inputs.eqDeleteFiles(),
                stagingSnapshot.snapshotId(),
                currentMainSnapshotId,
                triggerTs,
                nextPhaseTs)));

    advancePhase();
  }

  /**
   * Classifies the files added by {@code stagingSnapshot} into data files, eq delete files, and DV
   * files. Throws if the snapshot:
   *
   * <ul>
   *   <li>Removes data files (rewrites on the staging branch aren't supported).
   *   <li>Contains V2 positional delete files (the converter expects a V3 staging branch written by
   *       Flink, which produces only deletion vectors for deletes).
   *   <li>Contains an eq-delete file whose {@code equalityFieldIds()} doesn't match the
   *       builder-configured set (silent wrong-key serialization otherwise).
   * </ul>
   */
  private StagingInputs retrieveStagingFiles(Snapshot stagingSnapshot) {
    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(stagingSnapshot).build();

    // Rewrites on the staging branch would require rewriting the corresponding DVs against new
    // data files on target. Not implemented; fail fast instead of silently dropping work.
    if (changes.removedDataFiles().iterator().hasNext()) {
      throw new IllegalStateException(
          String.format(
              "Staging snapshot %s on branch '%s' removes data files; "
                  + "equality delete conversion does not support rewrites on the staging branch. "
                  + "Run compaction on the target branch instead.",
              stagingSnapshot.snapshotId(), stagingBranch));
    }

    List<DataFile> newDataFiles = Lists.newArrayList();
    List<DeleteFile> stagingDVFiles = Lists.newArrayList();
    List<DeleteFile> eqDeleteFiles = Lists.newArrayList();

    for (DataFile dataFile : changes.addedDataFiles()) {
      newDataFiles.add(dataFile);
    }

    for (DeleteFile deleteFile : changes.addedDeleteFiles()) {
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        Set<Integer> deleteFieldIds = Sets.newHashSet(deleteFile.equalityFieldIds());
        Preconditions.checkState(
            deleteFieldIds.equals(eqFieldIds),
            "Staging snapshot %s on branch '%s' contains an equality delete file %s with "
                + "equalityFieldIds=%s, which does not match the configured eqFieldIds=%s. "
                + "The writer must use the same equality field IDs as the converter.",
            stagingSnapshot.snapshotId(),
            stagingBranch,
            deleteFile.location(),
            deleteFieldIds,
            eqFieldIds);
        validateDeleteSpecPartitionColumns(stagingSnapshot, deleteFile);
        eqDeleteFiles.add(deleteFile);
      } else if (ContentFileUtil.isDV(deleteFile)) {
        stagingDVFiles.add(deleteFile);
      } else {
        throw new IllegalStateException(
            String.format(
                "Staging snapshot %s on branch '%s' contains a V2 positional delete file (%s); "
                    + "equality delete conversion expects a V3 staging branch written by Flink, "
                    + "which produces only deletion vectors for deletes.",
                stagingSnapshot.snapshotId(), stagingBranch, deleteFile.location()));
      }
    }

    return new StagingInputs(newDataFiles, stagingDVFiles, eqDeleteFiles);
  }

  private void validateDeleteSpecPartitionColumns(Snapshot stagingSnapshot, DeleteFile deleteFile) {
    PartitionSpec spec = table.specs().get(deleteFile.specId());
    for (PartitionField field : spec.fields()) {
      Preconditions.checkState(
          eqFieldIds.contains(field.sourceId()),
          "Staging snapshot %s on branch '%s' contains an equality delete file %s under spec %s, "
              + "which partitions by field '%s' (source id %s) that is not an equality field %s. "
              + "Partition columns must be a subset of the equality fields.",
          stagingSnapshot.snapshotId(),
          stagingBranch,
          deleteFile.location(),
          spec.specId(),
          field.name(),
          field.sourceId(),
          eqFieldIds);
    }
  }

  /** Files added by one staging snapshot, classified for cycle emission. */
  private record StagingInputs(
      List<DataFile> newDataFiles,
      List<DeleteFile> stagingDVFiles,
      List<DeleteFile> eqDeleteFiles) {

    boolean isEmpty() {
      return newDataFiles.isEmpty() && eqDeleteFiles.isEmpty() && stagingDVFiles.isEmpty();
    }
  }

  private void emitDeletePhase(List<DeleteFile> eqDeleteFiles) {
    for (DeleteFile deleteFile : eqDeleteFiles) {
      PartitionSpec spec = table.specs().get(deleteFile.specId());
      output.collect(
          new StreamRecord<>(
              ReadCommand.eqDeleteFile(
                  deleteFile,
                  spec,
                  indexSnapshotId,
                  indexGeneration,
                  dataSequenceNumber(deleteFile)),
              nextPhaseTs));
      processedEqDeleteFileNumCounter.inc();
    }

    advancePhase();
  }

  private void emitSnapshotDataPhase(List<DataFile> snapshotDataFiles) {
    // Shared-branch skip: when stagingBranch == targetBranch, these files are already on target
    // and were indexed by bootstrap/reindex. Re-emitting would duplicate entries in
    // dataRowPositions.
    if (!stagingOnTargetBranch) {
      for (DataFile dataFile : snapshotDataFiles) {
        PartitionSpec spec = table.specs().get(dataFile.specId());
        output.collect(
            new StreamRecord<>(
                ReadCommand.stagingDataFile(
                    new FlinkAddedRowsScanTask(dataFile, spec),
                    indexSnapshotId,
                    indexGeneration,
                    dataSequenceNumber(dataFile)),
                nextPhaseTs));
      }
    }

    advancePhase();
  }

  private void emitNoOpResult(long triggerTimestamp, Long currentMainSnapshotId) {
    skippedNoOpCyclesCounter.inc();
    emitDrainResult(triggerTimestamp, currentMainSnapshotId);
  }

  /** Emits an empty plan result and advances the phase so the pipeline drains. */
  private void emitDrainResult(long triggerTimestamp, Long currentMainSnapshotId) {
    output.collect(
        METADATA_STREAM,
        new StreamRecord<>(
            EqualityConvertPlan.noOp(currentMainSnapshotId, triggerTimestamp, nextPhaseTs)));
    advancePhase();
  }

  /**
   * Emits {@link ReadCommand}s for every data file on {@code mainSnapshot} so the worker indexes
   * them for the configured equality-field set. Existing DVs attached to a data file are loaded by
   * the reader and their positions are skipped. V2 positional deletes are not expected on main; the
   * reader throws if it encounters one. Equality deletes attached to the scan task are skipped
   * during indexing (they are processed via the planner's eq-delete read commands).
   */
  private void emitMainDataReadCommands(Snapshot mainSnapshot) {
    long commitSnapshotId = mainSnapshot.snapshotId();

    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(commitSnapshotId).planFiles()) {
      for (FileScanTask task : tasks) {
        output.collect(
            new StreamRecord<>(
                ReadCommand.dataFile(
                    task, indexSnapshotId, indexGeneration, dataSequenceNumber(task.file())),
                nextPhaseTs));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to plan files for main index", e);
    }

    LOG.info(
        "Emitted main data read commands for field IDs {} from snapshot {}.",
        eqFieldIds,
        commitSnapshotId);

    advancePhase();
  }

  /**
   * Emits a phase-end watermark and bumps the phase timestamp. Every phase-emitting method must
   * call this exactly once after its records; the worker uses these watermarks to gate keyed-state
   * transitions. Missing or extra calls silently break ordering.
   */
  private void advancePhase() {
    output.emitWatermark(new Watermark(nextPhaseTs));
    nextPhaseTs++;
  }

  /**
   * Returns {@code true} if {@code snapshot} can be skipped:
   *
   * <ul>
   *   <li>It was already committed by us (carries {@link
   *       EqualityConvertCommitter#COMMITTED_STAGING_SNAPSHOT_PROPERTY}), OR
   *   <li>it adds no data or delete files (e.g. delete-file-only removals), OR
   *   <li>{@code stagingBranch == targetBranch} and the snapshot adds no equality-delete files.
   *       Pure-insert (and data-file-only) commits on the shared branch don't need a conversion
   *       cycle: their data is already on target, and the worker's index stays fresh via {@link
   *       #ensureIndexCurrent} when the next eq-delete arrives.
   * </ul>
   *
   * <p>Filter checks read per-snapshot counts from {@link Snapshot#summary()}; we don't parse
   * manifests here. Manifest parsing happens later in {@link #retrieveStagingFiles} only for the
   * chosen snapshot.
   */
  private boolean shouldSkip(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    if (summary.containsKey(EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY)) {
      return true;
    }

    long addedDataFiles = summaryCount(summary, SnapshotSummary.ADDED_FILES_PROP);
    long addedDeleteFiles = summaryCount(summary, SnapshotSummary.ADDED_DELETE_FILES_PROP);
    if (addedDataFiles == 0 && addedDeleteFiles == 0) {
      LOG.info(
          "Skipping staging snapshot {}: no added data or delete files.", snapshot.snapshotId());
      return true;
    }

    if (stagingOnTargetBranch
        && summaryCount(summary, SnapshotSummary.ADD_EQ_DELETE_FILES_PROP) == 0) {
      return true;
    }

    return false;
  }

  private static long summaryCount(Map<String, String> summary, String key) {
    String value = summary.get(key);
    return value == null ? 0L : Long.parseLong(value);
  }

  /**
   * Returns the id of the newest snapshot that is reachable from both the staging and target branch
   * heads (i.e. the most recent common ancestor where the two branches last matched), or {@code
   * null} if they share no history. Used on cold start to know where staging diverged from target
   * so we can skip converting snapshots that already exist on target.
   */
  private Long findCommonAncestor(Snapshot stagingHead, Snapshot mainHead) {
    if (mainHead == null) {
      return null;
    }

    Set<Long> stagingSeen = Sets.newHashSet();
    Set<Long> mainSeen = Sets.newHashSet();
    Snapshot stagingCurrent = stagingHead;
    Snapshot mainCurrent = mainHead;

    while (stagingCurrent != null || mainCurrent != null) {
      if (stagingCurrent != null) {
        long id = stagingCurrent.snapshotId();
        if (mainSeen.contains(id)) {
          return id;
        }

        stagingSeen.add(id);
        stagingCurrent = parentOf(stagingCurrent);
      }

      if (mainCurrent != null) {
        long id = mainCurrent.snapshotId();
        if (stagingSeen.contains(id)) {
          return id;
        }

        mainSeen.add(id);
        mainCurrent = parentOf(mainCurrent);
      }
    }

    return null;
  }

  /** Returns the parent snapshot, or {@code null} if {@code snapshot} has no parent. */
  private Snapshot parentOf(Snapshot snapshot) {
    Long parentId = snapshot.parentId();
    return parentId != null ? table.snapshot(parentId) : null;
  }

  private static long dataSequenceNumber(ContentFile<?> file) {
    Long sequenceNumber = file.dataSequenceNumber();
    Preconditions.checkNotNull(
        sequenceNumber, "Missing data sequence number for committed file %s", file.location());
    return sequenceNumber;
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  @VisibleForTesting
  long reindexCount() {
    return reindexCounter.getCount();
  }

  @VisibleForTesting
  long skippedNoOpCycles() {
    return skippedNoOpCyclesCounter.getCount();
  }

  @VisibleForTesting
  long processedStagingSnapshotNum() {
    return processedStagingSnapshotNumCounter.getCount();
  }

  @VisibleForTesting
  long processedEqDeleteFileNum() {
    return processedEqDeleteFileNumCounter.getCount();
  }
}
