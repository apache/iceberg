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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the primary key index in step with the table by deciding, once per checkpoint, which data
 * files have to be indexed and which ones the index must forget. Runs with a parallelism of one so
 * that a single view of the table drives every shard of the index.
 *
 * <p>The index has to be complete before a delete can resolve against it, so the first checkpoint
 * requests every data file of the branch. From then on only what changed is requested: the
 * coordinator remembers the snapshot the index reflects and inspects the snapshots committed since.
 *
 * <ul>
 *   <li>Snapshots this sink committed are skipped, since the writer reports the rows it wrote.
 *   <li>Data files another operation removed, typically a compaction, are forgotten, and the files
 *       it added are indexed. That is what keeps compaction from breaking the index.
 *   <li>Data files another writer added are indexed as well, so that later deletes reach their
 *       rows. Deletes this sink resolved before those rows were indexed do not apply to them, which
 *       is logged.
 * </ul>
 *
 * <p>When the snapshot the index reflects is no longer an ancestor of the branch, typically because
 * it expired or the branch was rolled back, the changes since cannot be enumerated. The index is
 * then rebuilt under a new generation: every data file of the branch is read again for it, and a
 * {@link DvOnlyRecord.Type#CLEANUP} broadcast on {@link #CLEANUP_STREAM} drops every file that was
 * not, except the files holding rows of checkpoints this sink has not committed yet.
 *
 * <p>The snapshot the index reflects is also reported on {@link #BASELINE_STREAM}, once per
 * checkpoint. The deletes of that checkpoint resolve against the index as of that snapshot, which
 * is what lets {@link IcebergCommitter} tell whether anything committed since invalidates them.
 *
 * <p>Commands are emitted while the checkpoint barrier is handled, or when the input ends, so they
 * reach the index ahead of the deletes resolved at that point. They are idempotent, which is what
 * makes a replay after a failed checkpoint harmless: indexing a file twice yields the same
 * positions and dropping a file twice is a no-op.
 */
@Internal
class DvOnlyCoordinator extends AbstractStreamOperator<DvOnlyRecord>
    implements OneInputStreamOperator<CommittableMessage<SinkWriteResult>, DvOnlyRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyCoordinator.class);

  static final OutputTag<CommittableMessage<SinkWriteResult>> BASELINE_STREAM =
      new OutputTag<>(
          "dv-only-baseline", CommittableMessageTypeInfo.of(SinkWriteResultSerializer::dvOnly));

  static final OutputTag<DvOnlyRecord> CLEANUP_STREAM =
      new OutputTag<>("dv-only-cleanup", TypeInformation.of(DvOnlyRecord.class));

  private static final ListStateDescriptor<Boolean> BUILT_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexBuilt", Types.BOOLEAN);

  private static final ListStateDescriptor<Long> BASELINE_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexCoverage", Types.LONG);

  private static final ListStateDescriptor<String> KEY_FINGERPRINT_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexKeyFingerprint", Types.STRING);

  private static final ListStateDescriptor<Long> GENERATION_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyIndexGeneration", Types.LONG);

  private static final ListStateDescriptor<Long> COMMITTED_CHECKPOINT_DESCRIPTOR =
      new ListStateDescriptor<>("dvOnlyCommittedCheckpoint", Types.LONG);

  private final TableLoader tableLoader;
  private final String branch;
  private final String keyFingerprint;
  private final int workerPoolSize;

  private transient Table table;
  private transient ExecutorService workerPool;
  private transient ListState<Boolean> builtState;
  private transient ListState<Long> baselineState;
  private transient ListState<String> fingerprintState;
  private transient ListState<Long> generationState;
  private transient ListState<Long> committedCheckpointState;

  private transient boolean built;

  /** Snapshot the index reflects, or null while the branch has none. */
  private transient Long baselineSnapshotId;

  /** Generation the index was last rebuilt for; every data file read is stamped with it. */
  private transient long generation;

  /** Last checkpoint of this sink seen committed to the branch. */
  private transient long committedCheckpointId;

  private transient String jobId;

  private transient boolean finished;

  DvOnlyCoordinator(
      TableLoader tableLoader, String branch, String keyFingerprint, int workerPoolSize) {
    this.tableLoader = tableLoader;
    this.branch = branch;
    this.keyFingerprint = keyFingerprint;
    this.workerPoolSize = workerPoolSize;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    builtState = context.getOperatorStateStore().getListState(BUILT_DESCRIPTOR);
    for (Boolean value : builtState.get()) {
      built = built || value;
    }

    baselineState = context.getOperatorStateStore().getListState(BASELINE_DESCRIPTOR);
    for (Long value : baselineState.get()) {
      baselineSnapshotId = value;
    }

    generationState = context.getOperatorStateStore().getListState(GENERATION_DESCRIPTOR);
    generation = 0L;
    for (Long value : generationState.get()) {
      generation = value;
    }

    committedCheckpointState =
        context.getOperatorStateStore().getListState(COMMITTED_CHECKPOINT_DESCRIPTOR);
    committedCheckpointId = DvOnlyRecord.NO_CHECKPOINT;
    for (Long value : committedCheckpointState.get()) {
      committedCheckpointId = value;
    }

    fingerprintState = context.getOperatorStateStore().getListState(KEY_FINGERPRINT_DESCRIPTOR);
    String restoredFingerprint = null;
    for (String value : fingerprintState.get()) {
      restoredFingerprint = value;
    }

    if (restoredFingerprint != null) {
      // The index is keyed by serialized equality values, so an index built from other equality
      // fields than the ones currently configured can no longer match them.
      Preconditions.checkState(
          restoredFingerprint.equals(keyFingerprint),
          "The primary key index was built from equality fields [%s], but the sink is configured "
              + "with [%s]. Clear the job state and restart to rebuild the index.",
          restoredFingerprint,
          keyFingerprint);
    } else if (built) {
      LOG.warn("Restored a primary key index without a key fingerprint, skipping the check");
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    DvOnlyExecution.checkAligned(getContainingTask().getEnvironment().getJobConfiguration());
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();
    jobId = getContainingTask().getEnvironment().getJobID().toString();
    workerPool =
        ThreadPools.newFixedThreadPool(
            "iceberg-dv-only-coordinator-pool-" + getRuntimeContext().getOperatorUniqueID(),
            workerPoolSize);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    builtState.clear();
    builtState.add(built);

    baselineState.clear();
    if (baselineSnapshotId != null) {
      baselineState.add(baselineSnapshotId);
    }

    generationState.clear();
    generationState.add(generation);

    committedCheckpointState.clear();
    committedCheckpointState.add(committedCheckpointId);

    fingerprintState.clear();
    fingerprintState.add(keyFingerprint);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<SinkWriteResult>> element) {
    // The committables travel to the aggregator through the other branch of the topology. This
    // operator only consumes the stream to be driven by its checkpoint barriers.
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    // Once the input ended every change has been reported, see finish().
    if (!finished) {
      updateIndex(checkpointId);
    }

    super.prepareSnapshotPreBarrier(checkpointId);
  }

  /**
   * Updates the index one last time when the input ends. The deletes of the last changes are
   * resolved while the downstream operators finish, and no further barrier arrives to trigger it.
   */
  @Override
  public void finish() throws Exception {
    updateIndex(DvOnlyExecution.END_OF_INPUT);
    finished = true;
    super.finish();
  }

  private void updateIndex(long checkpointId) {
    table.refresh();
    Snapshot head = table.snapshot(branch);
    if (!built) {
      indexBranch(head);
      built = true;
    } else if (!SinkUtil.isTraceable(table, head, baselineSnapshotId)) {
      LOG.warn(
          "Snapshot {} that the primary key index of table {} reflects is no longer an ancestor of "
              + "branch '{}', most likely because it expired. Rebuilding the index from the whole "
              + "branch.",
          baselineSnapshotId,
          table.name(),
          branch);
      rebuildIndex(head);
    } else if (head != null) {
      indexChanges(head);
    }

    baselineSnapshotId = head != null ? head.snapshotId() : null;

    // The deletes resolved now resolve against the index as of this snapshot. The committer has to
    // know it, because anything committed after it was not seen by the index.
    output.collect(
        BASELINE_STREAM,
        new StreamRecord<>(
            new CommittableWithLineage<>(
                SinkWriteResult.baseline(baselineSnapshotId), checkpointId, 0)));
  }

  private void rebuildIndex(Snapshot head) {
    generation++;
    committedCheckpointId =
        Math.max(
            committedCheckpointId, SinkUtil.maxCommittedCheckpointId(table, head, jobId, null));
    output.collect(
        CLEANUP_STREAM,
        new StreamRecord<>(DvOnlyRecord.cleanup(generation, committedCheckpointId)));
    indexBranch(head);
  }

  private void indexBranch(Snapshot head) {
    if (head == null) {
      LOG.info("Branch '{}' of table {} is empty, nothing to index", branch, table.name());
      return;
    }

    long files = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(head.snapshotId()).planWith(workerPool).planFiles()) {
      for (FileScanTask task : tasks) {
        requestRead(task.file(), task.deletes());
        files++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to plan files for the primary key index of table " + table.name(), e);
    }

    LOG.info(
        "Requested indexing of {} data file(s) from snapshot {} of branch '{}' of table {} for "
            + "generation {}",
        files,
        head.snapshotId(),
        branch,
        table.name(),
        generation);
  }

  private void indexChanges(Snapshot head) {
    Set<String> removed = Sets.newLinkedHashSet();
    Map<String, DataFile> added = Maps.newLinkedHashMap();
    List<Snapshot> committedSince =
        Lists.reverse(
            Lists.newArrayList(
                SnapshotUtil.ancestorsBetween(table, head.snapshotId(), baselineSnapshotId)));
    for (Snapshot snapshot : committedSince) {
      Long ownCheckpointId = SinkUtil.committedCheckpointId(snapshot, jobId, null);
      boolean ownCommit = ownCheckpointId != null;
      if (ownCommit) {
        committedCheckpointId = Math.max(committedCheckpointId, ownCheckpointId);
      }

      boolean addsFiles = summaryCount(snapshot, SnapshotSummary.ADDED_FILES_PROP) > 0;
      boolean removesFiles = summaryCount(snapshot, SnapshotSummary.DELETED_FILES_PROP) > 0;
      if (removesFiles || (addsFiles && !ownCommit)) {
        collectChanges(snapshot, removed, added);
      }
    }

    removed.forEach(path -> output.collect(new StreamRecord<>(DvOnlyRecord.dropFile(path))));
    // The deletion vectors of the added files are not read along: a row indexed although it is
    // deleted only makes a later delete of its key mark it deleted again.
    added.values().forEach(file -> requestRead(file, ImmutableList.of()));

    if (!removed.isEmpty() || !added.isEmpty()) {
      LOG.info(
          "Dropped {} data file(s) from the primary key index and requested indexing of {} "
              + "data file(s) on branch '{}' of table {}",
          removed.size(),
          added.size(),
          branch,
          table.name());
    }
  }

  private void collectChanges(Snapshot snapshot, Set<String> removed, Map<String, DataFile> added) {
    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
    for (DataFile file : changes.removedDataFiles()) {
      if (added.remove(file.location()) == null) {
        removed.add(file.location());
      }
    }

    long addedBySnapshot = 0;
    for (DataFile file : changes.addedDataFiles()) {
      added.put(file.location(), file);
      addedBySnapshot++;
    }

    if (addedBySnapshot > 0 && !DataOperations.REPLACE.equals(snapshot.operation())) {
      LOG.info(
          "Snapshot {} ({}) of branch '{}' of table {} was committed by another writer and added "
              + "{} data file(s). Their rows are indexed now; deletes this sink resolved earlier "
              + "do not apply to them.",
          snapshot.snapshotId(),
          snapshot.operation(),
          branch,
          table.name(),
          addedBySnapshot);
    }
  }

  private static long summaryCount(Snapshot snapshot, String property) {
    return PropertyUtil.propertyAsLong(snapshot.summary(), property, 0);
  }

  private void requestRead(DataFile file, List<DeleteFile> deletes) {
    output.collect(
        new StreamRecord<>(
            DvOnlyRecord.readFile(
                file.location(), new PkIndexReadTask(file, deletes).encode(), generation)));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (workerPool != null) {
      workerPool.shutdown();
    }

    tableLoader.close();
  }
}
