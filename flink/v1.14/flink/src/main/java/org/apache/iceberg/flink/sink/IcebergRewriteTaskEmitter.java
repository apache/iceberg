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
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRewriteTaskEmitter extends AbstractStreamOperator<RewriteTask>
    implements OneInputStreamOperator<CommitResult, RewriteTask> {

  private static final long serialVersionUID = 1L;
  private static final long DUMMY_SNAPSHOT_ID = -1L;
  private static final long MAX_CHECKPOINT_ID = Long.MAX_VALUE;
  private static final Set<String> VALIDATE_DATA_CHANGE_FILES_OPERATIONS =
      ImmutableSet.of(DataOperations.APPEND, DataOperations.OVERWRITE, DataOperations.DELETE);

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteTaskEmitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  private final TableLoader tableLoader;

  private transient Table table;
  private transient String flinkJobId;
  private transient boolean caseSensitive;
  private transient StreamingBinPackStrategy strategy;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;

  private transient long lastReceivedSnapshotId;
  private transient Map<StructLikeWrapper, Deque<RewriteFileGroup>> pendingFileGroupsByPartition;
  private transient NavigableMap<Long, List<RewriteFileGroup>> rewrittenFileGroupsPerCheckpoint;

  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;

  private static final ListStateDescriptor<Long> RECEIVED_SNAPSHOT_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-streaming-rewrite-received-snapshot-id", BasicTypeInfo.LONG_TYPE_INFO);
  private transient ListState<Long> receivedSnapshotIdState;

  private static final ListStateDescriptor<byte[]> REWRITE_FILE_GROUPS_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-streaming-rewrite-pending-file-groups-state", TypeInformation.of(new TypeHint<byte[]>() {}));
  private transient ListState<byte[]> pendingRewriteFileGroupsState;

  public IcebergRewriteTaskEmitter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.strategy = new StreamingBinPackStrategy();

    this.caseSensitive = PropertyUtil.propertyAsBoolean(table.properties(),
        FlinkSinkOptions.STREAMING_REWRITE_CASE_SENSITIVE,
        FlinkSinkOptions.STREAMING_REWRITE_CASE_SENSITIVE_DEFAULT);

    this.lastReceivedSnapshotId = DUMMY_SNAPSHOT_ID;
    this.pendingFileGroupsByPartition = Maps.newHashMap();
    this.rewrittenFileGroupsPerCheckpoint = Maps.newTreeMap();

    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();
    this.manifestOutputFileFactory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, subTaskId, attemptId);

    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    this.receivedSnapshotIdState = context.getOperatorStateStore().getListState(RECEIVED_SNAPSHOT_ID_DESCRIPTOR);
    this.pendingRewriteFileGroupsState = context.getOperatorStateStore().getListState(REWRITE_FILE_GROUPS_DESCRIPTOR);
    if (context.isRestored()) {
      String restoredFlinkJobId = jobIdState.get().iterator().next();
      Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      for (byte[] bytes : pendingRewriteFileGroupsState.get()) {
        RewriteFileGroup fileGroup = SimpleVersionedSerialization.readVersionAndDeSerialize(
            RewriteFileGroup.Serializer.INSTANCE, bytes);
        StructLikeWrapper wrapper = StructLikeWrapper.forType(table.spec().partitionType()).set(fileGroup.partition());
        pendingFileGroupsByPartition.computeIfAbsent(wrapper, p -> Lists.newLinkedList()).add(fileGroup);
      }

      this.lastReceivedSnapshotId = receivedSnapshotIdState.get().iterator().next();
      long lastCommittedSnapshotId = getLastCommittedSnapshotId(table, restoredFlinkJobId);
      // The last received snapshot id would be negative on first commit.
      if (lastCommittedSnapshotId > 0 && (lastReceivedSnapshotId == DUMMY_SNAPSHOT_ID ||
          SnapshotUtil.isAncestorOf(table, lastCommittedSnapshotId, lastReceivedSnapshotId))) {
        // Restore last received snapshot id from state will lose the last committed snapshot of restored flink job,
        // because the committer emit committed result after checkpoint so that the last received snapshot id
        // has not updated when checkpointing. Therefore, we need to append all data files and delete files
        // which are committed by the restored flink job and append all eq-delete files which are committed by other
        // writer to catch up to the last committed snapshot of the restored flink job.
        appendFilesWithin(lastReceivedSnapshotId, lastCommittedSnapshotId, restoredFlinkJobId);
        emitRewriteTask();
        this.lastReceivedSnapshotId = lastCommittedSnapshotId;
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush rewrite tasks to state backend, table: {}, checkpointId: {}", table, checkpointId);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    receivedSnapshotIdState.clear();
    receivedSnapshotIdState.add(lastReceivedSnapshotId);

    List<byte[]> pendingRewriteFileGroups = Lists.newArrayListWithCapacity(pendingFileGroupsByPartition.size());
    for (Deque<RewriteFileGroup> rewriteFileGroups : pendingFileGroupsByPartition.values()) {
      for (RewriteFileGroup rewriteFileGroup : rewriteFileGroups) {
        pendingRewriteFileGroups.add(SimpleVersionedSerialization.writeVersionAndSerialize(
            RewriteFileGroup.Serializer.INSTANCE, rewriteFileGroup));
      }
    }
    pendingRewriteFileGroupsState.clear();
    pendingRewriteFileGroupsState.addAll(pendingRewriteFileGroups);

    // Setting checkpoint id to rewritten file groups which is rewritten in this checkpoint,
    // to prevent delete asynchronously rewritten file groups which is rewritten in next checkpoint.
    List<RewriteFileGroup> rewrittenFileGroups = rewrittenFileGroupsPerCheckpoint.remove(MAX_CHECKPOINT_ID);
    if (rewrittenFileGroups != null && !rewrittenFileGroups.isEmpty()) {
      rewrittenFileGroupsPerCheckpoint.put(checkpointId, rewrittenFileGroups);
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);

    // Delete rewritten file groups manifests after checkpoint to prevent lost manifest when restore from checkpoint.
    NavigableMap<Long, List<RewriteFileGroup>> pendingMap = rewrittenFileGroupsPerCheckpoint.headMap(
        checkpointId, true);
    pendingMap.values().forEach(fileGroups -> fileGroups.forEach(fileGroup -> {
      for (ManifestFile file : fileGroup.manifestFiles()) {
        try {
          table.io().deleteFile(file.path());
        } catch (Exception e) {
          LOG.warn("The file group {} has been rewritten, but we failed to clean the temporary manifests: {}",
              fileGroup, file.path(), e);
        }
      }
    }));
  }

  @Override
  public void processElement(StreamRecord<CommitResult> record) throws Exception {
    CommitResult committed = record.getValue();

    // Refresh the table to get the last committed snapshot of the commit result.
    table.refresh();
    Snapshot committedSnapshot = table.snapshot(committed.snapshotId());
    if (lastReceivedSnapshotId != DUMMY_SNAPSHOT_ID && lastReceivedSnapshotId != committedSnapshot.parentId()) {
      // Concurrent writer will commit other snapshot and make lastReceivedSnapshot and committedSnapshot discontinuous.
      // Concurrent writer maybe adding equality delete files which could delete any this job committed records
      // and should be applied to the data file we received before. Therefore, we need to emit all delete files
      // between (lastReceivedSnapshotId, committedSnapshotId) to avoid missing any equality delete files
      // when doing compaction.
      // See more details in https://github.com/apache/iceberg/pull/3323#issuecomment-962068331
      appendFilesWithin(lastReceivedSnapshotId, committedSnapshot.parentId(), flinkJobId);
    }

    LOG.info("Append files for snapshot id {} with flink job id {}", committed.snapshotId(), flinkJobId);
    Iterable<DataFile> dataFiles = Arrays.asList(committed.writeResult().dataFiles());
    Iterable<DeleteFile> deleteFiles = Arrays.asList(committed.writeResult().deleteFiles());
    appendFilesFor(committed.sequenceNumber(), committed.snapshotId(), dataFiles, deleteFiles);

    emitRewriteTask();

    lastReceivedSnapshotId = committed.snapshotId();
  }

  private void appendFilesWithin(long fromSnapshotId, long toSnapshotId, String jobId) throws IOException {
    LOG.info("Append files from snapshot id {} to snapshot id {} with flink job id {}",
            fromSnapshotId, toSnapshotId, jobId);
    List<Long> snapshotIds = SnapshotUtil.snapshotIdsBetween(table, fromSnapshotId, toSnapshotId);
    Collections.reverse(snapshotIds);  // emit file groups on sequence number
    for (Long snapshotId : snapshotIds) {
      Snapshot snapshot = table.snapshot(snapshotId);
      if (!VALIDATE_DATA_CHANGE_FILES_OPERATIONS.contains(snapshot.operation())) {
        continue;
      }

      // Only emit data files which are committed by the flink job of the job id.
      Iterable<DataFile> dataFiles = isCommittedByFlinkJob(snapshot, jobId) ?
          snapshot.addedFiles() : Collections.emptyList();

      // Emit all delete files which are committed by the flink job of the job id.
      // And only emit eq-delete files which are committed by others.
      Iterable<DeleteFile> deleteFiles = Iterables.filter(snapshot.addedDeleteFiles(),
          deleteFile -> isCommittedByFlinkJob(snapshot, jobId) || deleteFile.content() == FileContent.EQUALITY_DELETES);

      appendFilesFor(snapshot.sequenceNumber(), snapshotId, dataFiles, deleteFiles);
    }
  }

  private void appendFilesFor(long sequenceNumber, long snapshotId,
                              Iterable<DataFile> dataFiles, Iterable<DeleteFile> deleteFiles) throws IOException {
    Map<StructLikeWrapper, Collection<DataFile>> dataFileGroup = groupFilesByPartition(dataFiles);
    Map<StructLikeWrapper, Collection<DeleteFile>> deleteFileGroup = groupFilesByPartition(deleteFiles);

    Set<StructLikeWrapper> partitions = Sets.union(dataFileGroup.keySet(), deleteFileGroup.keySet());
    for (StructLikeWrapper partition : partitions) {
      Collection<DataFile> partitionDataFiles = dataFileGroup.getOrDefault(partition, Collections.emptyList());
      Collection<DeleteFile> partitionDeleteFiles = deleteFileGroup.getOrDefault(partition, Collections.emptyList());
      appendFiles(partition, sequenceNumber, snapshotId, partitionDataFiles, partitionDeleteFiles);
    }
  }

  private void appendFiles(StructLikeWrapper partition, long sequenceNumber, long snapshotId,
                           Collection<DataFile> dataFiles, Collection<DeleteFile> deleteFiles) throws IOException {
    Deque<RewriteFileGroup> bins = pendingFileGroupsByPartition.computeIfAbsent(partition, p -> Lists.newLinkedList());
    strategy.packTo(bins, partition.get(), sequenceNumber, snapshotId, dataFiles, deleteFiles);
  }

  private <F extends ContentFile<F>> Map<StructLikeWrapper, Collection<F>> groupFilesByPartition(Iterable<F> files) {
    Multimap<StructLikeWrapper, F> filesByPartition = Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (F file : files) {
      PartitionSpec spec = table.specs().get(file.specId());
      StructLikeWrapper partition = StructLikeWrapper.forType(spec.partitionType()).set(file.partition());
      filesByPartition.put(partition, file);
    }
    return filesByPartition.asMap();
  }

  private void emitRewriteTask() {
    Iterator<Deque<RewriteFileGroup>> iterator = pendingFileGroupsByPartition.values().iterator();
    while (iterator.hasNext()) {

      Deque<RewriteFileGroup> bins = iterator.next();
      for (RewriteFileGroup fileGroup : strategy.pickFrom(bins)) {

        Iterable<CombinedScanTask> tasks = strategy.planTasks(fileGroup);

        tasks.forEach(task -> emit(new RewriteTask(fileGroup.latestSnapshotId(), fileGroup.partition(), task)));

        rewrittenFileGroupsPerCheckpoint.computeIfAbsent(MAX_CHECKPOINT_ID, k -> Lists.newArrayList()).add(fileGroup);
      }

      if (bins.isEmpty() || bins.peek().isEmpty()) {
        iterator.remove();
      }
    }
  }

  private void emit(RewriteTask rewriteTask) {
    LOG.info("Emit rewrite task: {}.", rewriteTask);
    output.collect(new StreamRecord<>(rewriteTask));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private static long getLastCommittedSnapshotId(Table table, String flinkJobId) {
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null) {
      if (isCommittedByFlinkJob(snapshot, flinkJobId)) {
        return snapshot.snapshotId();
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return DUMMY_SNAPSHOT_ID;
  }

  private static int getCommitNumAfter(Table table, long snapshotId, String flinkJobId) {
    int counter = 0;
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null && snapshot.snapshotId() != snapshotId) {
      if (isCommittedByFlinkJob(snapshot, flinkJobId)) {
        counter++;
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return counter;
  }

  private static boolean isCommittedByFlinkJob(Snapshot snapshot, String flinkJobId) {
    String snapshotFlinkJobId = snapshot.summary().get(FLINK_JOB_ID);
    return snapshotFlinkJobId != null && snapshotFlinkJobId.equals(flinkJobId);
  }

  private class StreamingBinPackStrategy {

    private final long minFileSize;
    private final long maxFileSize;
    private final long targetFileSize;
    private final long writeMaxFileSize;
    private final int minGroupFiles;
    private final int maxGroupFiles;
    private final int maxWaitingCommits;

    private StreamingBinPackStrategy() {
      this.targetFileSize = PropertyUtil.propertyAsLong(table.properties(),
          FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES,
          PropertyUtil.propertyAsLong(table.properties(),
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT));

      this.minFileSize = PropertyUtil.propertyAsLong(table.properties(),
          FlinkSinkOptions.STREAMING_REWRITE_MIN_FILE_SIZE_BYTES,
          (long) (targetFileSize * FlinkSinkOptions.STREAMING_REWRITE_MIN_FILE_SIZE_DEFAULT_RATIO));

      this.maxFileSize = PropertyUtil.propertyAsLong(table.properties(),
          FlinkSinkOptions.STREAMING_REWRITE_MAX_FILE_SIZE_BYTES,
          (long) (targetFileSize * FlinkSinkOptions.STREAMING_REWRITE_MAX_FILE_SIZE_DEFAULT_RATIO));

      this.writeMaxFileSize = (long) (targetFileSize + ((maxFileSize - targetFileSize) * 0.5));

      this.minGroupFiles = PropertyUtil.propertyAsInt(table.properties(),
          FlinkSinkOptions.STREAMING_REWRITE_MIN_GROUP_FILES,
          FlinkSinkOptions.STREAMING_REWRITE_MIN_GROUP_FILES_DEFAULT);

      this.maxGroupFiles = PropertyUtil.propertyAsInt(table.properties(),
          FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES,
          FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES_DEFAULT);

      this.maxWaitingCommits = PropertyUtil.propertyAsInt(table.properties(),
          FlinkSinkOptions.STREAMING_REWRITE_MAX_WAITING_COMMITS,
          FlinkSinkOptions.STREAMING_REWRITE_MAX_WAITING_COMMITS_DEFAULT);

      validateOptions();
    }

    private boolean shouldBeRewritten(DataFile dataFile) {
      return dataFile.fileSizeInBytes() < minFileSize || dataFile.fileSizeInBytes() > maxFileSize;
    }

    private void packTo(Deque<RewriteFileGroup> bins, StructLike partition, long sequenceNumber, long snapshotId,
                        Collection<DataFile> dataFiles, Collection<DeleteFile> deleteFiles) throws IOException {

      RewriteFileGroup bin = bins.peekLast();
      if (bin == null) {
        bin = new RewriteFileGroup(partition);
        bins.addLast(bin);
      }

      long rewriteFilesSize = 0;
      List<DataFile> pendingFiles = Lists.newArrayList();
      for (DataFile dataFile : dataFiles) {
        // Only stat not reach target size data files which should be considered for rewriting
        // and keep rewrite file group size as multiple of target file size.
        rewriteFilesSize += shouldBeRewritten(dataFile) ? dataFile.fileSizeInBytes() : 0;
        pendingFiles.add(dataFile);

        // Rolling to a new rewrite file group to prevent file fragmentation.
        if (rewriteFilesSize + bin.rewriteFilesSize() >= targetFileSize ||
            pendingFiles.size() + bin.totalFilesCount() >= maxGroupFiles) {
          // Write all delete files to manifest to ensure delete files can be applied to previous data files.
          DeltaManifests deltaManifests = FlinkManifestUtil.writeExistingFiles(
              sequenceNumber, snapshotId, pendingFiles, deleteFiles,
              () -> manifestOutputFileFactory.createTmp(), table.spec()
          );
          bin.add(sequenceNumber, snapshotId, rewriteFilesSize, pendingFiles.size(), deltaManifests);

          bin = new RewriteFileGroup(partition);
          bins.add(bin);
          rewriteFilesSize = 0;
          pendingFiles.clear();
        }
      }

      // Append all delete files or remain files to current rewrite file group.
      if ((dataFiles.isEmpty() && !deleteFiles.isEmpty()) || !pendingFiles.isEmpty()) {
        DeltaManifests deltaManifests = FlinkManifestUtil.writeExistingFiles(
            sequenceNumber, snapshotId, pendingFiles, deleteFiles,
            () -> manifestOutputFileFactory.createTmp(), table.spec()
        );
        bin.add(sequenceNumber, snapshotId, rewriteFilesSize, pendingFiles.size(), deltaManifests);
      }
    }

    private List<RewriteFileGroup> pickFrom(Deque<RewriteFileGroup> bins) {
      List<RewriteFileGroup> picked = Lists.newArrayList();
      while (!bins.isEmpty() && canPick(bins.peek())) {
        picked.add(bins.poll());
      }
      return picked;
    }

    private boolean canPick(RewriteFileGroup bin) {
      return !bin.isEmpty() && (bin.rewriteFilesSize() >= targetFileSize || bin.totalFilesCount() >= maxGroupFiles ||
          getCommitNumAfter(table, bin.latestSnapshotId(), flinkJobId) > maxWaitingCommits);
    }

    private Iterable<CombinedScanTask> planTasks(RewriteFileGroup fileGroup) {
      CloseableIterable<FileScanTask> scanTasks = table.newScan()
          .useManifests(fileGroup.manifestFiles())
          .caseSensitive(caseSensitive)
          .ignoreResiduals()
          .planFiles();

      CloseableIterable<FileScanTask> filtered = CloseableIterable.withNoopClose(
          FluentIterable.from(scanTasks).filter(scanTask -> shouldBeRewritten(scanTask.file())));

      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(filtered, splitSize(totalSize(filtered)));
      return Iterables.filter(
          TableScanUtil.planTasks(splitFiles, writeMaxFileSize, 1, 0),
          task -> task.files().size() >= minGroupFiles || totalSize(task.files()) >= targetFileSize
      );
    }

    private long totalSize(Iterable<FileScanTask> tasks) {
      return FluentIterable.from(tasks).stream().mapToLong(FileScanTask::length).sum();
    }

    private long splitSize(long totalSizeInBytes) {
      if (totalSizeInBytes < targetFileSize) {
        return targetFileSize;
      }

      if (LongMath.mod(totalSizeInBytes, targetFileSize) > minFileSize) {
        return LongMath.divide(totalSizeInBytes, targetFileSize, RoundingMode.CEILING);
      }

      return totalSizeInBytes / LongMath.divide(totalSizeInBytes, targetFileSize, RoundingMode.FLOOR);
    }

    private void validateOptions() {
      Preconditions.checkArgument(minFileSize >= 0,
          "Cannot set %s to a negative number, %d < 0",
          FlinkSinkOptions.STREAMING_REWRITE_MIN_FILE_SIZE_BYTES, minFileSize);

      Preconditions.checkArgument(maxFileSize > minFileSize,
          "Cannot set %s greater than or equal to %s, %d >= %d",
          FlinkSinkOptions.STREAMING_REWRITE_MIN_FILE_SIZE_BYTES,
          FlinkSinkOptions.STREAMING_REWRITE_MAX_FILE_SIZE_BYTES, minFileSize, maxFileSize);

      Preconditions.checkArgument(targetFileSize > minFileSize,
          "Cannot set %s greater than or equal to %s, %d >= %d",
          FlinkSinkOptions.STREAMING_REWRITE_MIN_FILE_SIZE_BYTES,
          FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES, minFileSize, targetFileSize);

      Preconditions.checkArgument(targetFileSize < maxFileSize,
          "Cannot set %s is greater than or equal to %s, %d >= %d",
          FlinkSinkOptions.STREAMING_REWRITE_MAX_FILE_SIZE_BYTES,
          FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES, maxFileSize, targetFileSize);

      Preconditions.checkArgument(minGroupFiles > 0,
          "Cannot set %s to a negative number, %d < 0",
          FlinkSinkOptions.STREAMING_REWRITE_MIN_GROUP_FILES, minGroupFiles);

      Preconditions.checkArgument(maxGroupFiles > minGroupFiles,
          "Cannot set %s is greater than or equal to %s, %d >= %d",
          FlinkSinkOptions.STREAMING_REWRITE_MIN_GROUP_FILES,
          FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, minGroupFiles, maxGroupFiles);

      Preconditions.checkArgument(maxWaitingCommits > 0,
          "Cannot set %s to a negative number, %d < 0",
          FlinkSinkOptions.STREAMING_REWRITE_MAX_WAITING_COMMITS, maxWaitingCommits);
    }
  }
}
