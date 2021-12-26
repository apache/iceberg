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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCommittedFilesEmitter extends AbstractStreamOperator<PartitionFileGroup>
    implements OneInputStreamOperator<CommitResult, PartitionFileGroup>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long DUMMY_SNAPSHOT_ID = -1L;
  private static final Set<String> VALIDATE_DATA_CHANGE_FILES_OPERATIONS =
      ImmutableSet.of(DataOperations.APPEND, DataOperations.OVERWRITE, DataOperations.DELETE);

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCommittedFilesEmitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  private final TableLoader tableLoader;

  private transient Table table;
  private transient String flinkJobId;
  private transient long lastEmittedSnapshotId;

  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;

  private static final ListStateDescriptor<Long> EMITTED_SNAPSHOT_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-streaming-rewriter-emitted-snapshot-id", BasicTypeInfo.LONG_TYPE_INFO);
  private transient ListState<Long> emittedSnapshotIdState;


  public IcebergCommittedFilesEmitter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    this.lastEmittedSnapshotId = DUMMY_SNAPSHOT_ID;

    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    this.emittedSnapshotIdState = context.getOperatorStateStore().getListState(EMITTED_SNAPSHOT_ID_DESCRIPTOR);
    if (context.isRestored()) {
      String restoredFlinkJobId = jobIdState.get().iterator().next();
      Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      this.lastEmittedSnapshotId = emittedSnapshotIdState.get().iterator().next();
      Preconditions.checkState(lastEmittedSnapshotId > 0,
          "Last emitted snapshot id restore from checkpoint shouldn't be negative or 0");

      long lastCommittedSnapshotId = getLastCommittedSnapshotId(table, restoredFlinkJobId);
      if (SnapshotUtil.isAncestorOf(table, lastCommittedSnapshotId, lastEmittedSnapshotId)) {
        // Restore last emitted snapshot id from state will lose the last committed snapshot of restored flink job,
        // because the committer emit committed result after checkpoint so that the last emitted snapshot id
        // has not updated when checkpointing. Therefore, we need to emit all data and delete files
        // which are committed by the restored flink job and emit all eq-delete files which are committed by other
        // writer to catch up to the last committed snapshot of the restored flink job.
        emitPartitionFileGroupsWithin(lastEmittedSnapshotId, lastCommittedSnapshotId, restoredFlinkJobId);
        this.lastEmittedSnapshotId = lastCommittedSnapshotId;
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    emittedSnapshotIdState.clear();
    emittedSnapshotIdState.add(lastEmittedSnapshotId);
  }

  @Override
  public void processElement(StreamRecord<CommitResult> record) throws Exception {
    CommitResult committed = record.getValue();

    // Refresh the table to get the committed snapshot of the commit result.
    table.refresh();
    Snapshot committedSnapshot = table.snapshot(committed.snapshotId());
    if (lastEmittedSnapshotId != DUMMY_SNAPSHOT_ID && lastEmittedSnapshotId != committedSnapshot.parentId()) {
      // Concurrent writer will commit other snapshot and make lastEmittedSnapshot and committedSnapshot discontinuous.
      // Concurrent writer maybe adding equality delete files which could delete any this job committed records
      // and should be applied to the data file we emitted before. Therefore, we need to emit all delete files
      // between (lastEmittedSnapshotId, committedSnapshotId) to avoid missing any equality delete files
      // when doing compaction.
      //
      // See more details in https://github.com/apache/iceberg/pull/3323#issuecomment-962068331
      emitPartitionFileGroupsWithin(lastEmittedSnapshotId, committedSnapshot.parentId(), flinkJobId);
    }

    LOG.info("Emit partition file groups for snapshot id {} with flink job id {}", committed.snapshotId(), flinkJobId);
    Iterable<DataFile> dataFiles = Arrays.asList(committed.writeResult().dataFiles());
    Iterable<DeleteFile> deleteFiles = Arrays.asList(committed.writeResult().deleteFiles());
    emitPartitionFileGroups(committed.sequenceNumber(), committed.snapshotId(), dataFiles, deleteFiles);
    lastEmittedSnapshotId = committed.snapshotId();
  }

  private void emitPartitionFileGroupsWithin(long fromSnapshotId, long toSnapshotId, String jobId) {
    LOG.info("Emit partition file groups from snapshot id {} to snapshot id {} with flink job id {}",
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

      emitPartitionFileGroups(snapshot.sequenceNumber(), snapshotId, dataFiles, deleteFiles);
    }
  }

  private void emitPartitionFileGroups(long sequenceNumber, long snapshotId,
                                       Iterable<DataFile> dataFiles, Iterable<DeleteFile> deleteFiles) {
    Map<Pair<Integer, StructLikeWrapper>, Collection<DataFile>> dataFileGroup = groupFilesByPartition(dataFiles);
    Map<Pair<Integer, StructLikeWrapper>, Collection<DeleteFile>> deleteFileGroup = groupFilesByPartition(deleteFiles);

    Set<Pair<Integer, StructLikeWrapper>> partitions = Sets.union(dataFileGroup.keySet(), deleteFileGroup.keySet());
    for (Pair<Integer, StructLikeWrapper> partition : partitions) {
      PartitionFileGroup partitionFileGroup = PartitionFileGroup
          .builder(sequenceNumber, snapshotId, partition.first(), partition.second())
          .addDataFile(dataFileGroup.getOrDefault(partition, Collections.emptyList()))
          .addDeleteFile(deleteFileGroup.getOrDefault(partition, Collections.emptyList()))
          .build();
      emit(partitionFileGroup);
    }
  }

  private <F extends ContentFile<F>> Map<Pair<Integer, StructLikeWrapper>, Collection<F>> groupFilesByPartition(
      Iterable<F> files) {
    Multimap<Pair<Integer, StructLikeWrapper>, F> filesByPartition = Multimaps
        .newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (F file : files) {
      PartitionSpec spec = table.specs().get(file.specId());
      StructLikeWrapper partition = StructLikeWrapper.forType(spec.partitionType()).set(file.partition());
      filesByPartition.put(Pair.of(file.specId(), partition), file);
    }
    return filesByPartition.asMap();
  }

  private void emit(PartitionFileGroup fileGroup) {
    output.collect(new StreamRecord<>(fileGroup));
  }

  @Override
  public void endInput() throws Exception {
    if (lastEmittedSnapshotId != DUMMY_SNAPSHOT_ID) {
      long lastCommittedSnapshotId = getLastCommittedSnapshotId(table, flinkJobId);
      if (SnapshotUtil.isAncestorOf(table, lastCommittedSnapshotId, lastEmittedSnapshotId)) {
        emitPartitionFileGroupsWithin(lastEmittedSnapshotId, lastCommittedSnapshotId, flinkJobId);
        this.lastEmittedSnapshotId = lastCommittedSnapshotId;
      }
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

  private static boolean isCommittedByFlinkJob(Snapshot snapshot, String flinkJobId) {
    String snapshotFlinkJobId = snapshot.summary().get(FLINK_JOB_ID);
    return snapshotFlinkJobId != null && snapshotFlinkJobId.equals(flinkJobId);
  }

}
