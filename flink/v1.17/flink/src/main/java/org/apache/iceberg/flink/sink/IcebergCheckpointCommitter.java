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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;

class IcebergCheckpointCommitter extends IcebergFilesCommitter<WriteResult> {
  private static final long serialVersionUID = 1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not
  // been committed to iceberg table). We need a sorted map here because there's possible that few
  // checkpoints snapshot failed, for example: the 1st checkpoint have 2 data files <1, <file0,
  // file1>>, the 2st checkpoint have 1 data files <2, <file3>>. Snapshot for checkpoint#1
  // interrupted because of network/disk failure etc, while we don't expect any data loss in iceberg
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final List<WriteResult> writeResultsOfCurrentCkpt = Lists.newArrayList();

  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, byte[]>> STATE_DESCRIPTOR =
      buildStateDescriptor();
  private transient ListState<SortedMap<Long, byte[]>> checkpointsState;

  IcebergCheckpointCommitter(
      TableLoader tableLoader,
      boolean replacePartitions,
      Map<String, String> snapshotProperties,
      Integer workerPoolSize,
      String branch) {
    super(tableLoader, replacePartitions, snapshotProperties, workerPoolSize, branch);
  }

  @Override
  void initCheckpointState(StateInitializationContext context) throws Exception {
    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
  }

  @Override
  void commitUncommittedDataFiles(String jobId, long checkpointId) throws Exception {
    NavigableMap<Long, byte[]> uncommittedDataFiles =
        Maps.newTreeMap(checkpointsState.get().iterator().next()).tailMap(checkpointId, false);
    if (!uncommittedDataFiles.isEmpty()) {
      // Committed all uncommitted data files from the old flink job to iceberg table.
      long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
      commitUpToCheckpoint(uncommittedDataFiles, jobId, operatorId(), maxUncommittedCheckpointId);
    }
  }

  @Override
  void snapshotState(long checkpointId) throws Exception {
    // Update the checkpoint state.
    dataFilesPerCheckpoint.put(checkpointId, writeToManifest(checkpointId));

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    // Clear the local buffer for current checkpoint.
    writeResultsOfCurrentCkpt.clear();
  }

  @Override
  void commitUpToCheckpoint(long checkpointId) throws IOException {
    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId(), operatorId(), checkpointId);
  }

  private void commitUpToCheckpoint(
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
          e.getKey(), FlinkManifestUtil.readCompletedFiles(deltaManifests, table().io()));
      manifests.addAll(deltaManifests.manifests());
    }

    CommitSummary summary = new CommitSummary(pendingResults);
    commitPendingResult(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
    committerMetrics().updateCommitSummary(summary);
    pendingMap.clear();
    deleteCommittedManifests(manifests, newFlinkJobId, checkpointId);
  }

  @Override
  void committerOperation(SnapshotUpdate<?> operation) {
    operation.commit(); // abort is automatically called if this fails.
  }

  @Override
  void prepareOperation(SnapshotUpdate<?> operation) {}

  @Override
  public void processElement(StreamRecord<WriteResult> element) {
    this.writeResultsOfCurrentCkpt.add(element.getValue());
  }

  @Override
  public void endInput(long checkpointId) throws IOException {
    dataFilesPerCheckpoint.put(checkpointId, writeToManifest(checkpointId));
    writeResultsOfCurrentCkpt.clear();

    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId(), operatorId(), checkpointId);
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId) throws IOException {
    if (writeResultsOfCurrentCkpt.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResultsOfCurrentCkpt).build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result, () -> manifestOutputFileFactory().create(checkpointId), table().spec());

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  private static ListStateDescriptor<SortedMap<Long, byte[]>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, byte[]> sortedMapTypeInfo =
        new SortedMapTypeInfo<>(
            BasicTypeInfo.LONG_TYPE_INFO,
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
            longComparator);
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }
}
