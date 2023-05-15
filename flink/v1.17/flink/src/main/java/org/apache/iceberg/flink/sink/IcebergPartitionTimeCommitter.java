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
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.PartitionedWriteResult;
import org.apache.iceberg.flink.util.PartitionCommitTriggerUtils;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;

/**
 * The committer that commits data files to the partitioned table. When the committer’s watermark
 * exceeds the partition time corresponding to the newly added data, these data are committed to the
 * table. Uncommitted data files are stored in the state.
 */
class IcebergPartitionTimeCommitter extends IcebergFilesCommitter<PartitionedWriteResult> {
  private static final long serialVersionUID = 1L;
  protected static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  protected static final String FLINK_WATERMARK = "flink.watermark";

  private transient long lastWatermark;

  private final NavigableMap<Long, Map<PartitionKey, byte[]>> dataFilesPerCheckpoint =
      Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<PartitionKey, List<PartitionedWriteResult>> writeResultsOfCurrentCkpt =
      Maps.newHashMap();

  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, Map<PartitionKey, byte[]>>>
      STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<SortedMap<Long, Map<PartitionKey, byte[]>>> checkpointsState;

  private transient List<PartitionCommitPolicy> policies;

  private transient long currentWatermark;
  private final Duration commitDelay;
  private final String watermarkZoneId;
  private final String extractorPattern;
  private final String formatterPattern;
  private final String policyKind;
  private final String policyClass;
  private final String successFileName;

  private final Set<PartitionKey> pendingCommitPartitionKeys = Sets.newHashSet();

  IcebergPartitionTimeCommitter(
      TableLoader tableLoader,
      boolean replacePartitions,
      Map<String, String> snapshotProperties,
      Integer workerPoolSize,
      String branch,
      Duration commitDelay,
      String watermarkZoneId,
      String extractorPattern,
      String formatterPattern,
      String policyKind,
      String policyClass,
      String successFileName) {
    super(tableLoader, replacePartitions, snapshotProperties, workerPoolSize, branch);

    this.commitDelay = commitDelay;
    this.watermarkZoneId = watermarkZoneId;
    this.extractorPattern = extractorPattern;
    this.formatterPattern = formatterPattern;
    this.policyKind = policyKind;
    this.policyClass = policyClass;
    this.successFileName = successFileName;
  }

  @Override
  void initCheckpointState(StateInitializationContext context) throws Exception {
    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);

    PartitionCommitPolicyFactory partitionCommitPolicyFactory =
        new PartitionCommitPolicyFactory(policyKind, policyClass, successFileName);

    this.policies = partitionCommitPolicyFactory.createPolicyChain(getUserCodeClassloader());
  }

  @Override
  void commitUncommittedDataFiles(String jobId, long checkpointId) throws Exception {

    this.lastWatermark =
        getMaxCommittedSummaryValue(table(), jobId, operatorId(), branch(), FLINK_WATERMARK);

    NavigableMap<Long, Map<PartitionKey, byte[]>> uncommittedDataFiles =
        Maps.newTreeMap(checkpointsState.get().iterator().next());

    uncommittedDataFiles.forEach(
        (id, dataFiles) ->
            dataFiles
                .entrySet()
                .removeIf(
                    e ->
                        PartitionCommitTriggerUtils.isPartitionCommittable(
                            lastWatermark,
                            e.getKey(),
                            commitDelay,
                            watermarkZoneId,
                            extractorPattern,
                            formatterPattern)));

    if (!uncommittedDataFiles.isEmpty()) {
      // Committed all uncommitted data files from the old flink job to iceberg table.
      long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
      commitUpToCheckpoint(uncommittedDataFiles, jobId, operatorId(), maxUncommittedCheckpointId);
    }

    if (!uncommittedDataFiles.isEmpty()) {
      dataFilesPerCheckpoint.putAll(uncommittedDataFiles);
    }
  }

  @Override
  void snapshotState(long checkpointId) throws Exception {
    // Update the checkpoint state.
    long startNano = System.nanoTime();
    Map<PartitionKey, byte[]> result = writeToManifest(checkpointId);
    if (result != null) {
      dataFilesPerCheckpoint.put(checkpointId, result);
    }

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    // Clear the local buffer for current checkpoint.
    writeResultsOfCurrentCkpt.clear();
    committerMetrics()
        .checkpointDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
  }

  @Override
  void commitUpToCheckpoint(long checkpointId) throws IOException {
    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId(), operatorId(), checkpointId);
  }

  private void commitUpToCheckpoint(
      NavigableMap<Long, Map<PartitionKey, byte[]>> deltaManifestsMap,
      String newFlinkJobId,
      String operatorId,
      long checkpointId)
      throws IOException {
    NavigableMap<Long, Map<PartitionKey, byte[]>> pendingMap =
        deltaManifestsMap.headMap(checkpointId, true);
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();

    for (Map.Entry<Long, Map<PartitionKey, byte[]>> pendingManifestPreCheckpoint :
        pendingMap.entrySet()) {

      PartitionedWriteResult.PartitionWriteResultBuilder builder =
          PartitionedWriteResult.partitionWriteResultBuilder();

      Map<PartitionKey, byte[]> pendingManifest = pendingManifestPreCheckpoint.getValue();

      Iterator<Map.Entry<PartitionKey, byte[]>> iterator = pendingManifest.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<PartitionKey, byte[]> partitionedManifests = iterator.next();
        if (Arrays.equals(EMPTY_MANIFEST_DATA, partitionedManifests.getValue())) {
          // Skip the empty flink manifest.
          continue;
        }

        DeltaManifests deltaManifests =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                DeltaManifestsSerializer.INSTANCE, partitionedManifests.getValue());

        PartitionedWriteResult writeResult =
            FlinkManifestUtil.readPartitionedCompletedFiles(deltaManifests, table().io());

        if (PartitionCommitTriggerUtils.isPartitionCommittable(
            currentWatermark,
            partitionedManifests.getKey(),
            commitDelay,
            watermarkZoneId,
            extractorPattern,
            formatterPattern)) {
          builder.add(writeResult);
          manifests.addAll(deltaManifests.manifests());
          pendingCommitPartitionKeys.add(writeResult.partitionKey());
          iterator.remove();
        }
      }

      WriteResult result = builder.build();
      if (result.dataFiles().length > 0
          || result.deleteFiles().length > 0
          || result.referencedDataFiles().length > 0) {
        pendingResults.put(pendingManifestPreCheckpoint.getKey(), result);
      }
    }

    if (pendingResults.isEmpty()) {
      return;
    }

    CommitSummary summary = new CommitSummary(pendingResults);
    commitPendingResult(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
    committerMetrics().updateCommitSummary(summary);
    pendingMap.entrySet().removeIf(longSortedMapEntry -> longSortedMapEntry.getValue().isEmpty());

    deleteCommittedManifests(manifests, newFlinkJobId, checkpointId);
  }

  @Override
  public void endInput(long checkpointId) throws IOException {
    this.currentWatermark = Watermark.MAX_WATERMARK.getTimestamp();
    Map<PartitionKey, byte[]> result = writeToManifest(checkpointId);
    if (result != null) {
      dataFilesPerCheckpoint.put(checkpointId, result);
    }

    writeResultsOfCurrentCkpt.clear();
    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId(), operatorId(), checkpointId);
  }

  @Override
  void committerOperation(SnapshotUpdate<?> operation) {
    operation.commit(); // abort is automatically called if this fails.

    Iterator<PartitionKey> iterator = pendingCommitPartitionKeys.iterator();
    while (iterator.hasNext()) {
      PartitionKey partitionKey = iterator.next();
      if (PartitionCommitTriggerUtils.isPartitionCommittable(
          currentWatermark,
          partitionKey,
          commitDelay,
          watermarkZoneId,
          extractorPattern,
          formatterPattern)) {

        for (PartitionCommitPolicy policy : policies) {
          policy.commit(table(), partitionKey);
        }
        iterator.remove();
      }
    }
  }

  @Override
  void prepareOperation(SnapshotUpdate<?> operation) {
    operation.set(FLINK_WATERMARK, String.valueOf(currentWatermark));
  }

  @Override
  public void processElement(StreamRecord<PartitionedWriteResult> element) {
    this.writeResultsOfCurrentCkpt.compute(
        element.getValue().partitionKey(),
        (key, value) -> {
          if (value == null) {
            return Lists.newArrayList(element.getValue());
          } else {
            value.add(element.getValue());
            return value;
          }
        });
  }

  @Override
  public void processWatermark(Watermark mark) {
    this.currentWatermark = mark.getTimestamp();
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  private Map<PartitionKey, byte[]> writeToManifest(long checkpointId) throws IOException {
    if (writeResultsOfCurrentCkpt.isEmpty()) {
      return null;
    }

    Iterator<Map.Entry<PartitionKey, List<PartitionedWriteResult>>> iterator =
        writeResultsOfCurrentCkpt.entrySet().iterator();

    Map<PartitionKey, byte[]> result = Maps.newHashMap();

    while (iterator.hasNext()) {
      Map.Entry<PartitionKey, List<PartitionedWriteResult>> next = iterator.next();

      PartitionedWriteResult.PartitionWriteResultBuilder builder =
          PartitionedWriteResult.partitionWriteResultBuilder();

      if (dataFilesPerCheckpoint.containsKey(checkpointId)) {
        Map<PartitionKey, byte[]> longSortedMap = dataFilesPerCheckpoint.get(checkpointId);
        addExistWriteResult(next.getKey(), builder, longSortedMap);
      }

      PartitionedWriteResult writeResult = builder.addAll(next.getValue()).build();
      DeltaManifests deltaManifests =
          FlinkManifestUtil.writePartitionedCompletedFiles(
              writeResult, () -> manifestOutputFileFactory().create(checkpointId), table().spec());

      byte[] bytes =
          SimpleVersionedSerialization.writeVersionAndSerialize(
              DeltaManifestsSerializer.INSTANCE, deltaManifests);

      result.put(next.getKey(), bytes);
    }

    if (result.size() > 0) {
      return result;
    }

    return null;
  }

  private void addExistWriteResult(
      PartitionKey partitionkey,
      PartitionedWriteResult.PartitionWriteResultBuilder builder,
      Map<PartitionKey, byte[]> longSortedMap)
      throws IOException {
    if (longSortedMap.containsKey(partitionkey)) {
      DeltaManifests preDeltaManifests =
          SimpleVersionedSerialization.readVersionAndDeSerialize(
              DeltaManifestsSerializer.INSTANCE, longSortedMap.get(partitionkey));

      PartitionedWriteResult preWriteResult =
          FlinkManifestUtil.readPartitionedCompletedFiles(preDeltaManifests, table().io());

      for (ManifestFile manifest : preDeltaManifests.manifests()) {
        table().io().deleteFile(manifest.path());
      }

      builder.add(preWriteResult);
    }
  }

  private static ListStateDescriptor<SortedMap<Long, Map<PartitionKey, byte[]>>>
      buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, Map<PartitionKey, byte[]>> sortedMapTypeInfo =
        new SortedMapTypeInfo<>(
            BasicTypeInfo.LONG_TYPE_INFO,
            new MapTypeInfo<>(
                TypeInformation.of(PartitionKey.class),
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
            longComparator);
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }
}
