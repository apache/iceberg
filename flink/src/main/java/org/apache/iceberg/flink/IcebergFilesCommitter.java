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

package org.apache.iceberg.flink;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<DataFile, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always increasing, so we could
  // correctly commit all the data files whose checkpoint id is greater than the max committed one to iceberg table, for
  // avoiding committing the same data files twice. This id will be attached to iceberg's meta when committing the
  // iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";

  private static final FlinkCatalogFactory CATALOG_FACTORY = new FlinkCatalogFactory();

  private final String fullTableName;
  private final SerializableConfiguration conf;
  private final ImmutableMap<String, String> options;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.
  private final NavigableMap<Long, List<DataFile>> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The data files cache for current checkpoint. Once the snapshot barrier received, it will be flushed to the
  // 'dataFilesPerCheckpoint'.
  private final List<DataFile> dataFilesOfCurrentCheckpoint = Lists.newArrayList();
  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient Table table;

  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, List<DataFile>>> STATE_DESCRIPTOR = buildStateDescriptor();

  private transient ListState<SortedMap<Long, List<DataFile>>> checkpointsState;

  IcebergFilesCommitter(String fullTableName, Map<String, String> options, Configuration conf) {
    this.fullTableName = fullTableName;
    this.options = ImmutableMap.copyOf(options);
    this.conf = new SerializableConfiguration(conf);
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    flinkJobId = getContainingTask().getEnvironment().getJobID().toHexString();
    Catalog icebergCatalog = CATALOG_FACTORY.buildIcebergCatalog(fullTableName, options, conf.get());

    table = icebergCatalog.loadTable(TableIdentifier.parse(fullTableName));

    checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    if (context.isRestored()) {
      long maxCommittedCheckpointId = getMaxCommittedCheckpointId(table, flinkJobId);
      // In the restoring path, it should have one valid snapshot for current flink job at least, so the max committed
      // checkpoint id should be positive. If it's not positive, that means someone might have removed or expired the
      // iceberg snapshot, in that case we should throw an exception in case of committing duplicated data files into
      // the iceberg table.
      Preconditions.checkState(maxCommittedCheckpointId != INITIAL_CHECKPOINT_ID,
          "There should be an existing iceberg snapshot for current flink job: %s", flinkJobId);

      SortedMap<Long, List<DataFile>> restoredDataFiles = checkpointsState.get().iterator().next();
      // Only keep the uncommitted data files in the cache.
      dataFilesPerCheckpoint.putAll(restoredDataFiles.tailMap(maxCommittedCheckpointId + 1));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush snapshot state to state backend, table: {}, checkpointId: {}", table, checkpointId);

    // Update the checkpoint state.
    dataFilesPerCheckpoint.put(checkpointId, ImmutableList.copyOf(dataFilesOfCurrentCheckpoint));

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    // Clear the local buffer for current checkpoint.
    dataFilesOfCurrentCheckpoint.clear();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    commitUpToCheckpoint(checkpointId);
  }

  private void commitUpToCheckpoint(long checkpointId) {
    NavigableMap<Long, List<DataFile>> pendingFileMap = dataFilesPerCheckpoint.headMap(checkpointId, true);

    List<DataFile> pendingDataFiles = Lists.newArrayList();
    for (List<DataFile> dataFiles : pendingFileMap.values()) {
      pendingDataFiles.addAll(dataFiles);
    }

    if (!pendingDataFiles.isEmpty()) {
      AppendFiles appendFiles = table.newAppend();
      pendingDataFiles.forEach(appendFiles::appendFile);
      appendFiles.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
      appendFiles.set(FLINK_JOB_ID, flinkJobId);
      appendFiles.commit();
    }

    // Clear the committed data files from dataFilesPerCheckpoint.
    pendingFileMap.clear();
  }

  @Override
  public void processElement(StreamRecord<DataFile> element) {
    this.dataFilesOfCurrentCheckpoint.add(element.getValue());
  }

  @Override
  public void endInput() {
    commitUpToCheckpoint(Long.MAX_VALUE);
  }

  private static ListStateDescriptor<SortedMap<Long, List<DataFile>>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a ListTypeInfo.
    ListTypeInfo<DataFile> dataFileListTypeInfo = new ListTypeInfo<>(TypeInformation.of(DataFile.class));
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, List<DataFile>> sortedMapTypeInfo = new SortedMapTypeInfo<>(
        BasicTypeInfo.LONG_TYPE_INFO, dataFileListTypeInfo, longComparator
    );
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }

  static Long getMaxCommittedCheckpointId(Table table, String flinkJobId) {
    Snapshot snapshot = table.currentSnapshot();
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)) {
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
}
