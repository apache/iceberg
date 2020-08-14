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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergFilesCommitter extends RichSinkFunction<DataFile> implements
    CheckpointListener, CheckpointedFunction {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed.checkpoint.id";

  private static final FlinkCatalogFactory CATALOG_FACTORY = new FlinkCatalogFactory();

  private final String fullTableName;
  private final SerializableConfiguration conf;
  private final ImmutableMap<String, String> options;

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always increasing, so we could
  // correctly commit all the data files whose checkpoint id is greater than the max committed one to iceberg table, for
  // avoiding committing the same data files twice. This id will be attached to iceberg's meta when committing the
  // iceberg transaction.
  private transient long maxCommittedCheckpointId;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.
  private final NavigableMap<Long, List<DataFile>> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The data files cache for current checkpoint. Once the snapshot barrier received, it will be flushed to the
  // `dataFilesPerCheckpoint`.
  private final List<DataFile> dataFilesOfCurrentCheckpoint = Lists.newArrayList();
  private transient Table table;

  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<byte[]> STATE_DESCRIPTOR =
      new ListStateDescriptor<>("checkpoints-state", BytePrimitiveArraySerializer.INSTANCE);
  private transient ListState<byte[]> checkpointsState;

  IcebergFilesCommitter(String fullTableName, Map<String, String> options, Configuration conf) {
    this.fullTableName = fullTableName;
    this.options = ImmutableMap.copyOf(options);
    this.conf = new SerializableConfiguration(conf);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    Catalog icebergCatalog = CATALOG_FACTORY.buildIcebergCatalog(fullTableName, options, conf.get());

    table = icebergCatalog.loadTable(TableIdentifier.parse(fullTableName));
    maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    if (context.isRestored()) {
      maxCommittedCheckpointId = getMaxCommittedCheckpointId(table.currentSnapshot());
      dataFilesPerCheckpoint.putAll(deserializeState(checkpointsState.get().iterator().next()));
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush snapshot state to state backend, table: {}, checkpointId: {}", table, checkpointId);

    // Update the checkpoint state.
    dataFilesPerCheckpoint.put(checkpointId, ImmutableList.copyOf(dataFilesOfCurrentCheckpoint));

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.addAll(ImmutableList.of(serializeState(dataFilesPerCheckpoint)));

    // Clear the local buffer for current checkpoint.
    dataFilesOfCurrentCheckpoint.clear();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    NavigableMap<Long, List<DataFile>> pendingFileMap = dataFilesPerCheckpoint.tailMap(maxCommittedCheckpointId, false);

    List<DataFile> pendingDataFiles = Lists.newArrayList();
    for (List<DataFile> dataFiles : pendingFileMap.values()) {
      pendingDataFiles.addAll(dataFiles);
    }

    if (!pendingDataFiles.isEmpty()) {
      AppendFiles appendFiles = table.newAppend();
      pendingDataFiles.forEach(appendFiles::appendFile);
      appendFiles.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
      appendFiles.commit();

      maxCommittedCheckpointId = checkpointId;
    }

    // Clear the committed data files from dataFilesPerCheckpoint.
    pendingFileMap.clear();
  }

  @Override
  public void invoke(DataFile value, Context context) {
    this.dataFilesOfCurrentCheckpoint.add(value);
  }

  static Long getMaxCommittedCheckpointId(Snapshot snapshot) {
    if (snapshot != null && snapshot.summary() != null) {
      String value = snapshot.summary().get(MAX_COMMITTED_CHECKPOINT_ID);
      if (value != null) {
        return Long.parseLong(value);
      }
    }
    return INITIAL_CHECKPOINT_ID;
  }

  private static byte[] serializeState(Map<Long, List<DataFile>> dataFiles) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(dataFiles);
      return bos.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  private static NavigableMap<Long, List<DataFile>> deserializeState(byte[] data)
      throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
         ObjectInputStream in = new ObjectInputStream(bis)) {
      return (NavigableMap<Long, List<DataFile>>) in.readObject();
    }
  }
}
