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
  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed.checkpoint.id";

  private static final FlinkCatalogFactory CATALOG_FACTORY = new FlinkCatalogFactory();

  private final String path;
  private final SerializableConfiguration conf;
  private final ImmutableMap<String, String> options;

  private transient long maxCommittedCheckpointId;
  private transient NavigableMap<Long, List<DataFile>> dataFilesPerCheckpoint;
  private transient List<DataFile> dataFilesOfCurrentCheckpoint;
  private transient Table table;

  // State for all checkpoints;
  private static final ListStateDescriptor<byte[]> STATE_DESCRIPTOR =
      new ListStateDescriptor<>("checkpoints-state", BytePrimitiveArraySerializer.INSTANCE);
  private transient ListState<byte[]> checkpointsState;

  IcebergFilesCommitter(String path, Map<String, String> options, Configuration conf) {
    this.path = path;
    this.options = ImmutableMap.copyOf(options);
    this.conf = new SerializableConfiguration(conf);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    Catalog icebergCatalog = CATALOG_FACTORY.buildIcebergCatalog(path, options, conf.get());
    table = icebergCatalog.loadTable(TableIdentifier.parse(path));
    maxCommittedCheckpointId = parseMaxCommittedCheckpointId(table.currentSnapshot());

    dataFilesPerCheckpoint = Maps.newTreeMap();
    dataFilesOfCurrentCheckpoint = Lists.newArrayList();

    checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    if (context.isRestored()) {
      dataFilesPerCheckpoint = deserializeState(checkpointsState.get().iterator().next());
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

  static Long parseMaxCommittedCheckpointId(Snapshot snapshot) {
    if (snapshot != null && snapshot.summary() != null) {
      String value = snapshot.summary().get(MAX_COMMITTED_CHECKPOINT_ID);
      if (value != null) {
        return Long.parseLong(value);
      }
    }
    return -1L;
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
