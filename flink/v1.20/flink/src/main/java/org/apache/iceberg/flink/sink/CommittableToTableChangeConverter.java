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
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommittableToTableChangeConverter
    extends ProcessFunction<CommittableMessage<IcebergCommittable>, TableChange>
    implements CheckpointedFunction, CheckpointListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommittableToTableChangeConverter.class);

  private final TableLoader tableLoader;
  private transient FileIO io;
  private transient String tableName;
  private transient Map<Integer, PartitionSpec> specs;
  private transient ListState<ManifestFile> manifestFilesToRemoveState;
  private transient List<ManifestFile> manifestFilesToRemoveList;
  private transient long lastCompletedCheckpointId = -1L;
  private transient String flinkJobId;

  public CommittableToTableChangeConverter(TableLoader tableLoader) {
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.manifestFilesToRemoveList = Lists.newArrayList();
    this.manifestFilesToRemoveState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("manifests-to-remove", ManifestFile.class));
    if (context.isRestored()) {
      manifestFilesToRemoveList = Lists.newArrayList(manifestFilesToRemoveState.get());
    }
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    this.flinkJobId = getRuntimeContext().getJobId().toString();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    Table table = tableLoader.loadTable();
    this.io = table.io();
    this.specs = table.specs();
    this.tableName = table.name();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    manifestFilesToRemoveState.update(manifestFilesToRemoveList);
  }

  @Override
  public void processElement(
      CommittableMessage<IcebergCommittable> value,
      ProcessFunction<CommittableMessage<IcebergCommittable>, TableChange>.Context ctx,
      Collector<TableChange> out)
      throws Exception {
    if (value instanceof CommittableWithLineage) {
      CommittableWithLineage<IcebergCommittable> committable =
          (CommittableWithLineage<IcebergCommittable>) value;
      TableChange tableChange = convertToTableChange(committable.getCommittable());
      out.collect(tableChange);
    } else {
      LOG.warn("Unsupported type of committable message: {}", value.getClass());
    }
  }

  private TableChange convertToTableChange(IcebergCommittable icebergCommittable)
      throws IOException {
    if (icebergCommittable == null || icebergCommittable.manifest().length == 0) {
      return TableChange.empty();
    }

    DeltaManifests deltaManifests =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, icebergCommittable.manifest());
    WriteResult writeResult = FlinkManifestUtil.readCompletedFiles(deltaManifests, io, specs);
    manifestFilesToRemoveList.addAll(deltaManifests.manifests());

    TableChange tableChange =
        new TableChange(
            List.of(writeResult.dataFiles()), List.of(writeResult.deleteFiles()), false);
    return tableChange;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (checkpointId > lastCompletedCheckpointId) {
      this.lastCompletedCheckpointId = checkpointId;
      FlinkManifestUtil.deleteCommittedManifests(
          tableName, io, manifestFilesToRemoveList, flinkJobId, checkpointId);
      manifestFilesToRemoveList.clear();
      manifestFilesToRemoveState.clear();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tableLoader.isOpen()) {
      tableLoader.close();
    }

    io = null;
    specs = null;
  }
}
