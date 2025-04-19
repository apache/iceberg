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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommittableToTableChangeConverter extends AbstractStreamOperator<TableChange>
    implements OneInputStreamOperator<CommittableMessage<IcebergCommittable>, TableChange> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommittableToTableChangeConverter.class);

  private final TableLoader tableLoader;
  private Table table;
  private transient ListState<ManifestFile> toRemoveManifestFileState;
  private transient List<ManifestFile> toRemoveManifestFileList;
  private transient long lastCompletedCheckpointId = -1L;
  private transient String flinkJobId;

  public CommittableToTableChangeConverter(TableLoader tableLoader) {
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.toRemoveManifestFileList = Lists.newArrayList();
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.toRemoveManifestFileState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("to-remove-manifest", ManifestFile.class));
    if (context.isRestored()) {
      toRemoveManifestFileList = Lists.newArrayList(toRemoveManifestFileState.get());
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    toRemoveManifestFileState.update(toRemoveManifestFileList);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<IcebergCommittable>> record)
      throws Exception {
    if (record.getValue() instanceof CommittableWithLineage) {
      CommittableWithLineage<IcebergCommittable> committable =
          (CommittableWithLineage<IcebergCommittable>) record.getValue();
      TableChange tableChange = convertToTableChange(committable.getCommittable());
      output.collect(new StreamRecord<>(tableChange));
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
    WriteResult writeResult =
        FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs());
    toRemoveManifestFileList.addAll(deltaManifests.manifests());

    int dataFileCount = 0;
    long dataFileSizeInBytes = 0L;
    for (DataFile dataFile : writeResult.dataFiles()) {
      dataFileCount++;
      dataFileSizeInBytes += dataFile.fileSizeInBytes();
    }

    int posDeleteFileCount = 0;
    long posDeleteRecordCount = 0L;
    int eqDeleteFileCount = 0;
    long eqDeleteRecordCount = 0L;

    for (DeleteFile deleteFile : writeResult.deleteFiles()) {
      switch (deleteFile.content()) {
        case POSITION_DELETES:
          posDeleteFileCount++;
          posDeleteRecordCount += deleteFile.recordCount();
          break;
        case EQUALITY_DELETES:
          eqDeleteFileCount++;
          eqDeleteRecordCount += deleteFile.recordCount();
          break;
        default:
          // Unexpected delete file types don't impact compaction task statistics, so ignore.
          LOG.info("Unexpected delete file content:{}", deleteFile.content());
      }
    }

    TableChange tableChange =
        TableChange.builder()
            .dataFileCount(dataFileCount)
            .dataFileSizeInBytes(dataFileSizeInBytes)
            .posDeleteFileCount(posDeleteFileCount)
            .posDeleteRecordCount(posDeleteRecordCount)
            .eqDeleteRecordCount(eqDeleteRecordCount)
            .eqDeleteFileCount(eqDeleteFileCount)
            .commitCount(1)
            .build();

    return tableChange;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    if (checkpointId > lastCompletedCheckpointId) {
      this.lastCompletedCheckpointId = checkpointId;
      FlinkManifestUtil.deleteCommittedManifests(
          table, toRemoveManifestFileList, flinkJobId, checkpointId);
      toRemoveManifestFileList.clear();
      toRemoveManifestFileState.clear();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tableLoader.isOpen()) {
      tableLoader.close();
    }
  }
}
