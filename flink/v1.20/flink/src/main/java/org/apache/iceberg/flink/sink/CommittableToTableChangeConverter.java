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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommittableToTableChangeConverter
    extends ProcessFunction<CommittableMessage<IcebergCommittable>, TableChange>
    implements CheckpointedFunction {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommittableToTableChangeConverter.class);

  private FileIO io;
  private String tableName;
  private Map<Integer, PartitionSpec> specs;
  private transient ListState<Tuple2<Long, String>> manifestFilesCommitedState;
  private transient Set<String> manifestFilesCommitedSet;
  private transient NavigableMap<Long, List<String>> commitRequestMap;
  private transient List<Tuple2<Long, String>> manifestFilesCommitedList;
  private transient String flinkJobId;
  // Maximum number of manifests to be committed at a time.
  // It is hardcoded for now, we can revisit in the future if config is needed.
  private int maxSize = 1000;

  public CommittableToTableChangeConverter(
      FileIO fileIO, String tableName, Map<Integer, PartitionSpec> specs) {
    Preconditions.checkNotNull(fileIO, "FileIO should not be null");
    Preconditions.checkNotNull(tableName, "TableName should not be null");
    Preconditions.checkNotNull(specs, "Specs should not be null");
    this.io = fileIO;
    this.tableName = tableName;
    this.specs = specs;
  }

  @VisibleForTesting
  CommittableToTableChangeConverter(
      FileIO fileIO, String tableName, Map<Integer, PartitionSpec> specs, int maxSize) {
    Preconditions.checkNotNull(fileIO, "FileIO should not be null");
    Preconditions.checkNotNull(tableName, "TableName should not be null");
    Preconditions.checkNotNull(specs, "Specs should not be null");
    this.io = fileIO;
    this.tableName = tableName;
    this.specs = specs;
    this.maxSize = maxSize;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.manifestFilesCommitedSet = Sets.newHashSet();
    this.manifestFilesCommitedList = Lists.newArrayList();
    this.commitRequestMap = Maps.newTreeMap();
    this.manifestFilesCommitedState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "manifests-commited", TypeInformation.of(new TypeHint<>() {})));
    if (context.isRestored()) {
      for (Tuple2<Long, String> checkPointIdAndManifestTuple : manifestFilesCommitedState.get()) {
        manifestFilesCommitedSet.add(checkPointIdAndManifestTuple.f1);
        manifestFilesCommitedList.add(checkPointIdAndManifestTuple);
        manifestFilesCommitedList.forEach(
            tuple ->
                commitRequestMap
                    .computeIfAbsent(tuple.f0, k -> Lists.newArrayList())
                    .add(tuple.f1));
      }
    }
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    Preconditions.checkState(
        getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks() == 1,
        "CommittableToTableChangeConverter must run with parallelism 1, current parallelism: %s",
        getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());

    this.flinkJobId = getRuntimeContext().getJobId().toString();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    manifestFilesCommitedState.clear();
    manifestFilesCommitedState.addAll(manifestFilesCommitedList);
  }

  @Override
  public void processElement(
      CommittableMessage<IcebergCommittable> value,
      ProcessFunction<CommittableMessage<IcebergCommittable>, TableChange>.Context ctx,
      Collector<TableChange> out)
      throws Exception {
    if (value instanceof CommittableWithLineage) {
      IcebergCommittable committable =
          ((CommittableWithLineage<IcebergCommittable>) value).getCommittable();

      if (committable == null || committable.manifest().length == 0) {
        return;
      }

      DeltaManifests deltaManifests =
          SimpleVersionedSerialization.readVersionAndDeSerialize(
              DeltaManifestsSerializer.INSTANCE, committable.manifest());

      List<DataFile> dataFiles = Lists.newArrayList();
      List<DeleteFile> deleteFiles = Lists.newArrayList();

      ManifestFile dataManifest = deltaManifests.dataManifest();
      if (dataManifest != null) {
        if (!manifestFilesCommitedSet.contains(dataManifest.path())) {
          dataFiles.addAll(FlinkManifestUtil.readDataFiles(dataManifest, io, specs));
          addManifestInfo(committable.checkpointId(), dataManifest.path());
        } else {
          LOG.info("Data Manifest file {} has already been committed", dataManifest.path());
        }
      }

      ManifestFile deleteManifest = deltaManifests.deleteManifest();
      if (deleteManifest != null) {
        if (!manifestFilesCommitedSet.contains(deleteManifest.path())) {
          deleteFiles.addAll(FlinkManifestUtil.readDeleteFiles(deleteManifest, io, specs));
          addManifestInfo(committable.checkpointId(), deleteManifest.path());
        } else {
          LOG.info("Delete manifest file {} has already been committed", deleteManifest.path());
        }
      }

      if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
        return;
      }

      TableChange tableChange = new TableChange(dataFiles, deleteFiles);
      out.collect(tableChange);
    } else {
      LOG.warn("Unsupported type of committable message: {}", value.getClass());
    }
  }

  private void addManifestInfo(long checkpointId, String manifestPath) {
    manifestFilesCommitedSet.add(manifestPath);
    manifestFilesCommitedList.add(Tuple2.of(checkpointId, manifestPath));
    commitRequestMap.computeIfAbsent(checkpointId, k -> Lists.newArrayList()).add(manifestPath);

    // If the capacity is exceeded, delete the earliest file.
    if (manifestFilesCommitedSet.size() > maxSize) {
      long oldestCheckpointId = commitRequestMap.firstEntry().getKey();
      List<String> pathToRemoveList = commitRequestMap.get(oldestCheckpointId);
      commitRequestMap.remove(oldestCheckpointId);
      FlinkManifestUtil.deleteCommittedManifests(
          tableName, io, pathToRemoveList, flinkJobId, checkpointId);
      manifestFilesCommitedSet.clear();
      manifestFilesCommitedList.clear();
      commitRequestMap.forEach(
          (id, paths) -> {
            paths.forEach(
                path -> {
                  manifestFilesCommitedSet.add(path);
                  manifestFilesCommitedList.add(Tuple2.of(id, path));
                });
          });
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}
