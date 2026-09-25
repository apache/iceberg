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
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class CommittableToTableChangeConverter
    extends ProcessFunction<CommittableMessage<IcebergCommittable>, TableChange> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommittableToTableChangeConverter.class);

  private final FileIO io;
  private final String tableName;
  private final Map<Integer, PartitionSpec> specs;
  private transient String flinkJobId;

  public CommittableToTableChangeConverter(
      FileIO fileIO, String tableName, Map<Integer, PartitionSpec> specs) {
    Preconditions.checkNotNull(fileIO, "FileIO should not be null");
    Preconditions.checkNotNull(tableName, "TableName should not be null");
    Preconditions.checkNotNull(specs, "Specs should not be null");
    this.io = fileIO;
    this.tableName = tableName;
    this.specs = specs;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    Preconditions.checkState(
        getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks() == 1,
        "CommittableToTableChangeConverter must run with parallelism 1, current parallelism: %s",
        getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());

    this.flinkJobId = getRuntimeContext().getJobInfo().getJobId().toString();
  }

  @Override
  public void processElement(
      CommittableMessage<IcebergCommittable> value, Context ctx, Collector<TableChange> out)
      throws Exception {
    if (value instanceof CommittableWithLineage) {
      IcebergCommittable committable =
          ((CommittableWithLineage<IcebergCommittable>) value).getCommittable();

      if (committable == null || committable.manifest().length == 0) {
        return;
      }

      DeltaManifests deltaManifests;
      WriteResult writeResult;
      try {
        deltaManifests =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                DeltaManifestsSerializer.INSTANCE, committable.manifest());
        writeResult = FlinkManifestUtil.readCompletedFiles(deltaManifests, io, specs);
      } catch (Exception e) {
        LOG.warn(
            "Unable to read delta manifests for table {} at checkpoint {}",
            tableName,
            committable.checkpointId(),
            e);
        return;
      }

      TableChange tableChange =
          new TableChange(
              Arrays.asList(writeResult.dataFiles()), Arrays.asList(writeResult.deleteFiles()));
      out.collect(tableChange);
      FlinkManifestUtil.deleteCommittedManifests(
          tableName, io, deltaManifests.manifests(), flinkJobId, committable.checkpointId());
    }
  }
}
