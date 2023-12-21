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
package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.actions.ImmutableComputePartitionStats;
import org.apache.iceberg.data.PartitionStatsUtil;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link ComputePartitionStats} that computes and registers the partition
 * stats to table metadata
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class ComputePartitionStatsSparkAction
    extends BaseSparkAction<ComputePartitionStatsSparkAction> implements ComputePartitionStats {
  private static final Logger LOG = LoggerFactory.getLogger(ComputePartitionStatsSparkAction.class);
  private final Table table;

  ComputePartitionStatsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected ComputePartitionStatsSparkAction self() {
    return this;
  }

  @Override
  public Result execute() {
    String jobDesc =
        String.format(
            "Computing partition stats for the table %s with snapshot id %d",
            table.name(), table.currentSnapshot().snapshotId());
    JobGroupInfo info = newJobGroupInfo("COMPUTE-PARTITION-STATS", jobDesc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    if (currentSnapshotId == -1) {
      // when the action is executed on an empty table.
      return null;
    }

    Types.StructType partitionType = Partitioning.partitionType(table);

    // Collecting instead of converting to PartitionEntry because of below error.
    // Caused by: org.apache.spark.SparkUnsupportedOperationException: Cannot have circular
    // references in bean class,
    // but got the circular reference of class class org.apache.avro.Schema.
    List<PartitionEntryBean> partitionEntryBeans = partitionEntryDS(table).collectAsList();

    Map<PartitionData, PartitionEntry> partitionEntryMap = Maps.newHashMap();
    partitionEntryBeans.forEach(
        partitionEntryBean -> {
          PartitionEntry partitionEntry =
              fromPartitionEntryBean(partitionEntryBean, table.specs(), partitionType);
          if (partitionEntryMap.containsKey(partitionEntry.partitionData())) {
            partitionEntryMap.get(partitionEntry.partitionData()).update(partitionEntry);
          } else {
            partitionEntryMap.put(partitionEntry.partitionData(), partitionEntry);
          }
        });

    OutputFile outputFile =
        PartitionStatsUtil.newPartitionStatsFile(
            ((BaseTable) table).operations(), currentSnapshotId);
    PartitionStatsUtil.writePartitionStatsFile(
        partitionEntryMap.values().iterator(), outputFile, table.specs().values());

    PartitionStatisticsFile statisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(currentSnapshotId)
            .path(outputFile.location())
            .fileSizeInBytes(outputFile.toInputFile().getLength())
            .build();

    LOG.info(
        "Registering the partition stats file: {} for the snapshot id {} to the table {}",
        outputFile.location(),
        currentSnapshotId,
        table.name());

    table.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();

    return ImmutableComputePartitionStats.Result.builder().outputFile(statisticsFile).build();
  }

  private static PartitionEntry fromPartitionEntryBean(
      PartitionEntryBean entryBean,
      Map<Integer, PartitionSpec> specs,
      Types.StructType partitionType) {
    PartitionEntry.Builder builder = PartitionEntry.builder();
    StructLike partition =
        PartitionUtil.coercePartition(
            partitionType, specs.get(entryBean.specId()), entryBean.partition());
    builder.withPartitionData(toPartitionData(partition, partitionType));

    builder
        .withLastUpdatedSnapshotId(entryBean.lastUpdatedSnapshotId())
        .withLastUpdatedAt(entryBean.lastUpdatedAt())
        .withSpecId(entryBean.specId());
    switch (entryBean.content()) {
      case DATA:
        builder
            .withDataFileCount(1)
            .withDataRecordCount(entryBean.recordCount())
            .withDataFileSizeInBytes(entryBean.fileSizeInBytes());
        break;
      case POSITION_DELETES:
        builder.withPosDeleteFileCount(1).withPosDeleteRecordCount(entryBean.recordCount());
        break;
      case EQUALITY_DELETES:
        builder.withEqDeleteFileCount(1).withEqDeleteRecordCount(entryBean.recordCount());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file content type: " + entryBean.content());
    }

    // TODO: optionally compute TOTAL_RECORD_COUNT based on the flag
    return builder.build();
  }

  private static PartitionData toPartitionData(StructLike key, Types.StructType keyType) {
    PartitionData data = new PartitionData(keyType);
    for (int i = 0; i < keyType.fields().size(); i++) {
      Object val = key.get(i, keyType.fields().get(i).type().typeId().javaClass());
      if (val != null) {
        data.set(i, val);
      }
    }
    return data;
  }
}
