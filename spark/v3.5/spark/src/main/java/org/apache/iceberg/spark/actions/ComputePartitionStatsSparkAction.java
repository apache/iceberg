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

import java.io.IOException;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.actions.ImmutableComputePartitionStats;
import org.apache.iceberg.data.PartitionStatsHandler;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes and writes the partition statistics for an Iceberg table. Also registers it to table
 * metadata. Uses current snapshot if not specified.
 */
public class ComputePartitionStatsSparkAction
    extends BaseSparkAction<ComputePartitionStatsSparkAction> implements ComputePartitionStats {

  private static final Logger LOG = LoggerFactory.getLogger(ComputePartitionStatsSparkAction.class);
  private static final Result EMPTY_RESULT =
      ImmutableComputePartitionStats.Result.builder().build();

  private final Table table;
  private Snapshot snapshot;

  ComputePartitionStatsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.snapshot = table.currentSnapshot();
  }

  @Override
  protected ComputePartitionStatsSparkAction self() {
    return this;
  }

  @Override
  public ComputePartitionStats snapshot(long newSnapshotId) {
    Snapshot newSnapshot = table.snapshot(newSnapshotId);
    Preconditions.checkArgument(newSnapshot != null, "Snapshot not found: %s", newSnapshotId);
    this.snapshot = newSnapshot;
    return this;
  }

  @Override
  public Result execute() {
    if (snapshot == null) {
      LOG.info("No snapshot to compute partition stats for table {}", table.name());
      return EMPTY_RESULT;
    }

    JobGroupInfo info = newJobGroupInfo("COMPUTE-PARTITION-STATS", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    LOG.info("Computing partition stats for {} (snapshot {})", table.name(), snapshot.snapshotId());
    PartitionStatisticsFile statisticsFile;
    try {
      statisticsFile = PartitionStatsHandler.computeAndWriteStatsFile(table, snapshot.snapshotId());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (statisticsFile == null) {
      return EMPTY_RESULT;
    }

    table.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();
    return ImmutableComputePartitionStats.Result.builder().statisticsFile(statisticsFile).build();
  }

  private String jobDesc() {
    return String.format(
        "Computing partition stats for %s (snapshot=%s)", table.name(), snapshot.snapshotId());
  }
}
