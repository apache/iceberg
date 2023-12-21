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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.actions.ImmutableComputePartitionStats;
import org.apache.iceberg.data.PartitionStatsUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link ComputePartitionStats} that computes and registers the partition
 * stats to table metadata
 */
public class ComputePartitionStatsSparkAction
    extends BaseSparkAction<ComputePartitionStatsSparkAction> implements ComputePartitionStats {
  private static final Logger LOG = LoggerFactory.getLogger(ComputePartitionStatsSparkAction.class);
  private final Table table;

  private boolean local = false;

  ComputePartitionStatsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  public ComputePartitionStatsSparkAction localCompute(boolean localCompute) {
    this.local = localCompute;
    return this;
  }

  @Override
  protected ComputePartitionStatsSparkAction self() {
    return this;
  }

  @Override
  public Result execute() {
    String jobDesc = String.format("Computing partition stats for the table %s", table.name());
    JobGroupInfo info = newJobGroupInfo("COMPUTE-PARTITION-STATS", jobDesc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    if (currentSnapshotId == -1) {
      // when the action is executed on an empty table.
      return null;
    }

    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    OutputFile outputFile =
        PartitionStatsUtil.newPartitionStatsFile(
            ((BaseTable) table).operations(), currentSnapshotId, fileFormat);
    if (local) {
      localCompute(outputFile);
    } else {
      // don't have control over output file name from Spark. hence writing to a temp location
      // and moving stats file to the metadata folder after that.
      String tempLocation = table.location() + "/metadata/temp" + UUID.randomUUID();
      distributedCompute(tempLocation);
      File[] files =
          new File(tempLocation.replaceFirst("file:", ""))
              .listFiles((dir, name) -> name.toLowerCase().endsWith(".parquet"));
      if (files == null || files.length == 0) {
        LOG.error("partition stats file not found in temp location {}", tempLocation);
        return null;
      }

      try {
        // Since coalesce(1) is used, only one file will be written.
        files[0].renameTo(new File(outputFile.location().replaceFirst("file:", "")));
        FileUtils.deleteDirectory(new File(tempLocation.replaceFirst("file:", "")));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    PartitionStatisticsFile statisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(currentSnapshotId)
            .path(outputFile.location())
            .fileSizeInBytes(outputFile.toInputFile().getLength())
            .build();
    table.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();
    LOG.info(
        "Registered the partition stats file: {} for the snapshot id {} to the table {}",
        outputFile.location(),
        currentSnapshotId,
        table.name());

    return ImmutableComputePartitionStats.Result.builder().outputFile(statisticsFile).build();
  }

  private void distributedCompute(String outputDir) {
    Dataset<Row> dataset = partitionEntryDS(table);
    dataset
        .select(
            "PARTITION_DATA",
            "SPEC_ID",
            "DATA_RECORD_COUNT",
            "DATA_FILE_COUNT",
            "DATA_FILE_SIZE_IN_BYTES",
            "POSITION_DELETE_RECORD_COUNT",
            "POSITION_DELETE_FILE_COUNT",
            "EQUALITY_DELETE_RECORD_COUNT",
            "EQUALITY_DELETE_FILE_COUNT",
            "TOTAL_RECORD_COUNT",
            "LAST_UPDATED_AT",
            "LAST_UPDATED_SNAPSHOT_ID")
        .coalesce(1)
        .write()
        .format("parquet")
        .mode("overwrite")
        .option("path", outputDir)
        .save();
  }

  private void localCompute(OutputFile outputFile) {
    Map<PartitionData, PartitionEntry> partitionEntryMap = Maps.newConcurrentMap();
    List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests(table.io());

    Tasks.foreach(manifestFiles)
        .retry(3)
        .suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure(
            (file, thrown) ->
                LOG.warn(
                    "Failed to compute the partition stats for the manifest file: {}",
                    file.path(),
                    thrown))
        .run(
            manifest -> {
              try (CloseableIterable<PartitionEntry> entries =
                  PartitionEntry.fromManifest(table, manifest)) {
                entries.forEach(
                    entry ->
                        partitionEntryMap.compute(
                            entry.partitionData(),
                            (key, existingEntry) -> {
                              if (existingEntry != null) {
                                existingEntry.update(entry);
                                return existingEntry;
                              } else {
                                return entry;
                              }
                            }));
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    PartitionStatsUtil.writePartitionStatsFile(
        partitionEntryMap.values().iterator(), outputFile, table.specs().values());
  }
}
