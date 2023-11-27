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
package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratePartitionStats {
  private static final Logger LOG = LoggerFactory.getLogger(GeneratePartitionStats.class);

  private final Table table;

  GeneratePartitionStats(Table table) {
    this.table = table;
  }

  /**
   * Computes the partition stats for the current snapshot and writes it into the metadata folder.
   *
   * @return {@link PartitionStatisticsFile} for the latest snapshot id or null if table doesn't
   *     have any snapshot.
   */
  public PartitionStatisticsFile generate() {
    // TODO: Support an option to accept branches and tag info and lookup corresponding snapshot.
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      return null;
    }

    // Map of partitionData, partition-stats-entry per partitionData
    Map<Record, Record> partitionEntryMap = Maps.newConcurrentMap();
    Schema dataSchema = PartitionStatsUtil.schema(Partitioning.partitionType(table));
    List<ManifestFile> manifestFiles = currentSnapshot.allManifests(table.io());
    Tasks.foreach(manifestFiles)
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
              try (CloseableIterable<Record> entries =
                  PartitionStatsUtil.fromManifest(table, manifest, dataSchema)) {
                entries.forEach(
                    entry ->
                        partitionEntryMap.compute(
                            (Record) entry.get(PartitionStatsUtil.Column.PARTITION_DATA.ordinal()),
                            (key, existingEntry) -> {
                              if (existingEntry != null) {
                                PartitionStatsUtil.updateRecord(existingEntry, entry);
                                return existingEntry;
                              } else {
                                return entry;
                              }
                            }));
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    OutputFile outputFile =
        PartitionStatsWriterUtil.newPartitionStatsFile(table, currentSnapshot.snapshotId());
    PartitionStatsWriterUtil.writePartitionStatsFile(
        table, partitionEntryMap.values().iterator(), outputFile);
    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(currentSnapshot.snapshotId())
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }
}
