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
import java.util.Iterator;
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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates {@link PartitionStatisticsFile} file as per the spec for the given table. Computes the
 * stats by going through the partition info stored in each manifest file.
 */
public class PartitionStatsGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsGenerator.class);

  private final Table table;
  private String branch;
  private Types.StructType partitionType;
  // Map of PartitionData, partition-stats-entry per partitionData.
  private Map<Record, Record> partitionEntryMap;

  public PartitionStatsGenerator(Table table) {
    this.table = table;
  }

  public PartitionStatsGenerator(Table table, String branch) {
    this.table = table;
    this.branch = branch;
  }

  /**
   * Computes the partition stats for the current snapshot and writes it into the metadata folder.
   *
   * @return {@link PartitionStatisticsFile} for the latest snapshot id or null if table doesn't
   *     have any snapshot.
   */
  public PartitionStatisticsFile generate() {
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (currentSnapshot == null) {
      Preconditions.checkArgument(
          branch == null, "Couldn't find the snapshot for the branch %s", branch);
      return null;
    }

    partitionType = Partitioning.partitionType(table);
    partitionEntryMap = Maps.newConcurrentMap();

    Schema dataSchema = PartitionStatsUtil.schema(partitionType);
    List<ManifestFile> manifestFiles = currentSnapshot.allManifests(table.io());
    Tasks.foreach(manifestFiles)
        .stopOnFailure()
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
                    entry -> {
                      Record partitionKey =
                          (Record) entry.get(PartitionStatsUtil.Column.PARTITION.ordinal());
                      partitionEntryMap.merge(
                          partitionKey,
                          entry,
                          (existingEntry, newEntry) -> {
                            PartitionStatsUtil.appendStats(existingEntry, newEntry);
                            return existingEntry;
                          });
                    });
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    // Sorting the records based on partition as per spec.
    List<Record> sortedKeys = Lists.newArrayList(partitionEntryMap.keySet());
    sortedKeys.sort(Comparators.forType(partitionType));
    Iterator<Record> entriesForWriter =
        Iterators.transform(sortedKeys.iterator(), this::convertPartitionRecords);

    OutputFile outputFile =
        PartitionStatsGeneratorUtil.newPartitionStatsFile(table, currentSnapshot.snapshotId());
    PartitionStatsGeneratorUtil.writePartitionStatsFile(table, entriesForWriter, outputFile);
    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(currentSnapshot.snapshotId())
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }

  private Record convertPartitionRecords(Record key) {
    Record record = partitionEntryMap.get(key);
    Record partitionRecord = (Record) record.get(PartitionStatsUtil.Column.PARTITION.ordinal());
    if (partitionRecord != null) {
      for (int index = 0; index < partitionType.fields().size(); index++) {
        Object val = partitionRecord.get(index);
        if (val != null) {
          partitionRecord.set(
              index,
              IdentityPartitionConverters.convertConstant(
                  partitionType.fields().get(index).type(), val));
        }
      }
    }

    return record;
  }
}
