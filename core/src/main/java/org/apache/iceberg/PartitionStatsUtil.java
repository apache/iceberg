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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsUtil.class);

  /**
   * Computes the partition stats for the given snapshot of the table.
   *
   * @param table the table for which partition stats to be computed.
   * @param snapshot the snapshot for which partition stats is computed.
   * @return iterable {@link PartitionStats}
   */
  public static Iterable<PartitionStats> computeStats(Table table, Snapshot snapshot) {
    Preconditions.checkState(table != null, "table cannot be null");
    Preconditions.checkState(snapshot != null, "snapshot cannot be null");

    Types.StructType partitionType = Partitioning.partitionType(table);
    Map<Record, PartitionStats> partitionEntryMap = Maps.newConcurrentMap();

    List<ManifestFile> manifestFiles = snapshot.allManifests(table.io());
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
              try (CloseableIterable<PartitionStats> entries =
                  PartitionStatsUtil.fromManifest(table, manifest, partitionType)) {
                entries.forEach(
                    entry -> {
                      Record partitionKey = entry.partition();
                      partitionEntryMap.merge(
                          partitionKey,
                          entry,
                          (existingEntry, newEntry) -> {
                            existingEntry.appendStats(newEntry);
                            return existingEntry;
                          });
                    });
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    return partitionEntryMap.values();
  }

  /**
   * Sorts the {@link PartitionStats} based on the partition data.
   *
   * @param stats iterable {@link PartitionStats} which needs to be sorted.
   * @param partitionType unified partition schema.
   * @return Iterator of {@link PartitionStats}
   */
  public static Iterator<PartitionStats> sortStats(
      Iterable<PartitionStats> stats, Types.StructType partitionType) {
    List<PartitionStats> entries = Lists.newArrayList(stats.iterator());
    entries.sort(
        Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
    return entries.iterator();
  }

  private static CloseableIterable<PartitionStats> fromManifest(
      Table table, ManifestFile manifest, Types.StructType partitionType) {
    return CloseableIterable.transform(
        ManifestFiles.open(manifest, table.io(), table.specs())
            .select(BaseScan.scanColumns(manifest.content()))
            .entries(),
        entry -> {
          // partition data as per unified partition spec
          Record partitionData = coercedPartitionData(entry.file(), table.specs(), partitionType);
          PartitionStats partitionStats = new PartitionStats(partitionData);
          if (entry.isLive()) {
            partitionStats.liveEntry(entry.file(), table.snapshot(entry.snapshotId()));
          } else {
            partitionStats.deletedEntry(table.snapshot(entry.snapshotId()));
          }

          return partitionStats;
        });
  }

  private static Record coercedPartitionData(
      ContentFile<?> file, Map<Integer, PartitionSpec> specs, Types.StructType partitionType) {
    // keep the partition data as per the unified spec by coercing
    StructLike partition =
        PartitionUtil.coercePartition(partitionType, specs.get(file.specId()), file.partition());
    GenericRecord record = GenericRecord.create(partitionType);
    for (int index = 0; index < partitionType.fields().size(); index++) {
      Object val =
          partition.get(index, partitionType.fields().get(index).type().typeId().javaClass());
      if (val != null) {
        record.set(index, val);
      }
    }

    return record;
  }
}
