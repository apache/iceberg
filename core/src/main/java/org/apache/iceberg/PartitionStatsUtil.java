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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  /**
   * Computes the partition stats for the given snapshot of the table.
   *
   * @param table the table for which partition stats to be computed.
   * @param snapshot the snapshot for which partition stats is computed.
   * @return the collection of {@link PartitionStats}
   */
  public static Collection<PartitionStats> computeStats(Table table, Snapshot snapshot) {
    Preconditions.checkArgument(table != null, "table cannot be null");
    Preconditions.checkArgument(snapshot != null, "snapshot cannot be null");

    StructType partitionType = Partitioning.partitionType(table);
    if (partitionType.fields().isEmpty()) {
      throw new UnsupportedOperationException(
          "Computing partition stats for an unpartitioned table");
    }

    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    Queue<PartitionMap<PartitionStats>> statsByManifest = Queues.newConcurrentLinkedQueue();
    Tasks.foreach(manifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(manifest -> statsByManifest.add(collectStats(table, manifest, partitionType)));

    return mergeStats(statsByManifest, table.specs());
  }

  /**
   * Sorts the {@link PartitionStats} based on the partition data.
   *
   * @param stats collection of {@link PartitionStats} which needs to be sorted.
   * @param partitionType unified partition schema.
   * @return the list of {@link PartitionStats}
   */
  public static List<PartitionStats> sortStats(
      Collection<PartitionStats> stats, StructType partitionType) {
    List<PartitionStats> entries = Lists.newArrayList(stats.iterator());
    entries.sort(
        Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
    return entries;
  }

  private static PartitionMap<PartitionStats> collectStats(
      Table table, ManifestFile manifest, StructType partitionType) {
    try (ManifestReader<?> reader = openManifest(table, manifest)) {
      PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());

      for (ManifestEntry<?> entry : reader.entries()) {
        ContentFile<?> file = entry.file();
        PartitionSpec spec = table.specs().get(file.specId());
        int specId = file.specId();
        PartitionData partition =
            PartitionUtil.coercePartitionData(partitionType, spec, file.partition());
        Snapshot snapshot = table.snapshot(entry.snapshotId());
        PartitionStats stats =
            statsMap.computeIfAbsent(
                Pair.of(specId, partition), ignored -> new PartitionStats(partition, specId));
        if (entry.isLive()) {
          stats.liveEntry(file, snapshot);
        } else {
          stats.deletedEntry(snapshot);
        }
      }

      return statsMap;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ManifestReader<?> openManifest(Table table, ManifestFile manifest) {
    List<String> projection = BaseScan.scanColumns(manifest.content());
    return ManifestFiles.open(manifest, table.io()).select(projection);
  }

  private static Collection<PartitionStats> mergeStats(
      Queue<PartitionMap<PartitionStats>> statsByManifest, Map<Integer, PartitionSpec> specs) {
    PartitionMap<PartitionStats> statsMap = PartitionMap.create(specs);
    for (PartitionMap<PartitionStats> stats : statsByManifest) {
      stats.forEach(
          (key, value) ->
              statsMap.merge(
                  key,
                  value,
                  (existingEntry, newEntry) -> {
                    existingEntry.appendStats(newEntry);
                    return existingEntry;
                  }));
    }

    return statsMap.values();
  }
}
