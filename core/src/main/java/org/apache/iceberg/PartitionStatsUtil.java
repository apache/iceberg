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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.StructLikeMap;
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

    ExecutorService executorService = ThreadPools.getWorkerPool();
    List<Future<StructLikeMap<PartitionStats>>> futures = Lists.newArrayList();
    manifests.forEach(
        manifest -> {
          Future<StructLikeMap<PartitionStats>> future =
              executorService.submit(() -> collectStats(table, manifest, partitionType));
          futures.add(future);
        });

    StructLikeMap<PartitionStats> statsMap = StructLikeMap.create(partitionType);
    for (Future<StructLikeMap<PartitionStats>> future : futures) {
      try {
        future
            .get()
            .forEach(
                (key, value) ->
                    statsMap.merge(
                        key,
                        value,
                        (existingEntry, newEntry) -> {
                          existingEntry.appendStats(newEntry);
                          return existingEntry;
                        }));
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    return statsMap.values();
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

  private static StructLikeMap<PartitionStats> collectStats(
      Table table, ManifestFile manifest, StructType partitionType) {
    try (ManifestReader<?> reader = openManifest(table, manifest)) {
      StructLikeMap<PartitionStats> statsMap = StructLikeMap.create(partitionType);

      for (ManifestEntry<?> entry : reader.entries()) {
        ContentFile<?> file = entry.file();
        PartitionSpec spec = table.specs().get(file.specId());
        PartitionData key =
            PartitionUtil.coercePartitionData(partitionType, spec, file.partition());
        Snapshot snapshot = table.snapshot(entry.snapshotId());
        PartitionStats stats =
            statsMap.computeIfAbsent(key, ignored -> new PartitionStats(key, file.specId()));
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
}
