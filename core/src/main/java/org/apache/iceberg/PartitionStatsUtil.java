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
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
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
    Preconditions.checkArgument(Partitioning.isPartitioned(table), "table must be partitioned");
    Preconditions.checkArgument(snapshot != null, "snapshot cannot be null");

    StructType partitionType = Partitioning.partitionType(table);
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    return collectStats(table, manifests, partitionType).values();
  }

  /**
   * Computes the partition stats incrementally after the given snapshot to current snapshot.
   *
   * @param table the table for which partition stats to be computed.
   * @param afterSnapshot the snapshot after which partition stats is computed (exclusive).
   * @param currentSnapshot the snapshot till which partition stats is computed (inclusive).
   * @return the {@link PartitionMap} of {@link PartitionStats}
   */
  public static PartitionMap<PartitionStats> computeStatsIncremental(
      Table table, Snapshot afterSnapshot, Snapshot currentSnapshot) {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    Preconditions.checkArgument(Partitioning.isPartitioned(table), "Table must be partitioned");
    Preconditions.checkArgument(currentSnapshot != null, "Current snapshot cannot be null");
    Preconditions.checkArgument(afterSnapshot != null, "Snapshot cannot be null");
    Preconditions.checkArgument(currentSnapshot != afterSnapshot, "Both the snapshots are same");
    Preconditions.checkArgument(
        SnapshotUtil.isAncestorOf(table, currentSnapshot.snapshotId(), afterSnapshot.snapshotId()),
        "Starting snapshot %s is not an ancestor of current snapshot %s",
        afterSnapshot.snapshotId(),
        currentSnapshot.snapshotId());

    Set<Long> snapshotIdsRange =
        Sets.newHashSet(
            SnapshotUtil.ancestorIdsBetween(
                currentSnapshot.snapshotId(), afterSnapshot.snapshotId(), table::snapshot));
    StructType partitionType = Partitioning.partitionType(table);
    List<ManifestFile> manifests =
        currentSnapshot.allManifests(table.io()).stream()
            .filter(
                manifestFile ->
                    snapshotIdsRange.contains(manifestFile.snapshotId())
                        && !manifestFile.hasExistingFiles())
            .collect(Collectors.toList());
    return collectStats(table, manifests, partitionType);
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
    List<PartitionStats> entries = Lists.newArrayList(stats);
    entries.sort(partitionStatsCmp(partitionType));
    return entries;
  }

  private static Comparator<PartitionStats> partitionStatsCmp(StructType partitionType) {
    return Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType));
  }

  private static PartitionMap<PartitionStats> collectStats(
      Table table, List<ManifestFile> manifests, StructType partitionType) {
    Queue<PartitionMap<PartitionStats>> statsByManifest = Queues.newConcurrentLinkedQueue();
    Tasks.foreach(manifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(
            manifest ->
                statsByManifest.add(collectStatsForManifest(table, manifest, partitionType)));

    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
    for (PartitionMap<PartitionStats> stats : statsByManifest) {
      mergePartitionMap(stats, statsMap);
    }

    return statsMap;
  }

  private static PartitionMap<PartitionStats> collectStatsForManifest(
      Table table, ManifestFile manifest, StructType partitionType) {
    try (ManifestReader<?> reader = openManifest(table, manifest)) {
      PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
      int specId = manifest.partitionSpecId();
      PartitionSpec spec = table.specs().get(specId);
      PartitionData keyTemplate = new PartitionData(partitionType);

      for (ManifestEntry<?> entry : reader.entries()) {
        ContentFile<?> file = entry.file();
        StructLike coercedPartition =
            PartitionUtil.coercePartition(partitionType, spec, file.partition());
        StructLike key = keyTemplate.copyFor(coercedPartition);
        Snapshot snapshot = table.snapshot(entry.snapshotId());
        PartitionStats stats =
            statsMap.computeIfAbsent(
                specId,
                ((PartitionData) file.partition()).copy(),
                () -> new PartitionStats(key, specId));
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

  public static void mergePartitionMap(
      PartitionMap<PartitionStats> fromMap, PartitionMap<PartitionStats> toMap) {
    fromMap.forEach(
        (key, value) ->
            toMap.merge(
                key,
                value,
                (existingEntry, newEntry) -> {
                  existingEntry.appendStats(newEntry);
                  return existingEntry;
                }));
  }
}
