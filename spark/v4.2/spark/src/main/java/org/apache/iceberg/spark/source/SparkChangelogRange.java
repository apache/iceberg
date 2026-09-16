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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode;
import org.apache.spark.sql.connector.catalog.ChangelogRange;

/** Keeps CDC bounds independent of the snapshots available when a stream is created. */
class SparkChangelogRange {
  private final ChangelogContext context;
  private final Long startVersion;
  private final Long endVersion;

  SparkChangelogRange(ChangelogContext context) {
    this.context = context;
    if (context.range() instanceof ChangelogRange.VersionRange versions) {
      this.startVersion = parseVersion(versions.startingVersion());
      this.endVersion =
          versions.endingVersion().map(SparkChangelogRange::parseVersion).orElse(null);
    } else {
      this.startVersion = null;
      this.endVersion = null;
    }
  }

  boolean requiresPostProcessing() {
    return context.deduplicationMode() != DeduplicationMode.NONE || context.computeUpdates();
  }

  boolean includes(Snapshot snapshot) {
    ChangelogRange range = context.range();
    if (range instanceof ChangelogRange.VersionRange) {
      long version = snapshot.sequenceNumber();
      return (range.startingBoundInclusive() ? version >= startVersion : version > startVersion)
          && (endVersion == null
              || (range.endingBoundInclusive() ? version <= endVersion : version < endVersion));
    } else if (range instanceof ChangelogRange.TimestampRange timestamps) {
      long timestamp = snapshot.timestampMillis() * 1000;
      return (timestamps.startingBoundInclusive()
              ? timestamp >= timestamps.startingTimestamp()
              : timestamp > timestamps.startingTimestamp())
          && (timestamps.endingTimestamp().isEmpty()
              || (timestamps.endingBoundInclusive()
                  ? timestamp <= timestamps.endingTimestamp().get()
                  : timestamp < timestamps.endingTimestamp().get()));
    }

    return true;
  }

  void validateVersions(Table table) {
    Set<Long> versions = Sets.newHashSet();
    for (Snapshot snapshot : SnapshotUtil.currentAncestors(table)) {
      versions.add(snapshot.sequenceNumber());
    }

    Preconditions.checkArgument(
        startVersion == null || versions.contains(startVersion),
        "Cannot find Iceberg snapshot with sequence number: %s",
        startVersion);
    Preconditions.checkArgument(
        endVersion == null || versions.contains(endVersion),
        "Cannot find Iceberg snapshot with sequence number: %s",
        endVersion);
  }

  List<ScanTaskGroup<ChangelogScanTask>> planTasks(
      Table table, IncrementalChangelogScan scan, Long startExclusive, long endInclusive) {
    Preconditions.checkState(
        startExclusive == null
            || SnapshotUtil.isParentAncestorOf(table, endInclusive, startExclusive),
        "Cannot read CDC: snapshot %s is not an ancestor of %s",
        startExclusive,
        endInclusive);
    List<Snapshot> snapshots = Lists.newArrayList();
    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table, endInclusive, startExclusive)) {
      if (includes(snapshot)) {
        snapshots.add(snapshot);
      }
    }

    if (snapshots.isEmpty()) {
      return Collections.emptyList();
    }

    Set<Long> snapshotIds = Sets.newHashSet();
    for (Snapshot snapshot : snapshots) {
      validateSnapshot(snapshot);
      snapshotIds.add(snapshot.snapshotId());
    }

    IncrementalChangelogScan boundedScan =
        scan.fromSnapshotInclusive(snapshots.get(snapshots.size() - 1).snapshotId())
            .toSnapshot(snapshots.get(0).snapshotId());
    List<ScanTaskGroup<ChangelogScanTask>> result = Lists.newArrayList();
    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> groups = boundedScan.planTasks()) {
      for (ScanTaskGroup<ChangelogScanTask> group : groups) {
        List<ChangelogScanTask> tasks = Lists.newArrayList();
        for (ChangelogScanTask task : group.tasks()) {
          // Commit timestamps need not follow snapshot order. Filter whole commits, not rows.
          if (snapshotIds.contains(task.commitSnapshotId())) {
            validateTaskLineage(task);
            tasks.add(task);
          }
        }

        if (!tasks.isEmpty()) {
          result.add(new BaseScanTaskGroup<>(group.groupingKey(), tasks));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close Spark CDC scan tasks", e);
    }

    return result;
  }

  private void validateSnapshot(Snapshot snapshot) {
    Preconditions.checkArgument(
        snapshot.sequenceNumber() > 0,
        "Cannot read Spark CDC from snapshot %s without a commit sequence number",
        snapshot.snapshotId());
    Preconditions.checkArgument(
        !requiresPostProcessing() || snapshot.firstRowId() != null,
        "Cannot read Spark CDC from snapshot %s without row lineage",
        snapshot.snapshotId());
  }

  private void validateTaskLineage(ChangelogScanTask task) {
    Preconditions.checkArgument(
        !requiresPostProcessing()
            || (task instanceof ContentScanTask<?> contentTask
                && contentTask.file().firstRowId() != null),
        "Cannot read Spark CDC from a file without row lineage in snapshot %s",
        task.commitSnapshotId());
  }

  private static long parseVersion(String version) {
    try {
      return Long.parseLong(version);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid Iceberg snapshot sequence number: " + version, e);
    }
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SparkChangelogRange that && context.equals(that.context);
  }

  @Override
  public int hashCode() {
    return context.hashCode();
  }

  @Override
  public String toString() {
    return context.toString();
  }
}
