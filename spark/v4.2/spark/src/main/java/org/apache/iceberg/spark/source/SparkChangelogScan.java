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
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode;
import org.apache.spark.sql.connector.catalog.ChangelogRange;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;

class SparkChangelogScan implements Scan, SupportsReportStatistics {

  private static final Types.StructType EMPTY_GROUPING_KEY_TYPE = Types.StructType.of();

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final IncrementalChangelogScan scan;
  private final SparkReadConf readConf;
  private final Schema projection;
  private final List<Expression> filters;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final ChangelogContext context;
  private final Long pinnedEndSnapshotId;

  // lazy variables
  private List<ScanTaskGroup<ChangelogScanTask>> taskGroups = null;
  private StructType expectedSparkType = null;

  SparkChangelogScan(
      SparkSession spark,
      Table table,
      IncrementalChangelogScan scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters) {
    this(spark, table, scan, readConf, projection, filters, null, null);
  }

  SparkChangelogScan(
      SparkSession spark,
      Table table,
      IncrementalChangelogScan scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      ChangelogContext context,
      Long pinnedEndSnapshotId) {

    SparkSchemaUtil.validateMetadataColumnReferences(table.schema(), projection);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.scan = scan;
    this.readConf = readConf;
    this.projection = projection;
    this.filters = filters != null ? filters : Collections.emptyList();
    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    this.context = context;
    this.pinnedEndSnapshotId = pinnedEndSnapshotId;
    if (scan == null) {
      this.taskGroups = Collections.emptyList();
    }
  }

  @Override
  public Statistics estimateStatistics() {
    long rowsCount = taskGroups().stream().mapToLong(ScanTaskGroup::estimatedRowsCount).sum();
    long sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
    return new Stats(sizeInBytes, rowsCount, Collections.emptyMap());
  }

  @Override
  public StructType readSchema() {
    if (expectedSparkType == null) {
      this.expectedSparkType = SparkSchemaUtil.convert(projection);
    }

    return expectedSparkType;
  }

  @Override
  public Batch toBatch() {
    return new SparkBatch(
        sparkContext,
        table,
        null != scan ? scan.fileIO() : table::io,
        readConf,
        EMPTY_GROUPING_KEY_TYPE,
        taskGroups(),
        projection,
        hashCode());
  }

  private List<ScanTaskGroup<ChangelogScanTask>> taskGroups() {
    if (taskGroups == null) {
      this.taskGroups = planTaskGroups();
    }

    return taskGroups;
  }

  private List<ScanTaskGroup<ChangelogScanTask>> planTaskGroups() {
    if (context != null) {
      validateVersions(table, context);
      return pinnedEndSnapshotId == null
          ? Collections.emptyList()
          : planTasks(table, scan, context, pinnedEndSnapshotId);
    }

    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> groups = scan.planTasks()) {
      return Lists.newArrayList(groups);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close changelog scan: " + scan, e);
    }
  }

  static void validateRange(ChangelogContext context) {
    if (context.range() instanceof ChangelogRange.VersionRange versions) {
      long startVersion = parseVersion(versions.startingVersion());
      Long endVersion = versions.endingVersion().map(SparkChangelogScan::parseVersion).orElse(null);
      Preconditions.checkArgument(
          endVersion == null || startVersion <= endVersion,
          "Cannot set startingVersion %s greater than endingVersion %s",
          startVersion,
          endVersion);
    } else if (context.range() instanceof ChangelogRange.TimestampRange timestamps) {
      Preconditions.checkArgument(
          timestamps.endingTimestamp().isEmpty()
              || timestamps.startingTimestamp() <= timestamps.endingTimestamp().get(),
          "Cannot set startingTimestamp %s greater than endingTimestamp %s",
          timestamps.startingTimestamp(),
          timestamps.endingTimestamp().orElse(null));
    }
  }

  static boolean requiresPostProcessing(ChangelogContext context) {
    return context.deduplicationMode() != DeduplicationMode.NONE || context.computeUpdates();
  }

  private static boolean includes(ChangelogContext context, Snapshot snapshot) {
    ChangelogRange range = context.range();
    if (range instanceof ChangelogRange.VersionRange versions) {
      long version = snapshot.sequenceNumber();
      long startVersion = parseVersion(versions.startingVersion());
      Long endVersion = versions.endingVersion().map(SparkChangelogScan::parseVersion).orElse(null);
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

  private static void validateVersions(Table table, ChangelogContext context) {
    if (!(context.range() instanceof ChangelogRange.VersionRange range)) {
      return;
    }

    long startVersion = parseVersion(range.startingVersion());
    Long endVersion = range.endingVersion().map(SparkChangelogScan::parseVersion).orElse(null);
    Set<Long> versions = Sets.newHashSet();
    for (Snapshot snapshot : SnapshotUtil.currentAncestors(table)) {
      versions.add(snapshot.sequenceNumber());
    }

    Preconditions.checkArgument(
        versions.contains(startVersion),
        "Cannot find Iceberg snapshot with sequence number: %s",
        startVersion);
    Preconditions.checkArgument(
        endVersion == null || versions.contains(endVersion),
        "Cannot find Iceberg snapshot with sequence number: %s",
        endVersion);
  }

  static List<ScanTaskGroup<ChangelogScanTask>> planTasks(
      Table table, IncrementalChangelogScan scan, ChangelogContext context, long endInclusive) {
    List<Snapshot> snapshots = Lists.newArrayList();
    Set<Long> snapshotIds = Sets.newHashSet();
    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table, endInclusive, null)) {
      if (includes(context, snapshot)) {
        validateSnapshot(snapshot, context);
        snapshots.add(snapshot);
        snapshotIds.add(snapshot.snapshotId());
      }
    }

    if (snapshots.isEmpty()) {
      return Collections.emptyList();
    }

    IncrementalChangelogScan boundedScan =
        scan.fromSnapshotInclusive(snapshots.get(snapshots.size() - 1).snapshotId())
            .toSnapshot(snapshots.get(0).snapshotId());
    List<ChangelogScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<ChangelogScanTask> files = boundedScan.planFiles()) {
      for (ChangelogScanTask task : files) {
        // Commit timestamps need not follow snapshot order. Filter whole commits, not rows.
        if (snapshotIds.contains(task.commitSnapshotId())) {
          validateTaskLineage(task, context);
          tasks.add(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close Spark CDC scan tasks", e);
    }

    return TableScanUtil.planTaskGroups(
        tasks,
        boundedScan.targetSplitSize(),
        boundedScan.splitLookback(),
        boundedScan.splitOpenFileCost());
  }

  private static void validateSnapshot(Snapshot snapshot, ChangelogContext context) {
    Preconditions.checkArgument(
        snapshot.sequenceNumber() > 0,
        "Cannot read Spark CDC from snapshot %s without a commit sequence number",
        snapshot.snapshotId());
    Preconditions.checkArgument(
        !requiresPostProcessing(context) || snapshot.firstRowId() != null,
        "Cannot read Spark CDC from snapshot %s without row lineage",
        snapshot.snapshotId());
  }

  private static void validateTaskLineage(ChangelogScanTask task, ChangelogContext context) {
    Preconditions.checkArgument(
        !requiresPostProcessing(context)
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
  public String description() {
    return String.format(
        Locale.ROOT,
        "IcebergChangelogScan(table=%s, fromSnapshotId=%d, toSnapshotId=%d, filters=%s)",
        table,
        startSnapshotId,
        endSnapshotId,
        filtersDesc());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkChangelogScan that = (SparkChangelogScan) o;
    return table.name().equals(that.table.name())
        && Objects.equals(table.uuid(), that.table.uuid())
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field IDs
        && filtersDesc().equals(that.filtersDesc())
        && Objects.equals(startSnapshotId, that.startSnapshotId)
        && Objects.equals(endSnapshotId, that.endSnapshotId)
        && Objects.equals(context, that.context)
        && Objects.equals(pinnedEndSnapshotId, that.pinnedEndSnapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table.name(),
        table.uuid(),
        readSchema(),
        filtersDesc(),
        startSnapshotId,
        endSnapshotId,
        context,
        pinnedEndSnapshotId);
  }

  private String filtersDesc() {
    return Spark3Util.describe(filters);
  }
}
