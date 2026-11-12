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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode;
import org.apache.spark.sql.connector.catalog.ChangelogRange;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Exposes Iceberg changes through Spark's native CDC API. */
public class SparkChangelog extends SparkChangelogBase implements Changelog {

  static final int COMMIT_VERSION_ID = Integer.MAX_VALUE - 109;
  static final int COMMIT_TIMESTAMP_ID = Integer.MAX_VALUE - 110;

  static final String COMMIT_VERSION = "_commit_version";
  static final String COMMIT_TIMESTAMP = "_commit_timestamp";

  private static final Set<String> UNSUPPORTED_CDC_OPTIONS =
      ImmutableSet.of(
          SparkReadOptions.START_SNAPSHOT_ID,
          SparkReadOptions.END_SNAPSHOT_ID,
          SparkReadOptions.START_TIMESTAMP,
          SparkReadOptions.END_TIMESTAMP,
          SparkReadOptions.LEGACY_SNAPSHOT_ID,
          SparkReadOptions.LEGACY_AS_OF_TIMESTAMP,
          SparkReadOptions.LEGACY_TAG,
          SparkReadOptions.VERSION_AS_OF,
          SparkReadOptions.TIMESTAMP_AS_OF,
          SparkReadOptions.BRANCH,
          SparkReadOptions.STREAM_FROM_TIMESTAMP,
          SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS,
          SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS);

  private static final Schema CDC_METADATA_SCHEMA =
      new Schema(
          MetadataColumns.CHANGE_TYPE,
          Types.NestedField.required(
              COMMIT_VERSION_ID,
              COMMIT_VERSION,
              Types.LongType.get(),
              "Iceberg snapshot sequence number"),
          Types.NestedField.required(
              COMMIT_TIMESTAMP_ID,
              COMMIT_TIMESTAMP,
              Types.TimestampType.withZone(),
              "Iceberg snapshot commit timestamp"));

  private final ChangelogContext context;
  private final String[] identifierColumns;
  private final Schema sparkCdcSchema;

  public SparkChangelog(Table table, ChangelogContext context) {
    this(table, context, CaseInsensitiveStringMap.empty());
  }

  public SparkChangelog(Table table, ChangelogContext context, CaseInsensitiveStringMap options) {
    super(table);
    this.context = Preconditions.checkNotNull(context, "Changelog context cannot be null");
    validateCdcOptions(options);
    this.identifierColumns = identifierColumns(table.schema(), options);
    Preconditions.checkArgument(
        identifierColumns.length == 0
            || context.deduplicationMode() == DeduplicationMode.DROP_CARRYOVERS,
        "Business-key CDC requires deduplicationMode=dropCarryovers");
    validateRange(context);
    Preconditions.checkArgument(
        TableUtil.formatVersion(table) >= 2,
        "Spark CDC requires format version 2 or later for commit sequence numbers");
    boolean requiresRowLineage = identifierColumns.length == 0 && requiresPostProcessing(context);
    Preconditions.checkArgument(
        !requiresRowLineage || TableUtil.supportsRowLineage(table),
        "Spark CDC post-processing requires row lineage. "
            + "Use deduplicationMode=none with computeUpdates=false for raw changes");
    this.sparkCdcSchema =
        TypeUtil.join(cdcDataSchema(table, requiresRowLineage), CDC_METADATA_SCHEMA);
  }

  private static void validateCdcOptions(CaseInsensitiveStringMap options) {
    for (String option : UNSUPPORTED_CDC_OPTIONS) {
      Preconditions.checkArgument(
          !options.containsKey(option),
          "Use Spark CDC startingVersion/endingVersion or startingTimestamp/endingTimestamp "
              + "options. Option %s is not supported for Spark CDC",
          option);
    }
  }

  private static String[] identifierColumns(Schema schema, CaseInsensitiveStringMap options) {
    String value = options.get(SparkReadOptions.CDC_IDENTIFIER_COLUMNS);
    if (value == null) {
      return new String[0];
    }

    Set<String> resolved = Sets.newLinkedHashSet();
    boolean caseSensitive = SparkUtil.caseSensitive(SparkSession.active());
    for (String name : Splitter.on(',').trimResults().split(value)) {
      Types.NestedField field =
          caseSensitive
              ? schema.asStruct().field(name)
              : schema.asStruct().caseInsensitiveField(name);
      Preconditions.checkArgument(
          field != null && field.type().isPrimitiveType(),
          "CDC identifier column must be an existing top-level primitive field: %s",
          name);
      Preconditions.checkArgument(
          resolved.add(field.name()), "Duplicate CDC identifier column: %s", name);
    }

    return resolved.toArray(String[]::new);
  }

  /** Identifier fields consumed by the Iceberg CDC resolution rule. */
  public String[] identifierColumns() {
    return identifierColumns.clone();
  }

  /** Raw input for the Iceberg business-key CDC resolution rule. */
  public SparkChangelog rawChangelog() {
    Preconditions.checkState(identifierColumns.length > 0, "Not a business-key changelog");
    return new SparkChangelog(
        table(), new ChangelogContext(context.range(), DeduplicationMode.NONE, false));
  }

  static Schema cdcDataSchema(Table table) {
    return cdcDataSchema(table, true);
  }

  private static Schema cdcDataSchema(Table table, boolean requiresRowLineage) {
    // Raw CDC can expose older files whose lineage is unknown without inventing an identity.
    Schema lineageSchema =
        requiresRowLineage
            ? new Schema(
                MetadataColumns.ROW_ID.asRequired(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.asRequired())
            : new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    return TypeUtil.join(table.schema(), lineageSchema);
  }

  static Schema dropCdcMetadata(Schema schema) {
    return TypeUtil.selectNot(
        schema,
        Set.of(MetadataColumns.CHANGE_TYPE.fieldId(), COMMIT_VERSION_ID, COMMIT_TIMESTAMP_ID));
  }

  @Override
  Schema changelogSchema() {
    return sparkCdcSchema;
  }

  @Override
  public boolean containsCarryoverRows() {
    return identifierColumns.length == 0;
  }

  @Override
  public boolean containsIntermediateChanges() {
    return true;
  }

  @Override
  public boolean representsUpdateAsDeleteAndInsert() {
    return identifierColumns.length == 0 || !context.computeUpdates();
  }

  @Override
  public NamedReference[] rowId() {
    if (identifierColumns.length > 0) {
      return Arrays.stream(identifierColumns)
          .map(name -> Spark3Util.toNamedReference("`" + name.replace("`", "``") + "`"))
          .toArray(NamedReference[]::new);
    }

    return new NamedReference[] {Spark3Util.toNamedReference(MetadataColumns.ROW_ID.name())};
  }

  @Override
  public NamedReference rowVersion() {
    return Spark3Util.toNamedReference(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Preconditions.checkState(
        identifierColumns.length == 0, "Business-key CDC requires IcebergSparkSessionExtensions");
    validateCdcOptions(options);
    SparkChangelogScanBuilder builder =
        new SparkChangelogScanBuilder(spark(), table(), sparkCdcSchema, options);
    return () -> newScan(builder);
  }

  private Scan newScan(SparkChangelogScanBuilder builder) {
    IncrementalChangelogScan scan =
        builder.buildIcebergScan(
            ChangelogUtil.changelogSchema(dropCdcMetadata(sparkCdcSchema)), null, null);
    Snapshot currentSnapshot = table().currentSnapshot();
    Long endSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;
    return new SparkChangelogScan(
        spark(), table(), scan, builder.readConf(), sparkCdcSchema, builder.filters()) {
      @Override
      List<ScanTaskGroup<ChangelogScanTask>> planTaskGroups() {
        validateVersions(table(), context);
        return endSnapshotId == null
            ? Collections.emptyList()
            : planTasks(table(), scan, context, endSnapshotId);
      }

      @Override
      public boolean equals(Object other) {
        // Native ranges are not represented by the legacy snapshot options used for scan equality.
        return this == other;
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }
    };
  }

  private static void validateRange(ChangelogContext context) {
    if (context.range() instanceof ChangelogRange.VersionRange versions) {
      long startVersion = parseVersion(versions.startingVersion());
      Long endVersion = versions.endingVersion().map(SparkChangelog::parseVersion).orElse(null);
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

  private static boolean requiresPostProcessing(ChangelogContext context) {
    return context.deduplicationMode() != DeduplicationMode.NONE || context.computeUpdates();
  }

  private static boolean includes(ChangelogContext context, Snapshot snapshot) {
    ChangelogRange range = context.range();
    if (range instanceof ChangelogRange.VersionRange versions) {
      long version = snapshot.sequenceNumber();
      long startVersion = parseVersion(versions.startingVersion());
      Long endVersion = versions.endingVersion().map(SparkChangelog::parseVersion).orElse(null);
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
    Long endVersion = range.endingVersion().map(SparkChangelog::parseVersion).orElse(null);
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
}
