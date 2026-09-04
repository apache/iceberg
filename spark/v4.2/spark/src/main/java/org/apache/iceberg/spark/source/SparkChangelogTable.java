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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogRange;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Iceberg changelog relation used both as {@code table.changes} and Spark 4.2 {@link Changelog}.
 *
 * <p>The Table API keeps Iceberg's changelog columns ({@code _change_type}, {@code
 * _change_ordinal}, {@code _commit_snapshot_id}). The Changelog API maps those onto Spark CDC
 * columns ({@code _change_type}, {@code _commit_version}, {@code _commit_timestamp}).
 */
public class SparkChangelogTable
    implements org.apache.spark.sql.connector.catalog.Table,
        SupportsRead,
        SupportsMetadataColumns,
        Changelog {

  public static final String TABLE_NAME = "changes";

  static final String COMMIT_VERSION = "_commit_version";
  static final String COMMIT_TIMESTAMP = "_commit_timestamp";

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ);

  private static final Types.NestedField ROW_ID_FIELD =
      Types.NestedField.required(
          MetadataColumns.ROW_ID.fieldId(),
          MetadataColumns.ROW_ID.name(),
          MetadataColumns.ROW_ID.type(),
          MetadataColumns.ROW_ID.doc());
  private static final Types.NestedField ROW_VERSION_FIELD =
      Types.NestedField.required(
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.type(),
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.doc());

  private static final Types.NestedField COMMIT_VERSION_FIELD =
      Types.NestedField.required(
          Integer.MAX_VALUE - 109,
          COMMIT_VERSION,
          Types.LongType.get(),
          "Iceberg snapshot sequence number");
  private static final Types.NestedField COMMIT_TIMESTAMP_FIELD =
      Types.NestedField.required(
          Integer.MAX_VALUE - 110,
          COMMIT_TIMESTAMP,
          Types.TimestampType.withZone(),
          "Iceberg snapshot commit timestamp");

  private final Table table;
  private final Schema icebergChangelogSchema;
  private final RangeOptions rangeOptions;
  private final Schema sparkCdcSchema;
  private final Column[] sparkCdcColumns;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;

  public SparkChangelogTable(Table table) {
    this(table, RangeOptions.icebergChangelog());
  }

  public SparkChangelogTable(Table table, ChangelogContext context) {
    this(table, rangeOptions(table, context));
  }

  private SparkChangelogTable(Table table, RangeOptions rangeOptions) {
    this.table = table;
    this.icebergChangelogSchema = ChangelogUtil.changelogSchema(table.schema());
    this.rangeOptions = rangeOptions;
    Preconditions.checkArgument(
        !rangeOptions.readMode().isSparkCdc() || TableUtil.supportsRowLineage(table),
        "Spark CDC requires an Iceberg table with row lineage");
    this.sparkCdcSchema =
        TypeUtil.join(
            cdcDataSchema(table),
            new Schema(MetadataColumns.CHANGE_TYPE, COMMIT_VERSION_FIELD, COMMIT_TIMESTAMP_FIELD));
    this.sparkCdcColumns = toColumns(sparkCdcSchema);
  }

  static Schema cdcDataSchema(Table table) {
    return TypeUtil.join(table.schema(), new Schema(ROW_ID_FIELD, ROW_VERSION_FIELD));
  }

  @Override
  public String name() {
    return table.name() + "." + TABLE_NAME;
  }

  @Override
  public StructType schema() {
    if (lazySparkSchema == null) {
      Schema schema =
          rangeOptions.readMode().isSparkCdc() ? sparkCdcSchema : icebergChangelogSchema;
      this.lazySparkSchema = SparkSchemaUtil.convert(schema);
    }

    return lazySparkSchema;
  }

  @Override
  public Column[] columns() {
    return rangeOptions.readMode().isSparkCdc()
        ? sparkCdcColumns
        : toColumns(icebergChangelogSchema);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public boolean containsCarryoverRows() {
    return true;
  }

  @Override
  public boolean containsIntermediateChanges() {
    return true;
  }

  @Override
  public boolean representsUpdateAsDeleteAndInsert() {
    return true;
  }

  @Override
  public NamedReference[] rowId() {
    return new NamedReference[] {Spark3Util.toNamedReference(MetadataColumns.ROW_ID.name())};
  }

  @Override
  public NamedReference rowVersion() {
    return Spark3Util.toNamedReference(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (!rangeOptions.readMode().isSparkCdc()) {
      return new SparkChangelogScanBuilder(spark(), table, icebergChangelogSchema, options);
    }

    Map<String, String> scanOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    scanOptions.putAll(options.asCaseSensitiveMap());
    scanOptions.putAll(rangeOptions.options());
    return new SparkChangelogScanBuilder(
        spark(),
        table,
        sparkCdcSchema,
        new CaseInsensitiveStringMap(scanOptions),
        rangeOptions.readMode());
  }

  private SparkSession spark() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return new MetadataColumn[] {
      SparkMetadataColumns.SPEC_ID,
      SparkMetadataColumns.partition(table),
      SparkMetadataColumns.FILE_PATH,
      SparkMetadataColumns.ROW_POSITION,
      SparkMetadataColumns.IS_DELETED,
    };
  }

  private static RangeOptions rangeOptions(Table table, ChangelogContext context) {
    ChangelogRange range = context.range();
    if (range instanceof ChangelogRange.UnboundedRange) {
      return RangeOptions.scan(Collections.emptyMap());
    } else if (range instanceof ChangelogRange.VersionRange) {
      return versionRangeOptions(table, (ChangelogRange.VersionRange) range);
    } else if (range instanceof ChangelogRange.TimestampRange) {
      return timestampRangeOptions(table, (ChangelogRange.TimestampRange) range);
    } else {
      throw new UnsupportedOperationException("Unsupported Spark changelog range: " + range);
    }
  }

  private static RangeOptions versionRangeOptions(Table table, ChangelogRange.VersionRange range) {
    Snapshot start = snapshotWithSequenceNumber(table, range.startingVersion());
    Snapshot end =
        range
            .endingVersion()
            .map(version -> snapshotWithSequenceNumber(table, version))
            .orElse(table.currentSnapshot());
    if (end == null) {
      return RangeOptions.empty();
    }

    Long startExclusive = range.startingBoundInclusive() ? start.parentId() : start.snapshotId();
    Long endInclusive = range.endingBoundInclusive() ? end.snapshotId() : end.parentId();
    return snapshotRangeOptions(table, startExclusive, endInclusive);
  }

  private static RangeOptions timestampRangeOptions(
      Table table, ChangelogRange.TimestampRange range) {
    List<Snapshot> snapshots = currentAncestorsInCommitOrder(table);
    Snapshot start =
        snapshots.stream()
            .filter(
                snapshot ->
                    range.startingBoundInclusive()
                        ? snapshot.timestampMillis() * 1000 >= range.startingTimestamp()
                        : snapshot.timestampMillis() * 1000 > range.startingTimestamp())
            .findFirst()
            .orElse(null);
    Snapshot end =
        snapshots.stream()
            .filter(
                snapshot ->
                    range.endingTimestamp().isEmpty()
                        || (range.endingBoundInclusive()
                            ? snapshot.timestampMillis() * 1000 <= range.endingTimestamp().get()
                            : snapshot.timestampMillis() * 1000 < range.endingTimestamp().get()))
            .reduce((left, right) -> right)
            .orElse(null);

    if (start == null || end == null || start.sequenceNumber() > end.sequenceNumber()) {
      return RangeOptions.empty();
    }

    return snapshotRangeOptions(table, start.parentId(), end.snapshotId());
  }

  private static RangeOptions snapshotRangeOptions(
      Table table, Long startExclusive, Long endInclusive) {
    if (endInclusive == null) {
      return RangeOptions.empty();
    }

    Snapshot end = table.snapshot(endInclusive);
    if (end == null) {
      return RangeOptions.empty();
    }

    if (startExclusive != null) {
      Snapshot start = table.snapshot(startExclusive);
      if (start != null && start.sequenceNumber() >= end.sequenceNumber()) {
        return RangeOptions.empty();
      }
    }

    Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (startExclusive != null) {
      options.put(SparkReadOptions.START_SNAPSHOT_ID, String.valueOf(startExclusive));
    }
    options.put(SparkReadOptions.END_SNAPSHOT_ID, String.valueOf(endInclusive));
    return RangeOptions.scan(options);
  }

  private static Snapshot snapshotWithSequenceNumber(Table table, String version) {
    long sequenceNumber;
    try {
      sequenceNumber = Long.parseLong(version);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid Iceberg snapshot sequence number: " + version, e);
    }

    return Lists.newArrayList(SnapshotUtil.currentAncestors(table)).stream()
        .filter(snapshot -> snapshot.sequenceNumber() == sequenceNumber)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Cannot find Iceberg snapshot with sequence number: " + version));
  }

  private static List<Snapshot> currentAncestorsInCommitOrder(Table table) {
    List<Snapshot> snapshots = Lists.newArrayList(SnapshotUtil.currentAncestors(table));
    Collections.reverse(snapshots);
    return snapshots;
  }

  private static Column[] toColumns(Schema schema) {
    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    return Arrays.stream(sparkSchema.fields())
        .map(field -> Column.create(field.name(), field.dataType(), field.nullable()))
        .toArray(Column[]::new);
  }

  private static class RangeOptions {
    private final Map<String, String> options;
    private final SparkChangelogReadMode readMode;

    private RangeOptions(Map<String, String> options, SparkChangelogReadMode readMode) {
      this.options = options;
      this.readMode = readMode;
    }

    private static RangeOptions icebergChangelog() {
      return new RangeOptions(Collections.emptyMap(), SparkChangelogReadMode.ICEBERG_CHANGELOG);
    }

    private static RangeOptions scan(Map<String, String> options) {
      return new RangeOptions(options, SparkChangelogReadMode.SPARK_CDC);
    }

    private static RangeOptions empty() {
      return new RangeOptions(Collections.emptyMap(), SparkChangelogReadMode.EMPTY_SPARK_CDC);
    }

    private Map<String, String> options() {
      return options;
    }

    private SparkChangelogReadMode readMode() {
      return readMode;
    }
  }
}
