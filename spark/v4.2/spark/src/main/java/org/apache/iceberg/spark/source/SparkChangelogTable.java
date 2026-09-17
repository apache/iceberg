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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode;
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

  static final int COMMIT_VERSION_ID = Integer.MAX_VALUE - 109;
  static final int COMMIT_TIMESTAMP_ID = Integer.MAX_VALUE - 110;

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
          COMMIT_VERSION_ID,
          COMMIT_VERSION,
          Types.LongType.get(),
          "Iceberg snapshot sequence number");
  private static final Types.NestedField COMMIT_TIMESTAMP_FIELD =
      Types.NestedField.required(
          COMMIT_TIMESTAMP_ID,
          COMMIT_TIMESTAMP,
          Types.TimestampType.withZone(),
          "Iceberg snapshot commit timestamp");

  private final Table table;
  private final ChangelogContext context;
  private final String[] identifierColumns;
  private final Schema icebergChangelogSchema;
  private final SparkChangelogRange cdcRange;
  private final Schema sparkCdcSchema;
  private final Column[] sparkCdcColumns;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;

  public SparkChangelogTable(Table table) {
    this(table, null);
  }

  public SparkChangelogTable(Table table, ChangelogContext context) {
    this(table, context, CaseInsensitiveStringMap.empty());
  }

  public SparkChangelogTable(
      Table table, ChangelogContext context, CaseInsensitiveStringMap options) {
    this.table = table;
    this.context = context;
    this.identifierColumns = identifierColumns(table.schema(), options);
    Preconditions.checkArgument(
        identifierColumns.length == 0
            || (context != null
                && context.deduplicationMode() == DeduplicationMode.DROP_CARRYOVERS),
        "Business-key CDC requires deduplicationMode=dropCarryovers");
    this.icebergChangelogSchema = ChangelogUtil.changelogSchema(table.schema());
    this.cdcRange = context != null ? new SparkChangelogRange(context) : null;
    Preconditions.checkArgument(
        cdcRange == null || TableUtil.formatVersion(table) >= 2,
        "Spark CDC requires format version 2 or later for commit sequence numbers");
    Preconditions.checkArgument(
        cdcRange == null
            || !cdcRange.requiresPostProcessing()
            || identifierColumns.length > 0
            || TableUtil.supportsRowLineage(table),
        "Spark CDC post-processing requires row lineage. "
            + "Use deduplicationMode=none with computeUpdates=false for raw changes");
    this.sparkCdcSchema =
        cdcRange != null
            ? TypeUtil.join(
                cdcDataSchema(
                    table, identifierColumns.length == 0 && cdcRange.requiresPostProcessing()),
                new Schema(
                    MetadataColumns.CHANGE_TYPE, COMMIT_VERSION_FIELD, COMMIT_TIMESTAMP_FIELD))
            : null;
    this.sparkCdcColumns = cdcRange != null ? toColumns(sparkCdcSchema) : null;
  }

  private static String[] identifierColumns(Schema schema, CaseInsensitiveStringMap options) {
    String value = options.get(SparkReadOptions.CDC_IDENTIFIER_COLUMNS);
    if (value == null) {
      return new String[0];
    }

    List<String> names = Splitter.on(',').trimResults().splitToList(value);
    String[] resolved = new String[names.size()];
    boolean caseSensitive = SparkUtil.caseSensitive(SparkSession.active());
    Set<String> seen = Sets.newHashSet();
    for (int index = 0; index < names.size(); index++) {
      String name = names.get(index);
      Types.NestedField field =
          caseSensitive
              ? schema.asStruct().field(name)
              : schema.asStruct().caseInsensitiveField(name);
      Preconditions.checkArgument(
          field != null && field.type().isPrimitiveType(),
          "CDC identifier column must be an existing top-level primitive field: %s",
          name);
      Preconditions.checkArgument(
          seen.add(field.name()), "Duplicate CDC identifier column: %s", name);
      resolved[index] = field.name();
    }

    return resolved;
  }

  /** Identifier fields consumed by the Iceberg CDC resolution rule. */
  public String[] identifierColumns() {
    return identifierColumns.clone();
  }

  /** Raw input for the Iceberg business-key CDC resolution rule. */
  public SparkChangelogTable rawChangelog() {
    Preconditions.checkState(identifierColumns.length > 0, "Not a business-key changelog");
    return new SparkChangelogTable(
        table, new ChangelogContext(context.range(), DeduplicationMode.NONE, false));
  }

  static Schema cdcDataSchema(Table table) {
    return cdcDataSchema(table, true);
  }

  private static Schema cdcDataSchema(Table table, boolean requiresRowLineage) {
    // Raw CDC can expose older files whose lineage is unknown without inventing an identity.
    Schema lineageSchema =
        requiresRowLineage
            ? new Schema(ROW_ID_FIELD, ROW_VERSION_FIELD)
            : new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    return TypeUtil.join(table.schema(), lineageSchema);
  }

  static Schema dropCdcMetadata(Schema schema) {
    return TypeUtil.selectNot(
        schema,
        Set.of(MetadataColumns.CHANGE_TYPE.fieldId(), COMMIT_VERSION_ID, COMMIT_TIMESTAMP_ID));
  }

  @Override
  public String name() {
    return table.name() + "." + TABLE_NAME;
  }

  @Override
  public StructType schema() {
    if (lazySparkSchema == null) {
      Schema schema = cdcRange != null ? sparkCdcSchema : icebergChangelogSchema;
      this.lazySparkSchema = SparkSchemaUtil.convert(schema);
    }

    return lazySparkSchema;
  }

  @Override
  public Column[] columns() {
    return cdcRange != null ? sparkCdcColumns : toColumns(icebergChangelogSchema);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return cdcRange != null ? CAPABILITIES : ImmutableSet.of(TableCapability.BATCH_READ);
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
    if (cdcRange == null) {
      return new SparkChangelogScanBuilder(spark(), table, icebergChangelogSchema, options);
    }

    return new SparkChangelogScanBuilder(spark(), table, sparkCdcSchema, options, cdcRange);
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

  private static Column[] toColumns(Schema schema) {
    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    return Arrays.stream(sparkSchema.fields())
        .map(field -> Column.create(field.name(), field.dataType(), field.nullable()))
        .toArray(Column[]::new);
  }
}
