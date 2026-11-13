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

import java.util.Set;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.expressions.NamedReference;
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
  private final Schema dataSchema;

  public SparkChangelog(Table table, ChangelogContext context, CaseInsensitiveStringMap options) {
    super(table);
    this.context = Preconditions.checkNotNull(context, "Changelog context cannot be null");
    validateCdcOptions(options);
    SparkChangelogScan.validateRange(context);
    Preconditions.checkArgument(
        TableUtil.formatVersion(table) >= 2,
        "Spark CDC requires format version 2 or later for commit sequence numbers");
    boolean requiresRowLineage = SparkChangelogScan.requiresPostProcessing(context);
    Preconditions.checkArgument(
        !requiresRowLineage || TableUtil.supportsRowLineage(table),
        "Spark CDC post-processing requires row lineage. "
            + "Use deduplicationMode=none with computeUpdates=false for raw changes");
    // Raw CDC can expose older files whose lineage is unknown without inventing an identity.
    Schema lineageSchema =
        requiresRowLineage
            ? new Schema(
                MetadataColumns.ROW_ID.asRequired(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.asRequired())
            : new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    this.dataSchema = TypeUtil.join(table.schema(), lineageSchema);
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

  @Override
  Schema changelogSchema() {
    return TypeUtil.join(dataSchema, CDC_METADATA_SCHEMA);
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
    validateCdcOptions(options);
    SparkChangelogScanBuilder builder =
        new SparkChangelogScanBuilder(
            spark(), table(), dataSchema, changelogSchema(), context, options);
    return builder::build;
  }
}
