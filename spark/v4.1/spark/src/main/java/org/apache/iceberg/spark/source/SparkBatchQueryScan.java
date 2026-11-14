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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.data.SparkVariantExtractionUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.types.VariantType$;

class SparkBatchQueryScan extends SparkRuntimeFilterableScan {

  private final Snapshot snapshot;
  private final String branch;
  // All extractions Spark passed to pushVariantExtractions (in original order).
  private final VariantExtraction[] allVariantExtractions;
  // Parallel mask: true iff the corresponding extraction was accepted by this datasource.
  private final boolean[] variantExtractionAccepted;

  SparkBatchQueryScan(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      VariantExtraction[] allVariantExtractions,
      boolean[] variantExtractionAccepted,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, schema, scan, readConf, projection, filters, scanReportSupplier);
    this.snapshot = snapshot;
    this.branch = branch;
    this.allVariantExtractions = allVariantExtractions;
    this.variantExtractionAccepted = variantExtractionAccepted;
  }

  Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  @Override
  public StructType readSchema() {
    if (allVariantExtractions.length == 0) {
      return super.readSchema();
    }

    // Check whether any extraction was accepted.
    boolean hasAcceptedExtraction = false;
    for (boolean b : variantExtractionAccepted) {
      if (b) {
        hasAcceptedExtraction = true;
        break;
      }
    }
    final boolean anyAccepted = hasAcceptedExtraction;

    // Group extractions by top-level variant column in Spark batch order. List index per column
    // matches the ordinal Spark uses in GetStructField(ref, ordinal) after
    // buildScanWithPushedVariants.
    Map<String, List<PushedVariantSlot>> pushedSlotsByColumn = new LinkedHashMap<>();
    for (int i = 0; i < allVariantExtractions.length; i++) {
      String colName = allVariantExtractions[i].columnName()[0];
      List<PushedVariantSlot> slots =
          pushedSlotsByColumn.computeIfAbsent(colName, k -> Lists.newArrayList());
      slots.add(new PushedVariantSlot(allVariantExtractions[i], variantExtractionAccepted[i]));
    }

    StructType base = super.readSchema();
    StructField[] newFields =
        Arrays.stream(base.fields())
            .map(
                field -> {
                  List<PushedVariantSlot> slots = pushedSlotsByColumn.get(field.name());
                  if (slots == null) {
                    return field;
                  }

                  // Emit one struct field per Spark ordinal. Accepted extractions carry
                  // VariantMetadata so the Parquet reader maps them to typed_value.* shredded
                  // columns.
                  // Rejected extractions are VariantType placeholders so ordinals in the plan
                  // remain
                  // valid even when only a subset of slots are accepted.
                  StructField[] extracted = new StructField[slots.size()];
                  int acceptedCount = 0;
                  for (int ordinal = 0; ordinal < slots.size(); ordinal++) {
                    PushedVariantSlot slot = slots.get(ordinal);
                    VariantExtraction extraction = slot.extraction();
                    if (slot.accepted()) {
                      acceptedCount++;
                      extracted[ordinal] =
                          DataTypes.createStructField(
                              String.valueOf(ordinal),
                              extraction.expectedDataType(),
                              true,
                              extraction.metadata());
                    } else {
                      // Rejected extraction: use VariantType so Spark can fall back to full
                      // variant evaluation for this ordinal slot.
                      extracted[ordinal] =
                          DataTypes.createStructField(
                              String.valueOf(ordinal), VariantType$.MODULE$, true);
                    }
                  }

                  // Mirror ParquetScan: IsNotNull-only pushdown uses a boolean placeholder field
                  // instead of an all-VariantType struct (see Spark ParquetScan.scala).
                  if (acceptedCount == 0
                      && extracted.length == 1
                      && slots.get(0).extraction().expectedDataType() instanceof VariantType) {
                    Metadata placeholderMetadata =
                        new MetadataBuilder()
                            .putMetadata(
                                SparkVariantExtractionUtil.VARIANT_METADATA_KEY,
                                new MetadataBuilder()
                                    .putString("path", SparkVariantExtractionUtil.PLACEHOLDER_PATH)
                                    .putBoolean("failOnError", false)
                                    .putString("timeZoneId", "UTC")
                                    .build())
                            .build();
                    extracted[0] =
                        DataTypes.createStructField(
                            "0", DataTypes.BooleanType, true, placeholderMetadata);
                  } else if (!anyAccepted) {
                    // No typed extractions were accepted; keep the original variant column type.
                    return field;
                  }

                  // Use empty metadata for the rewritten top-level variant field. The original
                  // Iceberg field metadata (fieldId) must not be inherited here because Spark's
                  // Alias(realAttr, name)(expectedExprId) in buildScanWithPushedVariants carries
                  // realAttr.metadata as its own metadata. RemoveRedundantAliases strips the alias
                  // when alias.metadata == attr.metadata — if both carry the fieldId metadata the
                  // alias is removed, leaving a stale exprId in the plan and causing
                  // PLAN_VALIDATION_FAILED. Using empty metadata breaks that equality so the alias
                  // is preserved and the correct exprId binding is maintained.
                  return DataTypes.createStructField(
                      field.name(),
                      DataTypes.createStructType(extracted),
                      field.nullable(),
                      org.apache.spark.sql.types.Metadata$.MODULE$.empty());
                })
            .toArray(StructField[]::new);

    return DataTypes.createStructType(newFields);
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(snapshot);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && Objects.equals(snapshot, that.snapshot)
        && Objects.equals(branch, that.branch)
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filtersDesc().equals(that.filtersDesc())
        && runtimeFiltersDesc().equals(that.runtimeFiltersDesc());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        table().uuid(),
        snapshot,
        branch,
        readSchema(),
        filtersDesc(),
        runtimeFiltersDesc());
  }

  @Override
  public String description() {
    return String.format(
        "IcebergScan(table=%s, schemaId=%s, snapshotId=%s, branch=%s, filters=%s, runtimeFilters=%s, groupedBy=%s)",
        table(),
        schema().schemaId(),
        snapshotId(),
        branch,
        filtersDesc(),
        runtimeFiltersDesc(),
        groupingKeyDesc());
  }

  /** One pushed variant extraction for a top-level variant column. */
  private static final class PushedVariantSlot {
    private final VariantExtraction extraction;
    private final boolean accepted;

    private PushedVariantSlot(VariantExtraction extraction, boolean accepted) {
      this.extraction = extraction;
      this.accepted = accepted;
    }

    private VariantExtraction extraction() {
      return extraction;
    }

    private boolean accepted() {
      return accepted;
    }
  }
}
