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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/** Base class logic for files metadata tables */
abstract class BaseFilesTable extends BaseMetadataTable {

  BaseFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public Schema schema() {
    // avoid returning an empty struct, which is not always supported.
    // instead, drop the partition field
    boolean dropPartitionColumnForUnpartitioned = true;
    return schemaInternal(table(), dropPartitionColumnForUnpartitioned);
  }

  private static Schema schemaInternal(Table table, boolean dropPartitionColumnForUnpartitioned) {
    StructType partitionType = Partitioning.partitionType(table);
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (dropPartitionColumnForUnpartitioned && partitionType.fields().isEmpty()) {
      schema = TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    }

    return withDerivedColumns(table.schema(), schema);
  }

  private static Schema withDerivedColumns(Schema baseTableSchema, Schema meteTableSchema) {
    Schema metadataTableSchema =
        TypeUtil.join(meteTableSchema, dataSequenceNumberSchema(meteTableSchema));

    return TypeUtil.join(
        metadataTableSchema,
        MetricsUtil.readableMetricsSchema(baseTableSchema, metadataTableSchema));
  }

  private static Schema dataSequenceNumberSchema(Schema schema) {
    return new Schema(
        NestedField.optional(
            schema.highestFieldId() + 1,
            MetadataTableUtils.DATA_SEQUENCE_NUMBER,
            Types.LongType.get()));
  }

  private static CloseableIterable<FileScanTask> planFiles(
      Table table,
      CloseableIterable<ManifestFile> manifests,
      Schema tableSchema,
      Schema projectedSchema,
      TableScanContext context) {
    Expression rowFilter = context.rowFilter();
    boolean caseSensitive = context.caseSensitive();
    boolean ignoreResiduals = context.ignoreResiduals();

    LoadingCache<Integer, ManifestEvaluator> evalCache =
        Caffeine.newBuilder()
            .build(
                specId -> {
                  PartitionSpec spec = table.specs().get(specId);
                  PartitionSpec transformedSpec = BaseFilesTable.transformSpec(tableSchema, spec);
                  return ManifestEvaluator.forRowFilter(rowFilter, transformedSpec, caseSensitive);
                });

    CloseableIterable<ManifestFile> filteredManifests =
        CloseableIterable.filter(
            manifests, manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

    String schemaString = SchemaParser.toJson(projectedSchema);
    String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

    // metadata schema will represent the files schema and indifferent to if table is partitioned
    Schema metadataSchema = schemaInternal(table, false);

    return CloseableIterable.transform(
        filteredManifests,
        manifest ->
            new ManifestReadTask(
                table,
                manifest,
                metadataSchema,
                projectedSchema,
                schemaString,
                specString,
                residuals));
  }

  abstract static class BaseFilesTableScan extends BaseMetadataTableScan {

    protected BaseFilesTableScan(Table table, Schema schema, MetadataTableType tableType) {
      super(table, schema, tableType);
    }

    protected BaseFilesTableScan(
        Table table, Schema schema, MetadataTableType tableType, TableScanContext context) {
      super(table, schema, tableType, context);
    }

    /** Returns an iterable of manifest files to explore for this files metadata table scan */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  abstract static class BaseAllFilesTableScan extends BaseAllMetadataTableScan {

    protected BaseAllFilesTableScan(Table table, Schema schema, MetadataTableType tableType) {
      super(table, schema, tableType);
    }

    protected BaseAllFilesTableScan(
        Table table, Schema schema, MetadataTableType tableType, TableScanContext context) {
      super(table, schema, tableType, context);
    }

    /** Returns an iterable of manifest files to explore for this all files metadata table scan */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {

    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final ManifestFile manifest;
    private final Schema dataTableSchema;
    private final Schema metadataSchema;
    private final Schema projection;

    ManifestReadTask(
        Table table,
        ManifestFile manifest,
        Schema metadataSchema,
        Schema projection,
        String schemaString,
        String specString,
        ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.metadataSchema = metadataSchema;
      this.io = table.io();
      this.specsById = Maps.newHashMap(table.specs());
      this.manifest = manifest;
      this.dataTableSchema = table.schema();
      this.projection = projection;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      if (projectionWithComputedColumns(projection)) {
        return CloseableIterable.transform(files(), this::withComputedColumns);
      } else {
        return CloseableIterable.transform(files(projection), file -> (StructLike) file);
      }
    }

    private boolean projectionWithComputedColumns(Schema requestedProjection) {
      Types.NestedField readableMetricsField =
          requestedProjection.findField(MetricsUtil.READABLE_METRICS);
      Types.NestedField dataSequenceNumberField =
          requestedProjection.findField(MetadataTableUtils.DATA_SEQUENCE_NUMBER);
      return readableMetricsField != null || dataSequenceNumberField != null;
    }

    private CloseableIterable<? extends ContentFile<?>> files(Schema fileProjection) {
      return ManifestFiles.open(manifest, io, specsById).project(fileProjection);
    }

    private CloseableIterable<? extends ContentFile<?>> files() {
      return ManifestFiles.open(manifest, io, specsById);
    }

    /**
     * Given a content file metadata, append computed columns
     *
     * <ul>
     *   <li>readable_metrics: file's metrics in human-readable form
     *   <li>data_sequence_number: data sequence number assigned to file on commit
     * </ul>
     *
     * @param file content file metadata
     * @return result content with appended computed columns
     */
    private StructLike withComputedColumns(ContentFile<?> file) {
      Types.NestedField readableMetricsField = projection.findField(MetricsUtil.READABLE_METRICS);
      StructLike readAbleMetricsStruct =
          readableMetricsField == null
              ? EmptyStructLike.get()
              : readableMetrics(file, readableMetricsField);
      return new MetadataTableUtils.StructWithComputedColumns(
          metadataSchema,
          projection,
          (StructLike) file,
          file.dataSequenceNumber(),
          readAbleMetricsStruct);
    }

    private MetricsUtil.ReadableMetricsStruct readableMetrics(
        ContentFile<?> file, Types.NestedField readableMetricsField) {
      StructType projectedMetricType = readableMetricsField.type().asStructType();
      return MetricsUtil.readableMetricsStruct(dataTableSchema, file, projectedMetricType);
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }

    @VisibleForTesting
    ManifestFile manifest() {
      return manifest;
    }
  }
}
