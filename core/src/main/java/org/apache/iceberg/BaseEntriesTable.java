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
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructProjection;

/** Base class logic for entries metadata tables */
abstract class BaseEntriesTable extends BaseMetadataTable {

  BaseEntriesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = ManifestEntry.getSchema(partitionType);
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition
      // field (id 102)
      schema = TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    }

    return TypeUtil.join(schema, MetricsUtil.readableMetricsSchema(table().schema(), schema));
  }

  static CloseableIterable<FileScanTask> planFiles(
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

    return CloseableIterable.transform(
        filteredManifests,
        manifest ->
            new ManifestReadTask(
                table, manifest, projectedSchema, schemaString, specString, residuals));
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final Schema projection;
    private final Schema fileSchema;
    private final Schema dataTableSchema;
    private final FileIO io;
    private final ManifestFile manifest;
    private final Map<Integer, PartitionSpec> specsById;

    ManifestReadTask(
        Table table,
        ManifestFile manifest,
        Schema projection,
        String schemaString,
        String specString,
        ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.projection = projection;
      this.io = table.io();
      this.manifest = manifest;
      this.specsById = Maps.newHashMap(table.specs());
      this.dataTableSchema = table.schema();

      Type fileProjection = projection.findType("data_file");
      this.fileSchema =
          fileProjection != null
              ? new Schema(fileProjection.asStructType().fields())
              : new Schema();
    }

    @VisibleForTesting
    ManifestFile manifest() {
      return manifest;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      Types.NestedField readableMetricsField = projection.findField(MetricsUtil.READABLE_METRICS);
      // Project data-file fields
      CloseableIterable<StructLike> prunedRows;
      if (readableMetricsField == null) {
        if (manifest.content() == ManifestContent.DATA) {
          prunedRows =
              CloseableIterable.transform(
                  ManifestFiles.read(manifest, io, specsById).project(fileSchema).entries(),
                  entry -> (GenericManifestEntry<DataFile>) entry);
        } else {
          prunedRows =
              CloseableIterable.transform(
                  ManifestFiles.readDeleteManifest(manifest, io, specsById)
                      .project(fileSchema)
                      .entries(),
                  entry -> (GenericManifestEntry<DeleteFile>) entry);
        }

        // Project non-readable fields
        Schema readSchema = ManifestEntry.wrapFileSchema(fileSchema.asStruct());
        StructProjection structProjection = StructProjection.create(readSchema, this.projection);
        return CloseableIterable.transform(prunedRows, structProjection::wrap);
      } else {
        Set<Integer> readableMetricsIds = TypeUtil.getProjectedIds(readableMetricsField.type());
        int metricsPosition = projection.columns().indexOf(readableMetricsField);
        // projection minus virtual column such as readableMetrics
        Schema projectedSchemaMinusReadableMetrics =
            TypeUtil.selectNot(projection, readableMetricsIds);
        // Remove virtual columns from the file projection and ensure that the underlying metrics
        // used to create those columns are part of the file projection
        Schema projectionForReadableMetrics =
            new Schema(
                MetricsUtil.READABLE_METRIC_COLS.stream()
                    .map(MetricsUtil.ReadableMetricColDefinition::originalCol)
                    .collect(Collectors.toList()));

        Schema projectionForMetrics = TypeUtil.join(fileSchema, projectionForReadableMetrics);
        return CloseableIterable.transform(
            entries(projectionForMetrics),
            entry ->
                withReadableMetrics(projectedSchemaMinusReadableMetrics, entry, metricsPosition));
      }
    }

    private CloseableIterable<? extends ManifestEntry<? extends ContentFile<?>>> entries(
        Schema fileProjection) {
      switch (manifest.content()) {
        case DATA:
          return ManifestFiles.read(manifest, io, specsById).project(fileProjection).entries();
        case DELETES:
          return ManifestFiles.readDeleteManifest(manifest, io, specsById)
              .project(fileProjection)
              .entries();
        default:
          throw new IllegalArgumentException(
              "Unsupported manifest content type:" + manifest.content());
      }
    }

    private StructLike withReadableMetrics(
        Schema projectedSchema,
        ManifestEntry<? extends ContentFile<?>> entry,
        int metricsPosition) {
      int projectionColumnCount = projection.columns().size();
      StructType projectedMetricType =
          projection.findField(MetricsUtil.READABLE_METRICS).type().asStructType();
      MetricsUtil.ReadableMetricsStruct readableMetrics =
          MetricsUtil.readableMetricsStruct(dataTableSchema, entry.file(), projectedMetricType);

      Schema manifestEntrySchema = ManifestEntry.wrapFileSchema(fileSchema.asStruct());
      StructProjection entryStruct =
          StructProjection.create(manifestEntrySchema, projectedSchema).wrap((StructLike) entry);

      return new ManifestEntryStructWithMetrics(
          projectionColumnCount, metricsPosition, entryStruct, readableMetrics);
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }
  }

  static class ManifestEntryStructWithMetrics implements StructLike {
    private final StructProjection entryAsStruct;
    private final MetricsUtil.ReadableMetricsStruct readableMetrics;
    private final int projectionColumnCount;
    private final int metricsPosition;

    ManifestEntryStructWithMetrics(
        int projectionColumnCount,
        int metricsPosition,
        StructProjection entryAsStruct,
        MetricsUtil.ReadableMetricsStruct readableMetrics) {
      this.entryAsStruct = entryAsStruct;
      this.readableMetrics = readableMetrics;
      this.projectionColumnCount = projectionColumnCount;
      this.metricsPosition = metricsPosition;
    }

    @Override
    public int size() {
      return projectionColumnCount;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos < metricsPosition) {
        return entryAsStruct.get(pos, javaClass);
      } else if (pos == metricsPosition) {
        return javaClass.cast(readableMetrics);
      } else {
        // columnCount = fileAsStruct column count + the readable metrics field.
        // When pos is greater than metricsPosition, the actual position of the field in
        // fileAsStruct should be subtracted by 1.
        return entryAsStruct.get(pos - 1, javaClass);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("ManifestEntryStructWithMetrics is read only");
    }
  }
}
