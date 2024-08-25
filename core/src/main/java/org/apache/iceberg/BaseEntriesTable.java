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
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
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
    if (partitionType.fields().isEmpty()) {
      // avoid returning an empty struct, which is not always supported.
      // instead, drop the partition field (id 102)
      schema = TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    }

    return TypeUtil.join(schema, MetricsUtil.readableMetricsSchema(table().schema(), schema));
  }

  static CloseableIterable<FileScanTask> planFiles(
      Table table,
      Snapshot snapshot,
      CloseableIterable<ManifestFile> manifests,
      Schema tableSchema,
      Schema projectedSchema,
      TableScanContext context) {
    Expression rowFilter = context.rowFilter();
    boolean caseSensitive = context.caseSensitive();
    boolean ignoreResiduals = context.ignoreResiduals();
    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;

    LoadingCache<Integer, ManifestEvaluator> evalCache =
        Caffeine.newBuilder()
            .build(
                specId -> {
                  PartitionSpec spec = table.specs().get(specId);
                  PartitionSpec transformedSpec = BaseFilesTable.transformSpec(tableSchema, spec);
                  return ManifestEvaluator.forRowFilter(rowFilter, transformedSpec, caseSensitive);
                });
    ManifestContentEvaluator manifestContentEvaluator =
        new ManifestContentEvaluator(filter, tableSchema.asStruct(), caseSensitive);

    CloseableIterable<ManifestFile> filteredManifests =
        CloseableIterable.filter(
            manifests,
            manifest ->
                evalCache.get(manifest.partitionSpecId()).eval(manifest)
                    && manifestContentEvaluator.eval(manifest));

    return CloseableIterable.transform(
        filteredManifests,
        manifest -> new ManifestReadTask(table, snapshot, manifest, projectedSchema, filter));
  }

  /**
   * Evaluates an {@link Expression} on a {@link ManifestFile} to test whether a given data or
   * delete manifests shall be included in the scan
   */
  private static class ManifestContentEvaluator {

    private final Expression boundExpr;

    private ManifestContentEvaluator(
        Expression expr, Types.StructType structType, boolean caseSensitive) {
      Expression rewritten = Expressions.rewriteNot(expr);
      this.boundExpr = Binder.bind(structType, rewritten, caseSensitive);
    }

    private boolean eval(ManifestFile manifest) {
      return new ManifestEvalVisitor().eval(manifest);
    }

    private class ManifestEvalVisitor extends ExpressionVisitors.BoundExpressionVisitor<Boolean> {

      private int manifestContentId;

      private static final boolean ROWS_MIGHT_MATCH = true;
      private static final boolean ROWS_CANNOT_MATCH = false;

      private boolean eval(ManifestFile manifestFile) {
        this.manifestContentId = manifestFile.content().id();
        return ExpressionVisitors.visitEvaluator(boundExpr, this);
      }

      @Override
      public Boolean alwaysTrue() {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public Boolean alwaysFalse() {
        return ROWS_CANNOT_MATCH;
      }

      @Override
      public Boolean not(Boolean result) {
        return !result;
      }

      @Override
      public Boolean and(Boolean leftResult, Boolean rightResult) {
        return leftResult && rightResult;
      }

      @Override
      public Boolean or(Boolean leftResult, Boolean rightResult) {
        return leftResult || rightResult;
      }

      @Override
      public <T> Boolean isNull(BoundReference<T> ref) {
        if (fileContent(ref)) {
          return ROWS_CANNOT_MATCH; // date_file.content should not be null
        } else {
          return ROWS_MIGHT_MATCH;
        }
      }

      @Override
      public <T> Boolean notNull(BoundReference<T> ref) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean isNaN(BoundReference<T> ref) {
        if (fileContent(ref)) {
          return ROWS_CANNOT_MATCH; // date_file.content should not be nan
        } else {
          return ROWS_MIGHT_MATCH;
        }
      }

      @Override
      public <T> Boolean notNaN(BoundReference<T> ref) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
        if (fileContent(ref)) {
          Literal<Integer> intLit = lit.to(Types.IntegerType.get());
          if (!contentMatch(intLit.value())) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
        if (fileContent(ref)) {
          Literal<Integer> intLit = lit.to(Types.IntegerType.get());
          if (contentMatch(intLit.value())) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
        if (fileContent(ref)) {
          if (literalSet.stream().noneMatch(lit -> contentMatch((Integer) lit))) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
        if (fileContent(ref)) {
          if (literalSet.stream().anyMatch(lit -> contentMatch((Integer) lit))) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      private <T> boolean fileContent(BoundReference<T> ref) {
        return ref.fieldId() == DataFile.CONTENT.fieldId();
      }

      private boolean contentMatch(Integer fileContentId) {
        if (FileContent.DATA.id() == fileContentId) {
          return ManifestContent.DATA.id() == manifestContentId;
        } else if (FileContent.EQUALITY_DELETES.id() == fileContentId
            || FileContent.POSITION_DELETES.id() == fileContentId) {
          return ManifestContent.DELETES.id() == manifestContentId;
        } else {
          return false;
        }
      }
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final Schema projection;
    private final Schema fileProjection;
    private final Schema dataTableSchema;
    private final FileIO io;
    private final Snapshot snapshot;
    private final ManifestFile manifest;
    private final Map<Integer, PartitionSpec> specsById;

    private ManifestReadTask(
        Table table,
        Snapshot snapshot,
        ManifestFile manifest,
        Schema projection,
        Expression filter) {
      this(table.schema(), table.io(), table.specs(), snapshot, manifest, projection, filter);
    }

    ManifestReadTask(
        Schema dataTableSchema,
        FileIO io,
        Map<Integer, PartitionSpec> specsById,
        Snapshot snapshot,
        ManifestFile manifest,
        Schema projection,
        Expression filter) {
      super(
          DataFiles.fromManifest(manifest),
          null,
          SchemaParser.toJson(projection),
          PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
          ResidualEvaluator.unpartitioned(filter));
      this.projection = projection;
      this.io = io;
      this.snapshot = snapshot;
      this.manifest = manifest;
      this.specsById = Maps.newHashMap(specsById);
      this.dataTableSchema = dataTableSchema;

      Type fileProjectionType = projection.findType("data_file");
      this.fileProjection =
          fileProjectionType != null
              ? new Schema(fileProjectionType.asStructType().fields())
              : new Schema();
    }

    @Override
    public long estimatedRowsCount() {
      return (long) manifest.addedFilesCount()
          + (long) manifest.deletedFilesCount()
          + (long) manifest.existingFilesCount();
    }

    ManifestFile manifest() {
      return manifest;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      Types.NestedField refSnapshotIdField = projection.findField(MetricsUtil.REF_SNAPSHOT_ID);
      Types.NestedField refSnapshotTimestampMillisField =
          projection.findField(MetricsUtil.REF_SNAPSHOT_TIMESTAMP_MILLIS);
      Types.NestedField readableMetricsField = projection.findField(MetricsUtil.READABLE_METRICS);

      Schema actualProjection =
          actualProjection(
              projection,
              refSnapshotIdField,
              refSnapshotTimestampMillisField,
              readableMetricsField);
      StructProjection structProjection = structProjection(actualProjection);

      CloseableIterable<StructLike> rows;
      if (readableMetricsField == null) {
        rows =
            CloseableIterable.transform(
                entries(fileProjection), entry -> structProjection.wrap((StructLike) entry));
      } else {
        Schema requiredFileProjection = requiredFileProjection();
        rows =
            CloseableIterable.transform(
                entries(requiredFileProjection),
                entry -> withReadableMetrics(structProjection, entry, readableMetricsField));
      }
      if (refSnapshotIdField != null || refSnapshotTimestampMillisField != null) {
        return CloseableIterable.transform(
            rows,
            r ->
                new MetricsUtil.StructWithRefSnapshot(
                    r, projection, snapshot, refSnapshotIdField, refSnapshotTimestampMillisField));
      }

      return rows;
    }

    private Schema actualProjection(
        Schema projection,
        Types.NestedField refSnapshotIdField,
        Types.NestedField refSnapshotTimestampField,
        Types.NestedField readableMetricsField) {
      Schema actualProjection = projection;

      if (refSnapshotIdField != null) {
        actualProjection =
            TypeUtil.selectNot(actualProjection, Sets.newHashSet(refSnapshotIdField.fieldId()));
      }

      if (refSnapshotTimestampField != null) {
        actualProjection =
            TypeUtil.selectNot(
                actualProjection, Sets.newHashSet(refSnapshotTimestampField.fieldId()));
      }

      if (readableMetricsField != null) {
        actualProjection = removeReadableMetrics(actualProjection, readableMetricsField);
      }

      return actualProjection;
    }

    /**
     * Ensure that the underlying metrics used to populate readable metrics column are part of the
     * file projection.
     */
    private Schema requiredFileProjection() {
      Schema projectionForReadableMetrics =
          new Schema(
              MetricsUtil.READABLE_METRIC_COLS.stream()
                  .map(MetricsUtil.ReadableMetricColDefinition::originalCol)
                  .collect(Collectors.toList()));
      return TypeUtil.join(fileProjection, projectionForReadableMetrics);
    }

    private Schema removeReadableMetrics(
        Schema projectionSchema, Types.NestedField readableMetricsField) {
      Set<Integer> readableMetricsIds = TypeUtil.getProjectedIds(readableMetricsField.type());
      return TypeUtil.selectNot(projectionSchema, readableMetricsIds);
    }

    private StructProjection structProjection(Schema projectedSchema) {
      Schema manifestEntrySchema = ManifestEntry.wrapFileSchema(fileProjection.asStruct());
      return StructProjection.create(manifestEntrySchema, projectedSchema);
    }

    /**
     * @param fileStructProjection projection to apply on the 'data_files' struct
     * @return entries of this read task's manifest
     */
    private CloseableIterable<? extends ManifestEntry<? extends ContentFile<?>>> entries(
        Schema fileStructProjection) {
      return ManifestFiles.open(manifest, io, specsById).project(fileStructProjection).entries();
    }

    /**
     * Given a manifest entry and its projection, append a 'readable_metrics' column that returns
     * the entry's metrics in human-readable form.
     *
     * @param entry manifest entry
     * @param structProjection projection to apply on the manifest entry
     * @param readableMetricsField projected "readable_metrics" field
     * @return struct representing projected manifest entry, with appended readable_metrics field
     */
    private StructLike withReadableMetrics(
        StructProjection structProjection,
        ManifestEntry<? extends ContentFile<?>> entry,
        Types.NestedField readableMetricsField) {
      StructProjection struct = structProjection.wrap((StructLike) entry);
      int structSize = projection.columns().size();

      MetricsUtil.ReadableMetricsStruct readableMetrics =
          readableMetrics(entry.file(), readableMetricsField);
      int metricsPosition = projection.columns().indexOf(readableMetricsField);

      return new MetricsUtil.StructWithReadableMetrics(
          struct, structSize, readableMetrics, metricsPosition);
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

    FileIO io() {
      return io;
    }

    Map<Integer, PartitionSpec> specsById() {
      return specsById;
    }

    Schema dataTableSchema() {
      return dataTableSchema;
    }

    Schema projection() {
      return projection;
    }
  }
}
