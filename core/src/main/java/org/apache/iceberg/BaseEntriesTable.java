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
      // avoid returning an empty struct, which is not always supported.
      // instead, drop the partition field (id 102)
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

    String schemaString = SchemaParser.toJson(projectedSchema);
    String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

    return CloseableIterable.transform(
        filteredManifests,
        manifest ->
            new ManifestReadTask(
                table, manifest, projectedSchema, schemaString, specString, residuals));
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
          Integer fileContentId = intLit.value();
          if (fileContentId == FileContent.DATA.id()) {
            return manifestContentId == ManifestContent.DATA.id();
          } else {
            return manifestContentId == ManifestContent.DELETES.id();
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
        if (fileContent(ref)) {
          if (!literalSet.contains(manifestContentId)) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
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
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final Schema projection;
    private final Schema fileProjection;
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

      Type fileProjectionType = projection.findType("data_file");
      this.fileProjection =
          fileProjectionType != null
              ? new Schema(fileProjectionType.asStructType().fields())
              : new Schema();
    }

    @VisibleForTesting
    ManifestFile manifest() {
      return manifest;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      Types.NestedField readableMetricsField = projection.findField(MetricsUtil.READABLE_METRICS);

      if (readableMetricsField == null) {
        StructProjection structProjection = structProjection(projection);

        return CloseableIterable.transform(
            entries(fileProjection), entry -> structProjection.wrap((StructLike) entry));
      } else {
        Schema requiredFileProjection = requiredFileProjection();
        Schema actualProjection = removeReadableMetrics(projection, readableMetricsField);
        StructProjection structProjection = structProjection(actualProjection);

        return CloseableIterable.transform(
            entries(requiredFileProjection),
            entry -> withReadableMetrics(structProjection, entry, readableMetricsField));
      }
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
  }
}
