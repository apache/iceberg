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
package org.apache.iceberg.parquet;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.NaNUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

class ParquetMetrics {
  private ParquetMetrics() {}

  static Iterable<FieldMetrics<?>> fieldMetrics(
      Schema schema,
      MessageType type,
      MetricsConfig metricsConfig,
      ParquetMetadata metadata,
      Stream<FieldMetrics<?>> fields) {
    Multimap<ColumnPath, ColumnChunkMetaData> columns =
        Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        columns.put(column.getPath(), column);
      }
    }

    Map<Integer, FieldMetrics<?>> metricsById =
        fields.collect(Collectors.toMap(FieldMetrics::id, Function.identity()));

    return TypeWithSchemaVisitor.visit(
        schema.asStruct(), type, new MetricsVisitor(schema, metricsConfig, metricsById, columns));
  }

  private static long rowCount(ParquetMetadata metadata) {
    long rowCount = 0L;
    for (BlockMetaData block : metadata.getBlocks()) {
      rowCount += block.getRowCount();
    }

    return rowCount;
  }

  private static Map<Integer, Long> columnSizes(
      Schema schema, MessageType type, ParquetMetadata metadata, MetricsConfig metricsConfig) {
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        Type.ID id =
            type.getColumnDescription(column.getPath().toArray()).getPrimitiveType().getId();
        if (id != null) {
          int fieldId = id.intValue();
          MetricsModes.MetricsMode mode = MetricsUtil.metricsMode(schema, metricsConfig, fieldId);
          if (mode != MetricsModes.None.get()) {
            columnSizes.put(fieldId, columnSizes.getOrDefault(fieldId, 0L) + column.getTotalSize());
          }
        }
      }
    }

    return columnSizes;
  }

  static Metrics metrics(
      Schema schema,
      MessageType type,
      MetricsConfig metricsConfig,
      ParquetMetadata metadata,
      Stream<FieldMetrics<?>> fields) {
    long rowCount = rowCount(metadata);
    Map<Integer, Long> columnSizes = columnSizes(schema, type, metadata, metricsConfig);

    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    Map<Integer, org.apache.iceberg.types.Type> originalTypes = Maps.newHashMap();

    for (FieldMetrics<?> metrics : fieldMetrics(schema, type, metricsConfig, metadata, fields)) {
      int id = metrics.id();
      if (null != metrics.originalType()) {
        originalTypes.put(id, metrics.originalType());
      }

      if (metrics.valueCount() >= 0) {
        valueCounts.put(id, metrics.valueCount());
      }

      if (metrics.nullValueCount() >= 0) {
        nullValueCounts.put(id, metrics.nullValueCount());
      }

      if (metrics.nanValueCount() >= 0) {
        nanValueCounts.put(id, metrics.nanValueCount());
      }

      if (metrics.lowerBound() != null) {
        ByteBuffer lowerBound =
            Conversions.toByteBuffer(metrics.originalType(), metrics.lowerBound());
        lowerBounds.put(id, lowerBound);
      }

      if (metrics.upperBound() != null) {
        ByteBuffer upperBound =
            Conversions.toByteBuffer(metrics.originalType(), metrics.upperBound());
        upperBounds.put(id, upperBound);
      }
    }

    return new Metrics(
        rowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds,
        originalTypes);
  }

  private static class MetricsVisitor extends TypeWithSchemaVisitor<Iterable<FieldMetrics<?>>> {
    private final Schema schema;
    private final MetricsConfig metricsConfig;
    private final Map<Integer, FieldMetrics<?>> metricsById;
    private final Multimap<ColumnPath, ColumnChunkMetaData> columns;

    private MetricsVisitor(
        Schema schema,
        MetricsConfig metricsConfig,
        Map<Integer, FieldMetrics<?>> metricsById,
        Multimap<ColumnPath, ColumnChunkMetaData> columns) {
      this.schema = schema;
      this.metricsConfig = metricsConfig;
      this.metricsById = metricsById;
      this.columns = columns;
    }

    @Override
    public Iterable<FieldMetrics<?>> message(
        Types.StructType iStruct,
        MessageType message,
        List<Iterable<FieldMetrics<?>>> fieldResults) {
      return Iterables.concat(fieldResults);
    }

    @Override
    public Iterable<FieldMetrics<?>> struct(
        Types.StructType iStruct, GroupType struct, List<Iterable<FieldMetrics<?>>> fieldResults) {
      return Iterables.concat(fieldResults);
    }

    @Override
    public Iterable<FieldMetrics<?>> list(
        Types.ListType iList, GroupType array, Iterable<FieldMetrics<?>> elementResults) {
      // remove lower and upper bounds for repeated fields
      return ImmutableList.of();
    }

    @Override
    public Iterable<FieldMetrics<?>> map(
        Types.MapType iMap,
        GroupType map,
        Iterable<FieldMetrics<?>> keyResults,
        Iterable<FieldMetrics<?>> valueResults) {
      // repeated fields are not currently supported
      return ImmutableList.of();
    }

    @Override
    public Iterable<FieldMetrics<?>> primitive(
        org.apache.iceberg.types.Type.PrimitiveType iPrimitive, PrimitiveType primitive) {
      Type.ID id = primitive.getId();
      if (null == id) {
        return ImmutableList.of();
      }
      int fieldId = id.intValue();

      MetricsModes.MetricsMode mode = MetricsUtil.metricsMode(schema, metricsConfig, fieldId);
      if (mode == MetricsModes.None.get()) {
        return ImmutableList.of();
      }

      int length = truncateLength(mode);

      FieldMetrics<?> metrics =
          metricsFromFieldMetrics(metricsById.get(fieldId), iPrimitive, length);
      if (metrics != null) {
        return ImmutableList.of(metrics);
      }

      metrics = metricsFromFooter(fieldId, iPrimitive, primitive, length);
      if (metrics != null) {
        return ImmutableList.of(metrics);
      }

      return ImmutableList.of();
    }

    private <T> FieldMetrics<T> metricsFromFieldMetrics(
        FieldMetrics<T> fieldMetrics,
        org.apache.iceberg.types.Type.PrimitiveType icebergType,
        int truncateLength) {
      if (null == fieldMetrics) {
        return null;
      } else if (truncateLength <= 0) {
        return new FieldMetrics<>(
            fieldMetrics.id(),
            fieldMetrics.valueCount(),
            fieldMetrics.nullValueCount(),
            fieldMetrics.nanValueCount());
      } else {
        T lowerBound = truncateLowerBound(icebergType, fieldMetrics.lowerBound(), truncateLength);
        T upperBound = truncateUpperBound(icebergType, fieldMetrics.upperBound(), truncateLength);
        return new FieldMetrics<>(
            fieldMetrics.id(),
            fieldMetrics.valueCount(),
            fieldMetrics.nullValueCount(),
            fieldMetrics.nanValueCount(),
            lowerBound,
            upperBound,
            icebergType);
      }
    }

    private <T> FieldMetrics<T> metricsFromFooter(
        int fieldId,
        org.apache.iceberg.types.Type.PrimitiveType icebergType,
        PrimitiveType primitive,
        int truncateLength) {
      if (primitive.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
        return null;
      } else if (truncateLength <= 0
          || (icebergType != null && isGeospatial(icebergType.typeId()))) {
        // Parquet lexicographic min/max is not meaningful for spatial WKB.
        return counts(fieldId);
      } else {
        return bounds(fieldId, icebergType, primitive, truncateLength);
      }
    }

    private static boolean isGeospatial(TypeID typeId) {
      return typeId == TypeID.GEOMETRY || typeId == TypeID.GEOGRAPHY;
    }

    private <T> FieldMetrics<T> counts(int fieldId) {
      ColumnPath path = ColumnPath.get(currentPath());
      long valueCount = 0;
      long nullCount = 0;

      for (ColumnChunkMetaData column : columns.get(path)) {
        Statistics<?> stats = column.getStatistics();
        if (stats == null || stats.isEmpty()) {
          return null;
        }

        nullCount += stats.getNumNulls();
        valueCount += column.getValueCount();
      }

      return new FieldMetrics<>(fieldId, valueCount, nullCount);
    }

    private <T> FieldMetrics<T> bounds(
        int fieldId,
        org.apache.iceberg.types.Type.PrimitiveType icebergType,
        PrimitiveType primitive,
        int truncateLength) {
      if (icebergType == null) {
        return null;
      }

      ColumnPath path = ColumnPath.get(currentPath());
      Comparator<T> comparator = Comparators.forType(icebergType);
      long valueCount = 0;
      long nullCount = 0;
      T lowerBound = null;
      T upperBound = null;

      for (ColumnChunkMetaData column : columns.get(path)) {
        Statistics<?> stats = column.getStatistics();
        if (stats == null || stats.isEmpty()) {
          return null;
        }

        nullCount += stats.getNumNulls();
        valueCount += column.getValueCount();

        if (stats.hasNonNullValue()) {
          T chunkMin =
              ParquetConversions.convertValue(icebergType, primitive, stats.genericGetMin());
          if (lowerBound == null || comparator.compare(chunkMin, lowerBound) < 0) {
            lowerBound = chunkMin;
          }

          T chunkMax =
              ParquetConversions.convertValue(icebergType, primitive, stats.genericGetMax());
          if (upperBound == null || comparator.compare(chunkMax, upperBound) > 0) {
            upperBound = chunkMax;
          }
        }
      }

      if (NaNUtil.isNaN(lowerBound) || NaNUtil.isNaN(upperBound)) {
        return new FieldMetrics<>(fieldId, valueCount, nullCount);
      }

      lowerBound = truncateLowerBound(icebergType, lowerBound, truncateLength);
      upperBound = truncateUpperBound(icebergType, upperBound, truncateLength);

      return new FieldMetrics<>(
          fieldId, valueCount, nullCount, lowerBound, upperBound, icebergType);
    }

    @Override
    @SuppressWarnings("CyclomaticComplexity")
    public Iterable<FieldMetrics<?>> variant(
        Types.VariantType iVariant, GroupType variant, Iterable<FieldMetrics<?>> ignored) {
      Type.ID id = variant.getId();
      if (null == id) {
        return ImmutableList.of();
      }
      int fieldId = id.intValue();

      MetricsModes.MetricsMode mode = MetricsUtil.metricsMode(schema, metricsConfig, fieldId);
      if (mode == MetricsModes.None.get()) {
        return ImmutableList.of();
      }

      List<ParquetVariantUtil.VariantMetrics> results =
          Lists.newArrayList(
              ParquetVariantVisitor.visit(variant, new MetricsVariantVisitor(currentPath())));

      if (results.isEmpty()) {
        return ImmutableList.of();
      }

      ParquetVariantUtil.VariantMetrics metadataCounts = results.get(0);
      if (mode == MetricsModes.Counts.get() || results.size() == 1) {
        return ImmutableList.of(
            new FieldMetrics<>(fieldId, metadataCounts.valueCount(), metadataCounts.nullCount()));
      }

      Set<String> fieldNames = Sets.newTreeSet();
      for (ParquetVariantUtil.VariantMetrics result : results.subList(1, results.size())) {
        if (result.lowerBound() != null || result.upperBound() != null) {
          fieldNames.add(result.fieldName());
        }
      }

      if (fieldNames.isEmpty()) {
        return ImmutableList.of(
            new FieldMetrics<>(fieldId, metadataCounts.valueCount(), metadataCounts.nullCount()));
      }

      VariantMetadata metadata = Variants.metadata(fieldNames);
      ShreddedObject lowerBounds = Variants.object(metadata);
      ShreddedObject upperBounds = Variants.object(metadata);
      for (ParquetVariantUtil.VariantMetrics result : results.subList(1, results.size())) {
        String fieldName = result.fieldName();
        if (result.lowerBound() != null) {
          lowerBounds.put(fieldName, result.lowerBound());
        }

        if (result.upperBound() != null) {
          upperBounds.put(fieldName, result.upperBound());
        }
      }

      return ImmutableList.of(
          new FieldMetrics<>(
              fieldId,
              metadataCounts.valueCount(),
              metadataCounts.nullCount(),
              Variant.of(metadata, lowerBounds),
              Variant.of(metadata, upperBounds),
              Types.VariantType.get()));
    }

    private class MetricsVariantVisitor
        extends ParquetVariantVisitor<Iterable<ParquetVariantUtil.VariantMetrics>> {
      private final Deque<String> fieldNames = Lists.newLinkedList();
      private final String[] basePath;

      private MetricsVariantVisitor(String[] basePath) {
        this.basePath = basePath;
      }

      @Override
      public void beforeField(Type type) {
        fieldNames.addLast(type.getName());
      }

      @Override
      public void afterField(Type type) {
        fieldNames.removeLast();
      }

      private Stream<String> currentPath() {
        return Streams.concat(Stream.of(basePath), fieldNames.stream());
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> variant(
          GroupType variant,
          Iterable<ParquetVariantUtil.VariantMetrics> metadataResults,
          Iterable<ParquetVariantUtil.VariantMetrics> valueResults) {
        return Iterables.concat(metadataResults, valueResults);
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> object(
          GroupType object,
          Iterable<ParquetVariantUtil.VariantMetrics> valueResult,
          List<Iterable<ParquetVariantUtil.VariantMetrics>> fieldResults) {
        // shredded fields are not allowed in the variant-encoded value so the stats are trusted
        // even if value result has non-null values
        GroupType shreddedFields = object.getType(TYPED_VALUE).asGroupType();
        List<Iterable<ParquetVariantUtil.VariantMetrics>> results = Lists.newArrayList();

        // for each field, prepend the field name
        for (int i = 0; i < fieldResults.size(); i += 1) {
          String name = shreddedFields.getFieldName(i);
          results.add(
              Iterables.transform(fieldResults.get(i), result -> result.prependFieldName(name)));
        }

        // return metrics for all sub-fields
        return Iterables.concat(results);
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> array(
          GroupType array,
          Iterable<ParquetVariantUtil.VariantMetrics> valueResult,
          Iterable<ParquetVariantUtil.VariantMetrics> elementResult) {
        return ImmutableList.of();
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> value(
          GroupType value,
          Iterable<ParquetVariantUtil.VariantMetrics> valueResult,
          Iterable<ParquetVariantUtil.VariantMetrics> typedResult) {
        if (null == valueResult) {
          // a value field was not present so the typed metrics can be used
          return typedResult;
        }

        if (Iterables.isEmpty(valueResult)) {
          // missing value stats invalidate typed bounds
          return ImmutableList.of();
        }

        ParquetVariantUtil.VariantMetrics valueMetrics = Iterables.getOnlyElement(valueResult);
        if (typedResult != null && valueMetrics.valueCount() == valueMetrics.nullCount()) {
          // all the variant-encoded values are null, so the typed stats can be used
          return typedResult;
        } else {
          // any non-null encoded variant invalidates the typed lower and upper bounds
          return ImmutableList.of();
        }
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> metadata(PrimitiveType metadata) {
        ParquetVariantUtil.VariantMetrics counts = counts();
        if (counts != null) {
          return ImmutableList.of(counts);
        } else {
          return ImmutableList.of();
        }
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> serialized(PrimitiveType value) {
        ParquetVariantUtil.VariantMetrics counts = counts();
        if (counts != null) {
          return ImmutableList.of(counts);
        } else {
          return ImmutableList.of();
        }
      }

      @Override
      public Iterable<ParquetVariantUtil.VariantMetrics> primitive(PrimitiveType primitive) {
        ParquetVariantUtil.VariantMetrics result = metrics(primitive);
        if (result != null) {
          return ImmutableList.of(result);
        } else {
          return ImmutableList.of();
        }
      }

      private ParquetVariantUtil.VariantMetrics counts() {
        ColumnPath path = ColumnPath.get(currentPath().toArray(String[]::new));
        long valueCount = 0;
        long nullCount = 0;

        for (ColumnChunkMetaData column : columns.get(path)) {
          Statistics<?> stats = column.getStatistics();
          if (stats == null || stats.isEmpty()) {
            // the null count is unknown
            return null;
          }

          boolean hasOnlyNullVariants;
          if (stats.hasNonNullValue()) {
            hasOnlyNullVariants =
                Variants.isNull(ByteBuffer.wrap(stats.getMinBytes()))
                    && Variants.isNull(ByteBuffer.wrap(stats.getMaxBytes()));
          } else {
            // use the null count because the min/max were not defined
            hasOnlyNullVariants = false;
          }

          valueCount += column.getValueCount();
          nullCount += hasOnlyNullVariants ? column.getValueCount() : stats.getNumNulls();
        }

        return new ParquetVariantUtil.VariantMetrics(valueCount, nullCount);
      }

      @SuppressWarnings("CyclomaticComplexity")
      private <T> ParquetVariantUtil.VariantMetrics metrics(PrimitiveType primitive) {
        PhysicalType variantType = ParquetVariantUtil.convert(primitive);
        if (null == variantType) {
          // the type could not be converted and is either invalid or unsupported
          return null;
        }

        ColumnPath path = ColumnPath.get(currentPath().toArray(String[]::new));
        Comparator<T> comparator = ParquetVariantUtil.comparator(variantType);
        int scale = ParquetVariantUtil.scale(primitive);
        long valueCount = 0;
        long nullCount = 0;
        T lowerBound = null;
        T upperBound = null;

        for (ColumnChunkMetaData column : columns.get(path)) {
          Statistics<?> stats = column.getStatistics();
          if (stats == null || stats.isEmpty()) {
            return null;
          }

          nullCount += stats.getNumNulls();
          valueCount += column.getValueCount();

          if (stats.hasNonNullValue()) {
            T chunkMin = ParquetVariantUtil.convertValue(variantType, scale, stats.genericGetMin());
            if (lowerBound == null || comparator.compare(chunkMin, lowerBound) < 0) {
              lowerBound = chunkMin;
            }

            T chunkMax = ParquetVariantUtil.convertValue(variantType, scale, stats.genericGetMax());
            if (upperBound == null || comparator.compare(chunkMax, upperBound) > 0) {
              upperBound = chunkMax;
            }
          }
        }

        if (NaNUtil.isNaN(lowerBound) || NaNUtil.isNaN(upperBound)) {
          return null;
        }

        if (lowerBound != null && upperBound != null) {
          VariantValue lower = Variants.of(variantType, lowerBound);
          VariantValue upper = Variants.of(variantType, upperBound);
          return new ParquetVariantUtil.VariantMetrics(valueCount, nullCount, lower, upper);
        } else {
          return new ParquetVariantUtil.VariantMetrics(valueCount, nullCount);
        }
      }
    }
  }

  private static int truncateLength(MetricsModes.MetricsMode mode) {
    if (mode == MetricsModes.None.get()) {
      return 0;
    } else if (mode == MetricsModes.Counts.get()) {
      return 0;
    } else if (mode instanceof MetricsModes.Truncate) {
      return ((MetricsModes.Truncate) mode).length();
    } else {
      return Integer.MAX_VALUE;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T truncateLowerBound(
      org.apache.iceberg.types.Type.PrimitiveType type, T value, int length) {
    if (null == value) {
      return null;
    }

    switch (type.typeId()) {
      case STRING:
        return (T) UnicodeUtil.truncateStringMin((String) value, length);
      case BINARY:
        return (T) BinaryUtil.truncateBinaryMin((ByteBuffer) value, length);
      default:
        return value;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T truncateUpperBound(
      org.apache.iceberg.types.Type.PrimitiveType type, T value, int length) {
    if (null == value) {
      return null;
    }

    switch (type.typeId()) {
      case STRING:
        return (T) UnicodeUtil.truncateStringMax((String) value, length);
      case BINARY:
        return (T) BinaryUtil.truncateBinaryMax((ByteBuffer) value, length);
      default:
        return value;
    }
  }
}
