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
package org.apache.iceberg.vortex;

import dev.vortex.api.VortexColumnStatistics;
import dev.vortex.api.VortexWriteSummary;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.util.UnicodeUtil;

/**
 * Assembles Iceberg {@link Metrics} from statistics computed natively during Vortex writes,
 * respecting {@link MetricsConfig} modes (none, counts, truncate, full).
 */
final class VortexMetrics {

  private VortexMetrics() {}

  /**
   * Converts the {@link VortexWriteSummary} returned when a Vortex writer is finalized into Iceberg
   * {@link Metrics}.
   *
   * <p>Vortex reports statistics per top-level column, in Arrow schema order (which matches {@code
   * schema.columns()}). Counts and bounds are emitted for primitive and variant top-level fields;
   * fields nested inside structs, lists, and maps carry no per-field statistics. Column sizes are
   * recorded for every top-level field unless the field's metrics mode is {@code none}, matching
   * how Parquet footer metrics are collected.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static Metrics fromWriteSummary(
      Schema schema, MetricsConfig metricsConfig, VortexWriteSummary summary) {
    List<Types.NestedField> columns = schema.columns();
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    Map<Integer, Type> originalTypes = Maps.newHashMap();

    for (VortexColumnStatistics colStats : summary.columnStatistics()) {
      Preconditions.checkElementIndex(colStats.columnIndex(), columns.size(), "column index");
      Types.NestedField field = columns.get(colStats.columnIndex());
      int id = field.fieldId();
      Type type = field.type();

      MetricsModes.MetricsMode mode = MetricsUtil.metricsMode(schema, metricsConfig, id);
      if (mode == MetricsModes.None.get()) {
        continue;
      }

      columnSizes.put(id, colStats.compressedSize());

      // Vortex only computes row-level statistics for top-level columns, so struct columns (whose
      // metrics would belong to their nested fields) are skipped entirely.
      if (!type.isPrimitiveType() && !type.isVariantType()) {
        continue;
      }

      valueCounts.put(id, colStats.valueCount());
      colStats.nullValueCount().ifPresent(count -> nullValueCounts.put(id, count));
      colStats.nanValueCount().ifPresent(count -> nanValueCounts.put(id, count));

      if (mode == MetricsModes.Counts.get() || !type.isPrimitiveType()) {
        continue;
      }

      int truncateLength = truncateLength(mode);

      Object lowerValue =
          colStats
              .lowerBound()
              .map(bound -> toIcebergBound(type.asPrimitiveType(), bound))
              .orElse(null);
      if (lowerValue != null) {
        Object truncated = truncateLowerBound(type, lowerValue, truncateLength);
        if (truncated != null) {
          lowerBounds.put(id, Conversions.toByteBuffer(type, truncated));
          originalTypes.put(id, type);
        }
      }

      Object upperValue =
          colStats
              .upperBound()
              .map(bound -> toIcebergBound(type.asPrimitiveType(), bound))
              .orElse(null);
      if (upperValue != null) {
        Object truncated = truncateUpperBound(type, upperValue, truncateLength);
        if (truncated != null) {
          upperBounds.put(id, Conversions.toByteBuffer(type, truncated));
          originalTypes.put(id, type);
        }
      }
    }

    addVariantValueCounts(summary.rowCount(), schema, metricsConfig, valueCounts);

    return new Metrics(
        summary.rowCount(),
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts.isEmpty() ? null : nanValueCounts,
        lowerBounds.isEmpty() ? null : lowerBounds,
        upperBounds.isEmpty() ? null : upperBounds,
        originalTypes.isEmpty() ? null : originalTypes);
  }

  /**
   * Converts a bound reported by Vortex (using the Arrow scalar's Java representation) into the
   * value type Iceberg expects for the field's type, or null when the bound cannot be represented.
   */
  private static Object toIcebergBound(Type.PrimitiveType type, Object bound) {
    return switch (type.typeId()) {
      case BOOLEAN -> bound instanceof Boolean ? bound : null;
      case INTEGER, DATE -> bound instanceof Number number ? number.intValue() : null;
      case LONG, TIME, TIMESTAMP, TIMESTAMP_NANO ->
          bound instanceof Number number ? number.longValue() : null;
        // NaN is not a valid Iceberg bound; Vortex min/max should never report it, but guard
        // anyway.
      case FLOAT ->
          bound instanceof Number number && !Float.isNaN(number.floatValue())
              ? number.floatValue()
              : null;
      case DOUBLE ->
          bound instanceof Number number && !Double.isNaN(number.doubleValue())
              ? number.doubleValue()
              : null;
      case STRING -> bound instanceof String ? bound : null;
      case BINARY, FIXED -> bound instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : null;
      case UUID ->
          bound instanceof byte[] bytes && bytes.length == 16 ? UUIDUtil.convert(bytes) : null;
      case DECIMAL ->
          bound instanceof BigDecimal decimal
                  && decimal.scale() == ((Types.DecimalType) type).scale()
              ? decimal
              : null;
      default -> null;
    };
  }

  static Metrics buildMetrics(
      long rowCount, Schema schema, MetricsConfig metricsConfig, Stream<FieldMetrics<?>> fields) {
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    Map<Integer, Type> originalTypes = Maps.newHashMap();

    fields.forEach(
        fieldMetrics -> {
          int id = fieldMetrics.id();
          MetricsModes.MetricsMode mode = MetricsUtil.metricsMode(schema, metricsConfig, id);

          if (mode == MetricsModes.None.get()) {
            return;
          }

          valueCounts.put(id, fieldMetrics.valueCount());
          nullValueCounts.put(id, fieldMetrics.nullValueCount());

          if (fieldMetrics.nanValueCount() >= 0) {
            nanValueCounts.put(id, fieldMetrics.nanValueCount());
          }

          if (mode == MetricsModes.Counts.get()) {
            return;
          }

          if (fieldMetrics.hasBounds()) {
            Types.NestedField field = schema.findField(id);
            Type type = field.type();
            int truncateLength = truncateLength(mode);

            Object lower = truncateLowerBound(type, fieldMetrics.lowerBound(), truncateLength);
            if (lower != null) {
              lowerBounds.put(id, Conversions.toByteBuffer(type, lower));
              originalTypes.put(id, type);
            }

            Object upper = truncateUpperBound(type, fieldMetrics.upperBound(), truncateLength);
            if (upper != null) {
              upperBounds.put(id, Conversions.toByteBuffer(type, upper));
              originalTypes.put(id, type);
            }
          }
        });

    addVariantValueCounts(rowCount, schema, metricsConfig, valueCounts);

    return new Metrics(
        rowCount,
        null, // columnSizes are only available when metrics come from a VortexWriteSummary
        valueCounts,
        nullValueCounts,
        nanValueCounts.isEmpty() ? null : nanValueCounts,
        lowerBounds.isEmpty() ? null : lowerBounds,
        upperBounds.isEmpty() ? null : upperBounds,
        originalTypes.isEmpty() ? null : originalTypes);
  }

  private static void addVariantValueCounts(
      long rowCount, Schema schema, MetricsConfig metricsConfig, Map<Integer, Long> valueCounts) {
    for (Types.NestedField column : schema.columns()) {
      int id = column.fieldId();
      MetricsModes.MetricsMode mode = MetricsUtil.metricsMode(schema, metricsConfig, id);
      if (column.type().isVariantType()
          && mode != MetricsModes.None.get()
          && !valueCounts.containsKey(id)) {
        valueCounts.put(id, rowCount);
      }
    }
  }

  private static int truncateLength(MetricsModes.MetricsMode mode) {
    if (mode instanceof MetricsModes.Truncate truncate) {
      return truncate.length();
    }
    return Integer.MAX_VALUE;
  }

  @SuppressWarnings("unchecked")
  private static <T> T truncateLowerBound(Type type, T value, int length) {
    if (value == null) {
      return null;
    }
    return switch (type.typeId()) {
      case STRING -> (T) UnicodeUtil.truncateStringMin((String) value, length);
      case BINARY, FIXED -> (T) BinaryUtil.truncateBinaryMin((ByteBuffer) value, length);
      default -> value;
    };
  }

  @SuppressWarnings("unchecked")
  private static <T> T truncateUpperBound(Type type, T value, int length) {
    if (value == null) {
      return null;
    }
    return switch (type.typeId()) {
      case STRING -> (T) UnicodeUtil.truncateStringMax((String) value, length);
      case BINARY, FIXED -> (T) BinaryUtil.truncateBinaryMax((ByteBuffer) value, length);
      default -> value;
    };
  }
}
