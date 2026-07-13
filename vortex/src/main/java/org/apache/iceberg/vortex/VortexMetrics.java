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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.util.UnicodeUtil;

/**
 * Assembles Iceberg {@link Metrics} from per-field metrics collected during Vortex writes,
 * respecting {@link MetricsConfig} modes (none, counts, truncate, full).
 */
final class VortexMetrics {

  private VortexMetrics() {}

  /** Collects Iceberg metrics directly from Arrow batches before they are encoded by Vortex. */
  static class Accumulator {
    private final Schema schema;
    private final MetricsConfig metricsConfig;
    private final Map<Integer, FieldAccumulator<?>> fields = Maps.newHashMap();

    Accumulator(Schema schema, MetricsConfig metricsConfig) {
      this.schema = schema;
      this.metricsConfig = metricsConfig;
      for (Types.NestedField field : schema.columns()) {
        addField(field);
      }
    }

    void add(VectorSchemaRoot root, int rowCount) {
      for (Types.NestedField field : schema.columns()) {
        FieldVector vector = root.getVector(field.name());
        for (int row = 0; row < rowCount; row++) {
          addValue(field, vector, row, false);
        }
      }
    }

    Metrics metrics(long rowCount) {
      return buildMetrics(
          rowCount,
          schema,
          metricsConfig,
          fields.values().stream().map(FieldAccumulator::toFieldMetrics));
    }

    private void addField(Types.NestedField field) {
      Type type = field.type();
      if (type.isStructType()) {
        for (Types.NestedField child : type.asStructType().fields()) {
          addField(child);
        }
      } else if (type.isPrimitiveType() || type.isVariantType()) {
        MetricsModes.MetricsMode mode =
            MetricsUtil.metricsMode(schema, metricsConfig, field.fieldId());
        if (mode != MetricsModes.None.get()) {
          fields.put(field.fieldId(), new FieldAccumulator<>(field, mode));
        }
      }
    }

    private void addValue(
        Types.NestedField field, FieldVector vector, int row, boolean parentIsNull) {
      Type type = field.type();
      boolean isNull = parentIsNull || vector == null || vector.isNull(row);

      if (type.isStructType()) {
        StructVector struct = (StructVector) vector;
        for (Types.NestedField child : type.asStructType().fields()) {
          addValue(child, (FieldVector) struct.getChild(child.name()), row, isNull);
        }
      } else if (type.isPrimitiveType() || type.isVariantType()) {
        FieldAccumulator<Object> accumulator = accumulator(field.fieldId());
        if (accumulator == null) {
          return;
        }

        if (isNull) {
          accumulator.addNull();
        } else if (type.isVariantType()) {
          accumulator.addValue(null);
        } else {
          accumulator.addValue(readPrimitive(vector, type.asPrimitiveType(), row));
        }
      }
      // Metrics for fields in lists and maps are intentionally omitted. Counts for repeated fields
      // do not have row-level semantics, and Parquet follows the same rule.
    }

    @SuppressWarnings("unchecked")
    private FieldAccumulator<Object> accumulator(int fieldId) {
      return (FieldAccumulator<Object>) fields.get(fieldId);
    }
  }

  private static class FieldAccumulator<T> {
    private final int fieldId;
    private final Type type;
    private final Comparator<T> comparator;
    private long valueCount = 0L;
    private long nullCount = 0L;
    private long nanCount;
    private T lowerBound = null;
    private T upperBound = null;

    @SuppressWarnings("unchecked")
    private FieldAccumulator(Types.NestedField field, MetricsModes.MetricsMode mode) {
      this.fieldId = field.fieldId();
      this.type = field.type();
      this.comparator =
          mode != MetricsModes.Counts.get() && type.isPrimitiveType()
              ? Comparators.forType(type.asPrimitiveType())
              : null;
      this.nanCount =
          type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE ? 0L : -1L;
    }

    private void addNull() {
      valueCount++;
      nullCount++;
    }

    private void addValue(T value) {
      valueCount++;
      if (value instanceof Float floatValue && floatValue.isNaN()) {
        nanCount++;
        return;
      } else if (value instanceof Double doubleValue && doubleValue.isNaN()) {
        nanCount++;
        return;
      }

      if (comparator != null) {
        if (lowerBound == null || comparator.compare(value, lowerBound) < 0) {
          lowerBound = value;
        }

        if (upperBound == null || comparator.compare(value, upperBound) > 0) {
          upperBound = value;
        }
      }
    }

    private FieldMetrics<T> toFieldMetrics() {
      return new FieldMetrics<>(
          fieldId, valueCount, nullCount, nanCount, lowerBound, upperBound, type);
    }
  }

  private static Object readPrimitive(FieldVector vector, Type.PrimitiveType type, int row) {
    return switch (type.typeId()) {
      case BOOLEAN -> ((BitVector) vector).get(row) != 0;
      case INTEGER -> ((IntVector) vector).get(row);
      case LONG -> ((BigIntVector) vector).get(row);
      case FLOAT -> ((Float4Vector) vector).get(row);
      case DOUBLE -> ((Float8Vector) vector).get(row);
      case STRING -> new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
      case BINARY -> ByteBuffer.wrap(((VarBinaryVector) vector).get(row));
      case FIXED -> ByteBuffer.wrap(((FixedSizeBinaryVector) vector).get(row));
      case DECIMAL -> ((DecimalVector) vector).getObject(row);
      case DATE -> ((DateDayVector) vector).get(row);
      case TIME -> ((TimeMicroVector) vector).get(row);
      case TIMESTAMP -> readTimestampMicros(vector, row);
      case TIMESTAMP_NANO -> readTimestampNanos(vector, row);
      case UUID -> readUuid(vector, row);
      default ->
          throw new UnsupportedOperationException(
              "Unsupported Iceberg type for Vortex metrics: " + type);
    };
  }

  private static long readTimestampMicros(FieldVector vector, int row) {
    if (vector instanceof TimeStampMicroTZVector timestamp) {
      return timestamp.get(row);
    }
    return ((TimeStampMicroVector) vector).get(row);
  }

  private static long readTimestampNanos(FieldVector vector, int row) {
    if (vector instanceof TimeStampNanoTZVector timestamp) {
      return timestamp.get(row);
    }
    return ((TimeStampNanoVector) vector).get(row);
  }

  private static UUID readUuid(FieldVector vector, int row) {
    FixedSizeBinaryVector storage =
        vector instanceof ExtensionTypeVector<?> ext
            ? (FixedSizeBinaryVector) ext.getUnderlyingVector()
            : (FixedSizeBinaryVector) vector;
    return UUIDUtil.convert(storage.get(row));
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
        null, // columnSizes not available without Vortex JNI support
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
