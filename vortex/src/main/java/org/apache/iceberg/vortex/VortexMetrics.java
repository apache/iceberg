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
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;

/**
 * Assembles Iceberg {@link Metrics} from per-field metrics collected during Vortex writes,
 * respecting {@link MetricsConfig} modes (none, counts, truncate, full).
 */
final class VortexMetrics {

  private VortexMetrics() {}

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
