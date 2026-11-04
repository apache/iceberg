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
package org.apache.iceberg.spark.actions;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CurveQuantization {
  private static final Logger LOG = LoggerFactory.getLogger(CurveQuantization.class);

  static final String QUANTIZATION_BITS = "quantization-bits";

  static final int QUANTIZATION_BITS_DEFAULT = 0;

  private static final Set<Integer> VALID_QUANTIZATION_BITS = ImmutableSet.of(0, 8, 16, 24, 32);
  private static final Set<Type.TypeID> QUANTIZABLE_TYPES =
      ImmutableSet.of(
          Type.TypeID.INTEGER,
          Type.TypeID.LONG,
          Type.TypeID.DATE,
          Type.TypeID.TIMESTAMP,
          Type.TypeID.FLOAT,
          Type.TypeID.DOUBLE);

  private CurveQuantization() {}

  static int quantizationBits(Map<String, String> options) {
    int bits = PropertyUtil.propertyAsInt(options, QUANTIZATION_BITS, QUANTIZATION_BITS_DEFAULT);
    Preconditions.checkArgument(
        VALID_QUANTIZATION_BITS.contains(bits),
        "Cannot use quantization-bits %s, it must be 0 (off), 8, 16, 24 or 32",
        bits);
    return bits;
  }

  static void validateColumns(Table table, List<String> columns) {
    for (String column : columns) {
      Type type = table.schema().findType(column);
      Preconditions.checkArgument(
          QUANTIZABLE_TYPES.contains(type.typeId()),
          "Cannot quantize column %s of type %s, supported types are int, long, date, timestamp, float and double",
          column,
          type);
    }
  }

  static List<ColumnRange> ranges(List<FileScanTask> tasks, Table table, List<String> columns) {
    List<ColumnRange> ranges = Lists.newArrayListWithExpectedSize(columns.size());
    for (String column : columns) {
      ranges.add(range(tasks, table, column));
    }

    return ranges;
  }

  private static ColumnRange range(List<FileScanTask> tasks, Table table, String column) {
    Types.NestedField field = table.schema().findField(column);
    int fieldId = field.fieldId();
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    boolean hasBounds = false;
    int filesWithoutCounts = 0;

    for (FileScanTask task : tasks) {
      DataFile file = task.file();
      ByteBuffer lower = valueOrNull(file.lowerBounds(), fieldId);
      ByteBuffer upper = valueOrNull(file.upperBounds(), fieldId);
      if (lower != null && upper != null) {
        min = Math.min(min, finiteBound(column, field.type(), lower));
        max = Math.max(max, finiteBound(column, field.type(), upper));
        hasBounds = true;
      } else {
        Long valueCount = valueOrNull(file.valueCounts(), fieldId);
        if (valueCount == null) {
          filesWithoutCounts += 1;
        } else {
          Preconditions.checkArgument(
              valueCount == nullOrNaNCount(file, fieldId),
              "Cannot quantize column %s: file %s has values but no bounds, set %s or %s%s to truncate or full",
              column,
              file.location(),
              TableProperties.DEFAULT_WRITE_METRICS_MODE,
              TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX,
              column);
        }
      }
    }

    if (filesWithoutCounts > 0 && filesWithoutCounts == tasks.size()) {
      Preconditions.checkArgument(
          MetricsConfig.forTable(table).columnMode(column) != MetricsModes.None.get(),
          "Cannot quantize column %s: metrics are disabled for it, set %s or %s%s to truncate or full",
          column,
          TableProperties.DEFAULT_WRITE_METRICS_MODE,
          TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX,
          column);
      LOG.warn(
          "Leaving column {} unclustered for this file group because none of its {} files have stats for the column",
          column,
          filesWithoutCounts);
    } else if (filesWithoutCounts > 0) {
      LOG.warn(
          "Ignoring {} files without metrics for column {} when computing its quantization range",
          filesWithoutCounts,
          column);
    }

    return hasBounds ? new ColumnRange(min, max, false) : ColumnRange.NONE;
  }

  private static double finiteBound(String column, Type type, ByteBuffer bound) {
    double value = Conversions.<Number>fromByteBuffer(type, bound).doubleValue();
    Preconditions.checkArgument(
        Double.isFinite(value),
        "Cannot quantize column %s with a non-finite bound %s",
        column,
        value);
    return value;
  }

  private static long nullOrNaNCount(DataFile file, int fieldId) {
    Long nullCount = valueOrNull(file.nullValueCounts(), fieldId);
    Long nanCount = valueOrNull(file.nanValueCounts(), fieldId);
    return (nullCount != null ? nullCount : 0L) + (nanCount != null ? nanCount : 0L);
  }

  private static <T> T valueOrNull(Map<Integer, T> map, int fieldId) {
    return map != null ? map.get(fieldId) : null;
  }

  static final class ColumnRange implements Serializable {
    static final ColumnRange NONE = new ColumnRange(0.0, 0.0, true);

    private final double min;
    private final double max;
    private final boolean isNone;

    private ColumnRange(double min, double max, boolean isNone) {
      this.min = min;
      this.max = max;
      this.isNone = isNone;
    }

    double min() {
      return min;
    }

    double max() {
      return max;
    }

    boolean isNone() {
      return isNone;
    }
  }
}
