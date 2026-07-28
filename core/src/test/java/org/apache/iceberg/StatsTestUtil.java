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

import java.util.List;
import org.apache.iceberg.types.Types;
import org.mockito.Mockito;

class StatsTestUtil {
  private static final int FORMAT_VERSION_V4 = 4;

  private StatsTestUtil() {}

  static TrackedFile trackedFile(String location, long recordCount, ContentStats stats) {
    return new TrackedFileStruct(
        null,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.fromFileName(location),
        null,
        recordCount,
        1024L,
        null,
        stats,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  static ContentStats contentStats(Types.StructType statsType, FieldStats<?>... fieldStats) {
    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    for (FieldStats<?> field : fieldStats) {
      stats.setStats(field.fieldId(), field);
    }

    return stats;
  }

  /** Returns the stats for a field, where a null metric is one that the column does not track. */
  static FieldStats<Object> fieldStats(
      Types.StructType statsType,
      int fieldId,
      Object lower,
      Object upper,
      Long valueCount,
      Long nullCount,
      Long nanCount) {
    Types.StructType type = statsType.field(StatsUtil.toBaseId(fieldId)).type().asStructType();
    FieldStatsStruct<Object> stats = new FieldStatsStruct<>(type);
    set(stats, StatsUtil.LOWER_BOUND_NAME, lower);
    set(stats, StatsUtil.UPPER_BOUND_NAME, upper);
    set(stats, "value_count", valueCount);
    set(stats, "null_value_count", nullCount);
    set(stats, "nan_value_count", nanCount);

    return stats;
  }

  private static void set(FieldStatsStruct<Object> stats, String metric, Object value) {
    if (value == null) {
      return;
    }

    List<Types.NestedField> fields = stats.type().fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      if (fields.get(pos).name().equals(metric)) {
        stats.set(pos, value);
        return;
      }
    }

    throw new IllegalArgumentException("Cannot set " + metric + ": not tracked by " + stats.type());
  }

  /**
   * Mocks a {@link FieldStats} for a stats struct type, stubbing the bounds and only the counts
   * that are present. A null count leaves the matching {@code has*Count()} reporting false, so a
   * column that does not track a metric can be modeled without constructing an invalid struct.
   */
  @SuppressWarnings("unchecked")
  static FieldStats<Object> mockFieldStats(
      Types.StructType type,
      int id,
      Object lower,
      Object upper,
      Long valueCount,
      Long nullCount,
      Long nanCount) {
    FieldStats<Object> stats = Mockito.mock(FieldStats.class);
    Mockito.when(stats.fieldId()).thenReturn(id);
    Mockito.when(stats.type()).thenReturn(type);
    Mockito.when(stats.lowerBound()).thenReturn(lower);
    Mockito.when(stats.upperBound()).thenReturn(upper);
    Mockito.when(stats.hasValueCount()).thenReturn(valueCount != null);
    Mockito.when(stats.hasNullValueCount()).thenReturn(nullCount != null);
    Mockito.when(stats.hasNanValueCount()).thenReturn(nanCount != null);
    if (valueCount != null) {
      Mockito.when(stats.valueCount()).thenReturn(valueCount);
    }

    if (nullCount != null) {
      Mockito.when(stats.nullValueCount()).thenReturn(nullCount);
    }

    if (nanCount != null) {
      Mockito.when(stats.nanValueCount()).thenReturn(nanCount);
    }

    return stats;
  }
}
