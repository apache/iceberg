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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.mockito.Mockito;

class StatsTestUtil {
  private StatsTestUtil() {}

  static TrackedFile trackedFile(String location, long recordCount, ContentStats stats) {
    return new TrackedFileStruct(
        null,
        FileContent.DATA,
        4,
        location,
        FileFormat.fromFileName(location),
        recordCount,
        1024L,
        0,
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
    validateTrackedMetric(type, StatsUtil.LOWER_BOUND_NAME, lower);
    validateTrackedMetric(type, StatsUtil.UPPER_BOUND_NAME, upper);
    validateTrackedMetric(type, "value_count", valueCount);
    validateTrackedMetric(type, "null_value_count", nullCount);
    validateTrackedMetric(type, "nan_value_count", nanCount);

    return new TestFieldStats(fieldId, type, lower, upper, valueCount, nullCount, nanCount);
  }

  private static void validateTrackedMetric(Types.StructType type, String metric, Object value) {
    Preconditions.checkArgument(
        value == null || type.field(metric) != null,
        "Cannot set %s: not tracked by %s",
        metric,
        type);
  }

  private static class TestFieldStats implements FieldStats<Object> {
    private final int fieldId;
    private final Types.StructType type;
    private final Object lowerBound;
    private final Object upperBound;
    private final Long valueCount;
    private final Long nullValueCount;
    private final Long nanValueCount;

    private TestFieldStats(
        int fieldId,
        Types.StructType type,
        Object lowerBound,
        Object upperBound,
        Long valueCount,
        Long nullValueCount,
        Long nanValueCount) {
      this.fieldId = fieldId;
      this.type = type;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.valueCount = valueCount;
      this.nullValueCount = nullValueCount;
      this.nanValueCount = nanValueCount;
    }

    @Override
    public int fieldId() {
      return fieldId;
    }

    @Override
    public Types.StructType type() {
      return type;
    }

    @Override
    public Object lowerBound() {
      return lowerBound;
    }

    @Override
    public Object upperBound() {
      return upperBound;
    }

    @Override
    public boolean tightBounds() {
      return false;
    }

    @Override
    public boolean hasValueCount() {
      return valueCount != null;
    }

    @Override
    public long valueCount() {
      return valueCount;
    }

    @Override
    public boolean hasNullValueCount() {
      return nullValueCount != null;
    }

    @Override
    public long nullValueCount() {
      return nullValueCount;
    }

    @Override
    public boolean hasNanValueCount() {
      return nanValueCount != null;
    }

    @Override
    public long nanValueCount() {
      return nanValueCount;
    }

    @Override
    public Integer avgValueSizeInBytes() {
      return null;
    }

    @Override
    public FieldStats<Object> copy() {
      return this;
    }
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
    return mockFieldStats(type, id, lower, upper, valueCount, nullCount, nanCount, null);
  }

  @SuppressWarnings("unchecked")
  static FieldStats<Object> mockFieldStats(
      Types.StructType type,
      int id,
      Object lower,
      Object upper,
      Long valueCount,
      Long nullCount,
      Long nanCount,
      Integer avgValueSize) {
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

    Mockito.when(stats.avgValueSizeInBytes()).thenReturn(avgValueSize);

    return stats;
  }
}
