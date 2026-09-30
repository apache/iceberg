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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Reusable {@link ContentStats} view over a {@link ContentFile}'s stat maps. */
class MapBackedContentStats implements ContentStats {
  private final Types.StructType type;
  private final Map<Integer, FieldStats<?>> statsById = Maps.newHashMap();

  private Map<Integer, Long> valueCounts;
  private Map<Integer, Long> nullValueCounts;
  private Map<Integer, Long> nanValueCounts;
  private Map<Integer, Integer> avgValueSizes;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;

  MapBackedContentStats(Schema tableSchema, MetricsConfig metricsConfig) {
    Preconditions.checkArgument(tableSchema != null, "Invalid table schema: null");
    Preconditions.checkArgument(metricsConfig != null, "Invalid metrics config: null");
    this.type = StatsUtil.statsWriteSchema(tableSchema, metricsConfig);
  }

  MapBackedContentStats wrap(ContentFile<?> file) {
    this.valueCounts = file.valueCounts();
    this.nullValueCounts = file.nullValueCounts();
    this.nanValueCounts = file.nanValueCounts();
    this.avgValueSizes = file.avgValueSizes();
    this.lowerBounds = file.lowerBounds();
    this.upperBounds = file.upperBounds();
    return this;
  }

  @Override
  public Iterable<FieldStats<?>> fieldStats() {
    return Iterables.filter(
        Iterables.transform(type.fields(), field -> statsFor(StatsUtil.toFieldId(field.fieldId()))),
        Objects::nonNull);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> FieldStats<T> statsFor(int fieldId) {
    if (!hasStats(fieldId)) {
      return null;
    }

    FieldStats<?> fieldStats = statsById.get(fieldId);
    if (fieldStats == null) {
      fieldStats = new MapBackedFieldStats<>(fieldId);
      statsById.put(fieldId, fieldStats);
    }

    return (FieldStats<T>) fieldStats;
  }

  @Override
  public Types.StructType type() {
    return type;
  }

  @Override
  public ContentStats copy() {
    throw new UnsupportedOperationException("copy is not implemented");
  }

  @Override
  public ContentStats copy(Set<Integer> fieldIds) {
    throw new UnsupportedOperationException("copy is not implemented");
  }

  private boolean hasStats(int id) {
    return containsId(valueCounts, id)
        || containsId(nullValueCounts, id)
        || containsId(nanValueCounts, id)
        || containsId(avgValueSizes, id)
        || containsId(lowerBounds, id)
        || containsId(upperBounds, id);
  }

  private static boolean containsId(Map<Integer, ?> map, int id) {
    return map != null && map.containsKey(id);
  }

  /** Reusable {@link FieldStats} view over one field's entries in a {@link ContentFile}'s maps. */
  private class MapBackedFieldStats<T> implements FieldStats<T> {
    private final int fieldId;
    private final Types.StructType struct;
    private final Type boundType;

    MapBackedFieldStats(int fieldId) {
      Types.NestedField field = type.field(StatsUtil.toBaseId(fieldId));
      Preconditions.checkArgument(
          field != null,
          "Cannot convert stats for field ID %s: unknown, not a scalar, or not in metrics config",
          fieldId);
      this.fieldId = fieldId;
      this.struct = field.type().asStructType();
      this.boundType = struct.fieldType(StatsUtil.LOWER_BOUND_NAME);
    }

    @Override
    public int fieldId() {
      return fieldId;
    }

    @Override
    public Types.StructType type() {
      return struct;
    }

    @Override
    public T lowerBound() {
      return decodeBound(lowerBounds);
    }

    @Override
    public T upperBound() {
      return decodeBound(upperBounds);
    }

    @SuppressWarnings("unchecked")
    private T decodeBound(Map<Integer, ByteBuffer> bounds) {
      if (boundType == null || bounds == null) {
        return null;
      }

      return (T) Conversions.fromByteBuffer(boundType, bounds.get(fieldId));
    }

    @Override
    public boolean tightBounds() {
      return false;
    }

    @Override
    public boolean hasValueCount() {
      return count(valueCounts) != null;
    }

    @Override
    public long valueCount() {
      return count(valueCounts);
    }

    @Override
    public boolean hasNullValueCount() {
      return count(nullValueCounts) != null;
    }

    @Override
    public long nullValueCount() {
      return count(nullValueCounts);
    }

    @Override
    public boolean hasNanValueCount() {
      return count(nanValueCounts) != null;
    }

    @Override
    public long nanValueCount() {
      return count(nanValueCounts);
    }

    @Override
    public Integer avgValueSizeInBytes() {
      return avgValueSizes == null ? null : avgValueSizes.get(fieldId);
    }

    @Override
    public FieldStats<T> copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }

    private Long count(Map<Integer, Long> counts) {
      return counts == null ? null : counts.get(fieldId);
    }
  }
}
