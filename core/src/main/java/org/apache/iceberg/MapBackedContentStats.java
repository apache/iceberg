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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Reusable {@link ContentStats} view over a {@link ContentFile}'s stat maps. */
class MapBackedContentStats implements ContentStats {
  private final Schema tableSchema;
  private final Map<Integer, FieldStats<?>> statsById = Maps.newHashMap();

  private Types.StructType type;
  private Map<Integer, Long> valueCounts;
  private Map<Integer, Long> nullValueCounts;
  private Map<Integer, Long> nanValueCounts;
  private Map<Integer, Integer> avgValueSizes;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;

  MapBackedContentStats(Schema tableSchema) {
    Preconditions.checkArgument(tableSchema != null, "Invalid table schema: null");
    this.tableSchema = tableSchema;
  }

  MapBackedContentStats wrap(ContentFile<?> file) {
    this.valueCounts = file.valueCounts();
    this.nullValueCounts = file.nullValueCounts();
    this.nanValueCounts = file.nanValueCounts();
    this.avgValueSizes = file.avgValueSizes();
    this.lowerBounds = file.lowerBounds();
    this.upperBounds = file.upperBounds();
    this.type = null;
    return this;
  }

  @Override
  public Iterable<FieldStats<?>> fieldStats() {
    return Iterables.filter(Iterables.transform(statsFieldIds(), this::statsFor), Objects::nonNull);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> FieldStats<T> statsFor(int fieldId) {
    // Schema is fixed for this instance; wrap() rebinds the metric maps. An id absent
    // from the current maps is left uncached so a later file can still surface it.
    if (!containsFieldInMaps(fieldId)) {
      return null;
    }

    if (!statsById.containsKey(fieldId)) {
      statsById.put(fieldId, createFieldStats(fieldId));
    }

    return (FieldStats<T>) statsById.get(fieldId);
  }

  FieldStats<?> createFieldStats(int fieldId) {
    Types.NestedField field = tableSchema.findField(fieldId);
    // A file can carry metrics for an id this schema does not have. Skip it.
    if (field == null) {
      return null;
    }

    Type fieldType = field.type();
    Types.StructType struct =
        StatsUtil.fieldStatsStruct(fieldType, StatsUtil.toBaseId(fieldId), MetricsModes.Full.get());
    // null means a struct, list, or map, or an id outside the stats window. The schema is bound
    // once during wrapper creation, so the cached null stays valid across wrap().
    if (struct == null) {
      return null;
    }

    return new MapBackedFieldStats<>(fieldId, fieldType, struct);
  }

  @Override
  public Types.StructType type() {
    if (type == null) {
      this.type = StatsUtil.statsReadSchema(tableSchema, statsFieldIds());
    }

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

  boolean containsFieldInMaps(int fieldId) {
    return containsId(valueCounts, fieldId)
        || containsId(nullValueCounts, fieldId)
        || containsId(nanValueCounts, fieldId)
        || containsId(avgValueSizes, fieldId)
        || containsId(lowerBounds, fieldId)
        || containsId(upperBounds, fieldId);
  }

  private Set<Integer> statsFieldIds() {
    Set<Integer> ids =
        Sets.newHashSetWithExpectedSize(valueCounts == null ? 0 : valueCounts.size());
    addKeys(ids, valueCounts);
    addKeys(ids, nullValueCounts);
    addKeys(ids, nanValueCounts);
    addKeys(ids, avgValueSizes);
    addKeys(ids, lowerBounds);
    addKeys(ids, upperBounds);
    return ids;
  }

  private static void addKeys(Set<Integer> ids, Map<Integer, ?> map) {
    if (map != null) {
      ids.addAll(map.keySet());
    }
  }

  private static boolean containsId(Map<Integer, ?> map, int id) {
    return map != null && map.containsKey(id);
  }

  /** Reusable {@link FieldStats} view over one field's entries in a {@link ContentFile}'s maps. */
  private class MapBackedFieldStats<T> implements FieldStats<T> {
    private final int fieldId;
    private final Types.StructType struct;
    private final Type fieldType;
    private final Type boundType;

    private MapBackedFieldStats(int fieldId, Type fieldType, Types.StructType struct) {
      this.fieldId = fieldId;
      this.fieldType = fieldType;
      this.struct = struct;
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

      return (T) Conversions.fromByteBuffer(fieldType, bounds.get(fieldId));
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
