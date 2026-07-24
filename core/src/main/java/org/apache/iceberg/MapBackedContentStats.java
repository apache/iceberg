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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Reusable {@link ContentStats} view over a legacy {@link ContentFile}'s stat maps.
 *
 * <p>Instantiated once per writer and re-pointed at each file's maps via {@link #wrap}, avoiding
 * the per-row allocation of a materialized stats object. Bounds are decoded lazily on access. The
 * writer serializes this view directly through {@link StructLike}, so {@code copy} is not
 * supported; a stable snapshot must be materialized via the writer instead.
 */
class MapBackedContentStats implements ContentStats, StructLike {
  private final Types.StructType struct;
  private final int[] posToId;
  private final Map<Integer, FieldStats<?>> statsById;

  private Map<Integer, Long> valueCounts;
  private Map<Integer, Long> nullValueCounts;
  private Map<Integer, Long> nanValueCounts;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;

  MapBackedContentStats(Schema tableSchema, MetricsConfig metricsConfig) {
    this.struct = StatsUtil.statsWriteSchema(tableSchema, metricsConfig);
    List<Types.NestedField> fields = struct.fields();
    this.posToId = new int[fields.size()];
    this.statsById = Maps.newHashMapWithExpectedSize(fields.size());
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      int fieldId = StatsUtil.toFieldId(field.fieldId());
      posToId[i] = fieldId;
      statsById.put(
          fieldId,
          new MapBackedFieldStats<>(
              field.type().asStructType(), fieldId, tableSchema.findType(fieldId)));
    }
  }

  MapBackedContentStats wrap(ContentFile<?> file) {
    this.valueCounts = file.valueCounts();
    this.nullValueCounts = file.nullValueCounts();
    this.nanValueCounts = file.nanValueCounts();
    this.lowerBounds = file.lowerBounds();
    this.upperBounds = file.upperBounds();
    return this;
  }

  private boolean hasStats(int id) {
    return containsId(valueCounts, id)
        || containsId(nullValueCounts, id)
        || containsId(nanValueCounts, id)
        || containsId(lowerBounds, id)
        || containsId(upperBounds, id);
  }

  private static boolean containsId(Map<Integer, ?> map, int id) {
    return map != null && map.containsKey(id);
  }

  @Override
  public Iterable<FieldStats<?>> fieldStats() {
    return Iterables.transform(
        Iterables.filter(statsById.entrySet(), entry -> hasStats(entry.getKey())),
        Map.Entry::getValue);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> FieldStats<T> statsFor(int fieldId) {
    return hasStats(fieldId) ? (FieldStats<T>) statsById.get(fieldId) : null;
  }

  @Override
  public Types.StructType type() {
    return struct;
  }

  @Override
  public int size() {
    return struct.fields().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    int id = posToId[pos];
    return javaClass.cast(hasStats(id) ? statsById.get(id) : null);
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException(
        "Reusable content stats wrapper does not support set()");
  }

  @Override
  public ContentStats copy() {
    throw new UnsupportedOperationException(
        "Reusable content stats wrapper does not support copy(); materialize via a writer instead");
  }

  @Override
  public ContentStats copy(Set<Integer> fieldIds) {
    throw new UnsupportedOperationException(
        "Reusable content stats wrapper does not support copy(); materialize via a writer instead");
  }

  /** Reusable {@link FieldStats} view over one field's entries in a {@link ContentFile}'s maps. */
  private class MapBackedFieldStats<T> implements FieldStats<T>, StructLike {
    private final Types.StructType struct;
    private final int fieldId;
    private final Type boundType;
    private final int[] posToOffset;

    MapBackedFieldStats(Types.StructType struct, int fieldId, Type boundType) {
      this.struct = struct;
      this.fieldId = fieldId;
      this.boundType = boundType;
      this.posToOffset = posToOffset(struct);
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
    @SuppressWarnings("unchecked")
    public T lowerBound() {
      ByteBuffer buf = lowerBounds == null ? null : lowerBounds.get(fieldId);
      return buf == null ? null : (T) Conversions.fromByteBuffer(boundType, buf);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T upperBound() {
      ByteBuffer buf = upperBounds == null ? null : upperBounds.get(fieldId);
      return buf == null ? null : (T) Conversions.fromByteBuffer(boundType, buf);
    }

    @Override
    public boolean tightBounds() {
      return false;
    }

    @Override
    public boolean hasValueCount() {
      return boxedCount(valueCounts) != null;
    }

    @Override
    public long valueCount() {
      return count(valueCounts);
    }

    @Override
    public boolean hasNullValueCount() {
      return boxedCount(nullValueCounts) != null;
    }

    @Override
    public long nullValueCount() {
      return count(nullValueCounts);
    }

    @Override
    public boolean hasNanValueCount() {
      return boxedCount(nanValueCounts) != null;
    }

    @Override
    public long nanValueCount() {
      return count(nanValueCounts);
    }

    @Override
    public Integer avgValueSizeInBytes() {
      return null;
    }

    private long count(Map<Integer, Long> counts) {
      Long value = counts == null ? null : counts.get(fieldId);
      // -1 signals "not tracked", matching FieldMetrics; 0 would falsely assert a known zero count
      return value == null ? -1L : value;
    }

    private Long boxedCount(Map<Integer, Long> counts) {
      return counts == null ? null : counts.get(fieldId);
    }

    @Override
    public int size() {
      return struct.fields().size();
    }

    @Override
    public <C> C get(int pos, Class<C> javaClass) {
      return javaClass.cast(getOffset(posToOffset[pos]));
    }

    private Object getOffset(int offset) {
      return switch (offset) {
        case StatsUtil.LOWER_BOUND_OFFSET -> lowerBound();
        case StatsUtil.UPPER_BOUND_OFFSET -> upperBound();
        case StatsUtil.TIGHT_BOUNDS_OFFSET -> tightBounds();
        case StatsUtil.VALUE_COUNT_OFFSET -> boxedCount(valueCounts);
        case StatsUtil.NULL_VALUE_COUNT_OFFSET -> boxedCount(nullValueCounts);
        case StatsUtil.NAN_VALUE_COUNT_OFFSET -> boxedCount(nanValueCounts);
        case StatsUtil.AVG_VALUE_SIZE_OFFSET -> null;
        default -> throw new UnsupportedOperationException("Unsupported stats offset: " + offset);
      };
    }

    @Override
    public <C> void set(int pos, C value) {
      throw new UnsupportedOperationException(
          "Reusable field stats wrapper does not support set()");
    }

    @Override
    public FieldStats<T> copy() {
      throw new UnsupportedOperationException(
          "Reusable field stats wrapper does not support copy(); materialize via a writer instead");
    }

    private static int[] posToOffset(Types.StructType struct) {
      List<Types.NestedField> fields = struct.fields();
      int[] offsets = new int[fields.size()];
      for (int i = 0; i < offsets.length; i += 1) {
        offsets[i] = StatsUtil.statOffset(fields.get(i).fieldId());
      }

      return offsets;
    }
  }
}
