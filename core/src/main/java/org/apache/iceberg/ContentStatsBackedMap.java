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
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * A lazy, read-only {@link Map} view of one stat across the columns of a {@link ContentStats},
 * keyed by field ID, mirroring the per-column stat maps on {@link ContentFile}.
 */
class ContentStatsBackedMap<V> extends AbstractMap<Integer, V> {
  enum Kind {
    VALUE_COUNT,
    NULL_VALUE_COUNT,
    NAN_VALUE_COUNT,
    LOWER_BOUND,
    UPPER_BOUND
  }

  private final ContentStats stats;
  private final Kind kind;
  private Map<Integer, V> materialized;

  ContentStatsBackedMap(ContentStats stats, Kind kind) {
    this.stats = stats;
    this.kind = kind;
  }

  /** Returns a lazy view of the metric across columns, or {@code null} if no column tracks it. */
  static <V> Map<Integer, V> forKind(Kind kind, ContentStats stats) {
    return isEmpty(stats, kind) ? null : new ContentStatsBackedMap<>(stats, kind);
  }

  @Override
  public V get(Object key) {
    if (!(key instanceof Integer)) {
      throw new ClassCastException("Key must be an Integer field id: " + key);
    }

    FieldStats<?> fieldStats = stats.statsFor((Integer) key);
    if (fieldStats == null) {
      return null;
    }

    return getStatValue(fieldStats, kind);
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public boolean isEmpty() {
    // avoid AbstractMap's default, which materializes entrySet() just to answer emptiness
    return isEmpty(stats, kind);
  }

  @Override
  public Set<Entry<Integer, V>> entrySet() {
    if (materialized == null) {
      Map<Integer, V> entries = Maps.newLinkedHashMap();
      for (FieldStats<?> fieldStats : stats.fieldStats()) {
        if (fieldStats != null) {
          V value = getStatValue(fieldStats, kind);
          if (value != null) {
            entries.put(fieldStats.fieldId(), value);
          }
        }
      }

      this.materialized = entries;
    }

    return materialized.entrySet();
  }

  /** Returns whether no column contributes an entry for the metric. */
  static boolean isEmpty(ContentStats stats, Kind kind) {
    for (FieldStats<?> fieldStats : stats.fieldStats()) {
      if (fieldStats != null && contributes(fieldStats, kind)) {
        return false;
      }
    }

    return true;
  }

  // Whether getStatValue would return a non-null value, without allocating a boxed count or
  // decoding a bound. Must mirror getStatValue's null-ness.
  private static boolean contributes(FieldStats<?> fieldStats, Kind kind) {
    switch (kind) {
      case VALUE_COUNT:
        return true;
      case NULL_VALUE_COUNT:
        return fieldStats.hasNullCount();
      case NAN_VALUE_COUNT:
        return fieldStats.hasNaNCount();
      case LOWER_BOUND:
        return fieldStats.lowerBound() != null
            && tracksStat(fieldStats, StatsUtil.LOWER_BOUND_OFFSET);
      case UPPER_BOUND:
        return fieldStats.upperBound() != null
            && tracksStat(fieldStats, StatsUtil.UPPER_BOUND_OFFSET);
      default:
        throw new IllegalArgumentException("Unknown content stats kind: " + kind);
    }
  }

  // Whether the field's stats struct declares the metric at the given offset.
  private static boolean tracksStat(FieldStats<?> fieldStats, int statOffset) {
    return fieldStats.type().field(StatsUtil.toBaseId(fieldStats.fieldId()) + statOffset) != null;
  }

  @SuppressWarnings("unchecked")
  private static <V> V getStatValue(FieldStats<?> fieldStats, Kind kind) {
    switch (kind) {
      case VALUE_COUNT:
        return (V) Long.valueOf(fieldStats.valueCount());
      case NULL_VALUE_COUNT:
        return fieldStats.hasNullCount() ? (V) Long.valueOf(fieldStats.nullValueCount()) : null;
      case NAN_VALUE_COUNT:
        return fieldStats.hasNaNCount() ? (V) Long.valueOf(fieldStats.nanValueCount()) : null;
      case LOWER_BOUND:
        return (V) bound(fieldStats, fieldStats.lowerBound(), StatsUtil.LOWER_BOUND_OFFSET);
      case UPPER_BOUND:
        return (V) bound(fieldStats, fieldStats.upperBound(), StatsUtil.UPPER_BOUND_OFFSET);
      default:
        throw new IllegalArgumentException("Unknown content stats kind: " + kind);
    }
  }

  private static ByteBuffer bound(FieldStats<?> fieldStats, Object bound, int boundOffset) {
    Types.NestedField boundField =
        fieldStats.type().field(StatsUtil.toBaseId(fieldStats.fieldId()) + boundOffset);
    if (bound == null || boundField == null) {
      return null;
    }

    return Conversions.toByteBuffer(boundField.type(), bound);
  }
}
