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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

/**
 * A lazy, read-only {@link Map} view of one stat across the columns of a {@link ContentStats},
 * keyed by field ID, mirroring the per-column stat maps on {@link ContentFile}.
 */
class ContentStatsBackedMap<V> extends AbstractMap<Integer, V> {
  private enum Kind {
    VALUE_COUNT,
    NULL_VALUE_COUNT,
    NAN_VALUE_COUNT,
    LOWER_BOUND,
    UPPER_BOUND
  }

  /** Per-column value counts, or {@code null} if no column tracks the value count. */
  static <V> Map<Integer, V> valueCounts(ContentStats stats) {
    return viewOrNull(stats, Kind.VALUE_COUNT);
  }

  /** Per-column null value counts, or {@code null} if no column tracks the null value count. */
  static <V> Map<Integer, V> nullValueCounts(ContentStats stats) {
    return viewOrNull(stats, Kind.NULL_VALUE_COUNT);
  }

  /** Per-column NaN value counts, or {@code null} if no column tracks the NaN value count. */
  static <V> Map<Integer, V> nanValueCounts(ContentStats stats) {
    return viewOrNull(stats, Kind.NAN_VALUE_COUNT);
  }

  /** Per-column lower bounds, or {@code null} if no column tracks a lower bound. */
  static <V> Map<Integer, V> lowerBounds(ContentStats stats) {
    return viewOrNull(stats, Kind.LOWER_BOUND);
  }

  /** Per-column upper bounds, or {@code null} if no column tracks an upper bound. */
  static <V> Map<Integer, V> upperBounds(ContentStats stats) {
    return viewOrNull(stats, Kind.UPPER_BOUND);
  }

  private static <V> Map<Integer, V> viewOrNull(ContentStats stats, Kind kind) {
    return isEmpty(stats, kind) ? null : new ContentStatsBackedMap<>(stats, kind);
  }

  private final ContentStats stats;
  private final Kind kind;
  private Set<Entry<Integer, V>> materialized;

  private ContentStatsBackedMap(ContentStats stats, Kind kind) {
    this.stats = stats;
    this.kind = kind;
  }

  @Override
  public V get(Object key) {
    if (!(key instanceof Integer)) {
      return null;
    }

    FieldStats<?> fieldStats = stats.statsFor((Integer) key);
    if (fieldStats == null) {
      return null;
    }

    return statValue(fieldStats, kind);
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
      Set<Entry<Integer, V>> entries = Sets.newLinkedHashSet();
      for (FieldStats<?> fieldStats : stats.fieldStats()) {
        if (fieldStats != null) {
          V value = statValue(fieldStats, kind);
          if (value != null) {
            entries.add(new SimpleImmutableEntry<>(fieldStats.fieldId(), value));
          }
        }
      }

      this.materialized = entries;
    }

    return materialized;
  }

  /** Returns whether no column contributes an entry for the metric. */
  private static boolean isEmpty(ContentStats stats, Kind kind) {
    for (FieldStats<?> fieldStats : stats.fieldStats()) {
      if (fieldStats != null && isKnown(fieldStats, kind)) {
        return false;
      }
    }

    return true;
  }

  private static boolean isKnown(FieldStats<?> fieldStats, Kind kind) {
    switch (kind) {
      case VALUE_COUNT:
        return fieldStats.hasValueCount();
      case NULL_VALUE_COUNT:
        return fieldStats.hasNullValueCount();
      case NAN_VALUE_COUNT:
        return fieldStats.hasNanValueCount();
      case LOWER_BOUND:
        return fieldStats.lowerBound() != null;
      case UPPER_BOUND:
        return fieldStats.upperBound() != null;
      default:
        throw new IllegalArgumentException("Unknown content stats kind: " + kind);
    }
  }

  @SuppressWarnings("unchecked")
  private static <V> V statValue(FieldStats<?> fieldStats, Kind kind) {
    switch (kind) {
      case VALUE_COUNT:
        return fieldStats.hasValueCount() ? (V) Long.valueOf(fieldStats.valueCount()) : null;
      case NULL_VALUE_COUNT:
        return fieldStats.hasNullValueCount()
            ? (V) Long.valueOf(fieldStats.nullValueCount())
            : null;
      case NAN_VALUE_COUNT:
        return fieldStats.hasNanValueCount() ? (V) Long.valueOf(fieldStats.nanValueCount()) : null;
      case LOWER_BOUND:
        return (V) bound(fieldStats, fieldStats.lowerBound(), StatsUtil.LOWER_BOUND_NAME);
      case UPPER_BOUND:
        return (V) bound(fieldStats, fieldStats.upperBound(), StatsUtil.UPPER_BOUND_NAME);
      default:
        throw new IllegalArgumentException("Unknown content stats kind: " + kind);
    }
  }

  private static ByteBuffer bound(FieldStats<?> fieldStats, Object bound, String boundFieldName) {
    Type boundType = fieldStats.type().fieldType(boundFieldName);
    // toByteBuffer returns null for a null bound
    return boundType == null ? null : Conversions.toByteBuffer(boundType, bound);
  }
}
