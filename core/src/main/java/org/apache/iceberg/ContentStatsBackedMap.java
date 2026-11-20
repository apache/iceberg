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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

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

  /** Per-column value counts, or null if no column tracks the value count. */
  static <V> Map<Integer, V> valueCounts(ContentStats stats) {
    return viewOrNull(stats, Kind.VALUE_COUNT);
  }

  /** Per-column null value counts, or null if no column tracks the null value count. */
  static <V> Map<Integer, V> nullValueCounts(ContentStats stats) {
    return viewOrNull(stats, Kind.NULL_VALUE_COUNT);
  }

  /** Per-column NaN value counts, or null if no column tracks the NaN value count. */
  static <V> Map<Integer, V> nanValueCounts(ContentStats stats) {
    return viewOrNull(stats, Kind.NAN_VALUE_COUNT);
  }

  /** Per-column lower bounds, or null if no column tracks a lower bound. */
  static <V> Map<Integer, V> lowerBounds(ContentStats stats) {
    return viewOrNull(stats, Kind.LOWER_BOUND);
  }

  /** Per-column upper bounds, or null if no column tracks an upper bound. */
  static <V> Map<Integer, V> upperBounds(ContentStats stats) {
    return viewOrNull(stats, Kind.UPPER_BOUND);
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
    // a factory returns null for an empty view, so a constructed instance always has an entry;
    // this also avoids AbstractMap's default, which materializes entrySet() to answer emptiness
    return false;
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

  private static <V> Map<Integer, V> viewOrNull(ContentStats stats, Kind kind) {
    return isEmpty(stats, kind) ? null : new ContentStatsBackedMap<>(stats, kind);
  }

  /** Returns true if no field tracks the metric. */
  private static boolean isEmpty(ContentStats stats, Kind kind) {
    if (stats == null) {
      return true;
    }

    for (FieldStats<?> fieldStats : stats.fieldStats()) {
      if (fieldStats != null && isKnown(fieldStats, kind)) {
        return false;
      }
    }

    return true;
  }

  private static boolean isKnown(FieldStats<?> fieldStats, Kind kind) {
    return switch (kind) {
      case VALUE_COUNT -> fieldStats.hasValueCount();
      case NULL_VALUE_COUNT -> fieldStats.hasNullValueCount();
      case NAN_VALUE_COUNT -> fieldStats.hasNanValueCount();
      case LOWER_BOUND -> hasBound(fieldStats, fieldStats.lowerBound(), StatsUtil.LOWER_BOUND_NAME);
      case UPPER_BOUND -> hasBound(fieldStats, fieldStats.upperBound(), StatsUtil.UPPER_BOUND_NAME);
    };
  }

  private static boolean hasBound(FieldStats<?> fieldStats, Object bound, String boundFieldName) {
    if (bound == null) {
      return false;
    }

    Type boundType = fieldStats.type().fieldType(boundFieldName);
    return boundType != null
        && (!boundType.isStructType() || projectsGeoXY(boundType.asStructType()));
  }

  @SuppressWarnings("unchecked")
  private static <V> V statValue(FieldStats<?> fieldStats, Kind kind) {
    return switch (kind) {
      case VALUE_COUNT ->
          fieldStats.hasValueCount() ? (V) Long.valueOf(fieldStats.valueCount()) : null;
      case NULL_VALUE_COUNT ->
          fieldStats.hasNullValueCount() ? (V) Long.valueOf(fieldStats.nullValueCount()) : null;
      case NAN_VALUE_COUNT ->
          fieldStats.hasNanValueCount() ? (V) Long.valueOf(fieldStats.nanValueCount()) : null;
      case LOWER_BOUND ->
          (V) bound(fieldStats, fieldStats.lowerBound(), StatsUtil.LOWER_BOUND_NAME);
      case UPPER_BOUND ->
          (V) bound(fieldStats, fieldStats.upperBound(), StatsUtil.UPPER_BOUND_NAME);
    };
  }

  private static ByteBuffer bound(FieldStats<?> fieldStats, Object bound, String boundFieldName) {
    Type boundType = fieldStats.type().fieldType(boundFieldName);
    if (boundType == null) {
      return null;
    }

    if (boundType.isStructType()) {
      // geo bounds are bounding-box structs; convert to the spec's single-point encoding
      return geoBound(boundType.asStructType(), (StructLike) bound);
    }

    // toByteBuffer returns null for a null bound
    return Conversions.toByteBuffer(boundType, bound);
  }

  /**
   * Encodes a geo bounding-box struct as the spec's single-point bound, or returns null if the
   * struct is projected without x or y.
   */
  private static ByteBuffer geoBound(Types.StructType boundType, StructLike bound) {
    if (bound == null || !projectsGeoXY(boundType)) {
      return null;
    }

    double coordX = coordinate(boundType, bound, StatsUtil.GEO_BOUND_X_NAME);
    double coordY = coordinate(boundType, bound, StatsUtil.GEO_BOUND_Y_NAME);
    Double coordZ = coordinate(boundType, bound, StatsUtil.GEO_BOUND_Z_NAME);
    Double coordM = coordinate(boundType, bound, StatsUtil.GEO_BOUND_M_NAME);

    if (coordZ != null && coordM != null) {
      return GeospatialBound.createXYZM(coordX, coordY, coordZ, coordM).toByteBuffer();
    } else if (coordZ != null) {
      return GeospatialBound.createXYZ(coordX, coordY, coordZ).toByteBuffer();
    } else if (coordM != null) {
      return GeospatialBound.createXYM(coordX, coordY, coordM).toByteBuffer();
    } else {
      return GeospatialBound.createXY(coordX, coordY).toByteBuffer();
    }
  }

  private static boolean projectsGeoXY(Types.StructType boundType) {
    return boundType.field(StatsUtil.GEO_BOUND_X_NAME) != null
        && boundType.field(StatsUtil.GEO_BOUND_Y_NAME) != null;
  }

  private static Double coordinate(Types.StructType boundType, StructLike bound, String name) {
    List<Types.NestedField> fields = boundType.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      if (fields.get(pos).name().equals(name)) {
        return bound.get(pos, Double.class);
      }
    }

    return null;
  }
}
