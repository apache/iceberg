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
package org.apache.iceberg.flink.data;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;

/**
 * Lets the write path treat a Flink {@code MULTISET<T>} as the map it is stored as.
 *
 * <p>{@link org.apache.iceberg.flink.FlinkTypeToType#visit(MultisetType)} converts a {@code
 * MULTISET<T>} to an Iceberg {@code map<T, int>} of element to occurrence count, and {@code
 * RowData} represents both a map and a multiset as {@code MapData}, so the writers only need the
 * equivalent {@link MapType} rather than a separate multiset code path.
 */
class FlinkMultisets {

  /**
   * The occurrence count is not nullable, because {@code FlinkTypeToType#visit(MultisetType)}
   * produces {@code Types.MapType#ofRequired}.
   */
  private static final IntType OCCURRENCE_COUNT_TYPE = new IntType(false);

  private FlinkMultisets() {}

  /** Returns whether the type is stored as a map, that is, a {@code MAP} or a {@code MULTISET}. */
  static boolean isMapLike(LogicalType logicalType) {
    return logicalType instanceof MapType || logicalType instanceof MultisetType;
  }

  /**
   * Returns the given type as a {@link MapType}, converting a {@code MULTISET<T>} to {@code map<T,
   * int not null>}.
   *
   * @throws ClassCastException if the type is neither a {@code MAP} nor a {@code MULTISET}
   */
  static MapType asMapType(LogicalType logicalType) {
    if (logicalType instanceof MultisetType multisetType) {
      return new MapType(
          logicalType.isNullable(), multisetType.getElementType(), OCCURRENCE_COUNT_TYPE);
    }

    return (MapType) logicalType;
  }
}
