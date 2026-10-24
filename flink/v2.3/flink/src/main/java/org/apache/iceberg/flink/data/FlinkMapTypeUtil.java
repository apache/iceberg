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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Lets the write path treat a Flink {@code MULTISET<T>} as the map it is stored as. */
class FlinkMapTypeUtil {

  // Required, to match the map<T, int> that FlinkTypeToType creates for a MULTISET<T>
  private static final IntType OCCURRENCE_COUNT_TYPE = new IntType(false);

  private FlinkMapTypeUtil() {}

  static boolean isMapType(LogicalType logicalType) {
    return logicalType instanceof MapType || logicalType instanceof MultisetType;
  }

  /**
   * Returns the type as a {@link MapType}, with a {@code MULTISET<T>} as {@code map<T, int not
   * null>}.
   *
   * @throws IllegalArgumentException if the type is neither a {@code MAP} nor a {@code MULTISET}
   */
  static MapType asMapType(LogicalType logicalType) {
    Preconditions.checkArgument(
        isMapType(logicalType), "Invalid map: %s is not a map or multiset", logicalType);
    if (logicalType instanceof MultisetType multisetType) {
      return new MapType(
          logicalType.isNullable(), multisetType.getElementType(), OCCURRENCE_COUNT_TYPE);
    }

    return (MapType) logicalType;
  }
}
