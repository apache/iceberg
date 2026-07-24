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

import org.apache.iceberg.types.Types;

class StatsTestUtil {
  private StatsTestUtil() {}

  /**
   * Builds a {@link FieldStatsStruct} through the setter path, the way a reader populates one: only
   * the supplied fields that also exist in the schema are set, so a null count stays absent and
   * {@code hasNullValueCount()}/{@code hasNanValueCount()} report false. This keeps "absent count"
   * states out of the production constructor.
   */
  static <T> FieldStatsStruct<T> fieldStats(
      Types.StructType struct,
      T lower,
      T upper,
      Boolean tightBounds,
      Long valueCount,
      Long nullCount,
      Long nanCount,
      Integer avgSize) {
    FieldStatsStruct<T> stats = new FieldStatsStruct<>(struct);
    setIfPresent(stats, struct, "lower_bound", lower);
    setIfPresent(stats, struct, "upper_bound", upper);
    setIfPresent(stats, struct, "tight_bounds", tightBounds);
    setIfPresent(stats, struct, "value_count", valueCount);
    setIfPresent(stats, struct, "null_value_count", nullCount);
    setIfPresent(stats, struct, "nan_value_count", nanCount);
    setIfPresent(stats, struct, "avg_value_size_in_bytes", avgSize);
    return stats;
  }

  private static void setIfPresent(
      FieldStatsStruct<?> stats, Types.StructType struct, String fieldName, Object value) {
    Types.NestedField field = struct.field(fieldName);
    if (value == null || field == null) {
      return;
    }

    stats.set(struct.fields().indexOf(field), value);
  }
}
