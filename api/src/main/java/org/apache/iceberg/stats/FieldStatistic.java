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
package org.apache.iceberg.stats;

import static org.apache.iceberg.types.Types.NestedField.optional;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public enum FieldStatistic {
  VALUE_COUNT(1, "value_count"),
  NULL_VALUE_COUNT(2, "null_value_count"),
  NAN_VALUE_COUNT(3, "nan_value_count"),
  AVG_VALUE_SIZE(4, "avg_value_size"),
  MAX_VALUE_SIZE(5, "max_value_size"),
  LOWER_BOUND(6, "lower_bound"),
  UPPER_BOUND(7, "upper_bound"),
  EXACT_BOUNDS(8, "exact_bounds");

  private final int offset;
  private final String fieldName;

  FieldStatistic(int offset, String fieldName) {
    this.offset = offset;
    this.fieldName = fieldName;
  }

  /**
   * The offset from the field ID of the base stats structure
   *
   * @return The offset from the field ID of the base strats structure
   */
  public int offset() {
    return offset;
  }

  /**
   * The ordinal position (0-based) within the stats structure
   *
   * @return The ordinal position (0-based) within the stats structure
   */
  public int position() {
    return offset - 1;
  }

  /**
   * The field name
   *
   * @return The field name
   */
  public String fieldName() {
    return fieldName;
  }

  /**
   * Returns the {@link FieldStatistic} from its ordinal position (0-based) in the stats structure
   *
   * @param position The ordinal position (0-based) in the stats structure
   * @return The {@link FieldStatistic} from its ordinal position (0-based) in the stats structure
   */
  public static FieldStatistic fromPosition(int position) {
    return switch (position) {
      case 0 -> VALUE_COUNT;
      case 1 -> NULL_VALUE_COUNT;
      case 2 -> NAN_VALUE_COUNT;
      case 3 -> AVG_VALUE_SIZE;
      case 4 -> MAX_VALUE_SIZE;
      case 5 -> LOWER_BOUND;
      case 6 -> UPPER_BOUND;
      case 7 -> EXACT_BOUNDS;
      default -> throw new IllegalArgumentException("Invalid statistic position: " + position);
    };
  }

  public static Types.StructType fieldStatsFor(Type type, int baseFieldId) {
    return Types.StructType.of(
        optional(
            baseFieldId + VALUE_COUNT.offset(),
            VALUE_COUNT.fieldName(),
            Types.LongType.get(),
            "Total value count, including null and NaN"),
        optional(
            baseFieldId + NULL_VALUE_COUNT.offset(),
            NULL_VALUE_COUNT.fieldName(),
            Types.LongType.get(),
            "Total null value count"),
        optional(
            baseFieldId + NAN_VALUE_COUNT.offset(),
            NAN_VALUE_COUNT.fieldName(),
            Types.LongType.get(),
            "Total NaN value count"),
        optional(
            baseFieldId + AVG_VALUE_SIZE.offset(),
            AVG_VALUE_SIZE.fieldName(),
            Types.IntegerType.get(),
            "Avg value size of variable-length types (String, Binary)"),
        optional(
            baseFieldId + MAX_VALUE_SIZE.offset(),
            MAX_VALUE_SIZE.fieldName(),
            Types.IntegerType.get(),
            "Max value size of variable-length types (String, Binary)"),
        optional(baseFieldId + LOWER_BOUND.offset(), LOWER_BOUND.fieldName(), type, "Lower bound"),
        optional(baseFieldId + UPPER_BOUND.offset(), UPPER_BOUND.fieldName(), type, "Upper bound"),
        optional(
            baseFieldId + EXACT_BOUNDS.offset(),
            EXACT_BOUNDS.fieldName(),
            Types.BooleanType.get(),
            "Whether the upper/lower bound is exact or not"));
  }
}
