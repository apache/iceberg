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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

enum FieldStatistic {
  LOWER_BOUND(1, "lower_bound"),
  UPPER_BOUND(2, "upper_bound"),
  TIGHT_BOUNDS(3, "tight_bounds"),
  VALUE_COUNT(4, "value_count"),
  NULL_VALUE_COUNT(5, "null_value_count"),
  NAN_VALUE_COUNT(6, "nan_value_count"),
  AVG_VALUE_SIZE_IN_BYTES(7, "avg_value_size_in_bytes");

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
      case 0 -> LOWER_BOUND;
      case 1 -> UPPER_BOUND;
      case 2 -> TIGHT_BOUNDS;
      case 3 -> VALUE_COUNT;
      case 4 -> NULL_VALUE_COUNT;
      case 5 -> NAN_VALUE_COUNT;
      case 6 -> AVG_VALUE_SIZE_IN_BYTES;
      default -> throw new IllegalArgumentException("Invalid statistic position: " + position);
    };
  }

  public static Types.StructType fieldStatsFor(Types.NestedField field, int baseFieldId) {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(7);
    Type type = field.type();
    Type.TypeID typeId = type.typeId();
    boolean isGeo = typeId == Type.TypeID.GEOMETRY || typeId == Type.TypeID.GEOGRAPHY;
    boolean isVariant = type.isVariantType();

    fields.add(
        optional(
            baseFieldId + LOWER_BOUND.offset(),
            LOWER_BOUND.fieldName(),
            type,
            "Lower bound stored as the field's type"));
    fields.add(
        optional(
            baseFieldId + UPPER_BOUND.offset(),
            UPPER_BOUND.fieldName(),
            type,
            "Upper bound stored as the field's type"));

    if (!isGeo && !isVariant) {
      fields.add(
          optional(
              baseFieldId + TIGHT_BOUNDS.offset(),
              TIGHT_BOUNDS.fieldName(),
              Types.BooleanType.get(),
              "When true, lower_bound and upper_bound must be equal to the min and max values"));
    }

    fields.add(
        optional(
            baseFieldId + VALUE_COUNT.offset(),
            VALUE_COUNT.fieldName(),
            Types.LongType.get(),
            "Number of values in the column (including null and NaN values)"));

    if (field.isOptional()) {
      fields.add(
          optional(
              baseFieldId + NULL_VALUE_COUNT.offset(),
              NULL_VALUE_COUNT.fieldName(),
              Types.LongType.get(),
              "Number of null values in the column"));
    }

    if (typeId == Type.TypeID.FLOAT || typeId == Type.TypeID.DOUBLE) {
      fields.add(
          optional(
              baseFieldId + NAN_VALUE_COUNT.offset(),
              NAN_VALUE_COUNT.fieldName(),
              Types.LongType.get(),
              "Number of NaN values in the column"));
    }

    if (typeId == Type.TypeID.STRING || typeId == Type.TypeID.BINARY || isVariant) {
      fields.add(
          optional(
              baseFieldId + AVG_VALUE_SIZE_IN_BYTES.offset(),
              AVG_VALUE_SIZE_IN_BYTES.fieldName(),
              Types.IntegerType.get(),
              "Avg value size (uncompressed) in bytes to estimate memory consumption"));
    }

    return Types.StructType.of(fields);
  }
}
