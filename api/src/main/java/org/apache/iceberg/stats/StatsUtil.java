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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class StatsUtil {
  private static final int NUM_STATS_PER_COLUMN = 200;
  static final int RESERVED_FIELD_IDS = 200;
  static final int DATA_SPACE_FIELD_ID_START = 10_000;
  static final int METADATA_SPACE_FIELD_ID_START = 1_417_000_000;
  static final int RESERVED_FIELD_IDS_START = Integer.MAX_VALUE - RESERVED_FIELD_IDS;

  private StatsUtil() {}

  public static int statsFieldIdFor(int fieldId) {
    int idSpaceStart = DATA_SPACE_FIELD_ID_START;
    int id = fieldId;
    if (fieldId >= RESERVED_FIELD_IDS_START) {
      // this is a reserved field ID, which uses a different calculation
      idSpaceStart = METADATA_SPACE_FIELD_ID_START;
      id = RESERVED_FIELD_IDS - (Integer.MAX_VALUE - fieldId);
    }

    long finalId = idSpaceStart + NUM_STATS_PER_COLUMN * (long) id;
    if (finalId < 0
        || finalId > RESERVED_FIELD_IDS_START
        || (finalId >= METADATA_SPACE_FIELD_ID_START
            && idSpaceStart != METADATA_SPACE_FIELD_ID_START)) {
      // ID overflows
      return -1;
    }

    return (int) finalId;
  }

  public static int fieldIdFor(int statsFieldId) {
    if (statsFieldId < 0 || statsFieldId % NUM_STATS_PER_COLUMN != 0) {
      return -1;
    }

    int finalId;
    if (statsFieldId < METADATA_SPACE_FIELD_ID_START) {
      finalId = (statsFieldId - DATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN;
    } else {
      // this is a reserved field ID, which uses a different calculation
      finalId =
          statsFieldId
              - RESERVED_FIELD_IDS
              + (Integer.MAX_VALUE - statsFieldId)
              + (statsFieldId - METADATA_SPACE_FIELD_ID_START) / NUM_STATS_PER_COLUMN;
    }

    return Math.max(-1, finalId);
  }

  public static Types.NestedField contentStatsFor(Schema schema) {
    return TypeUtil.visit(schema, new ContentStatsSchemaVisitor());
  }

  private static Types.StructType contentStatsFor(Type type, int id) {
    int fieldId = id;

    return Types.StructType.of(
        optional(fieldId++, "column_size", Types.LongType.get(), "Total size on disk"),
        optional(
            fieldId++,
            "value_count",
            Types.LongType.get(),
            "Total value count, including null and NaN"),
        optional(fieldId++, "nan_value_count", Types.LongType.get(), "Total NaN value count"),
        optional(fieldId++, "null_value_count", Types.LongType.get(), "Total null value count"),
        optional(fieldId++, "lower_bound", type, "Lower bound"),
        optional(fieldId, "upper_bound", type, "Upper bound"));
  }

  private static class ContentStatsSchemaVisitor extends TypeUtil.SchemaVisitor<Types.NestedField> {
    private final List<Types.NestedField> statsFields = Lists.newArrayList();

    @Override
    public Types.NestedField schema(Schema schema, Types.NestedField structResult) {
      return optional(
          146,
          "content_stats",
          Types.StructType.of(
              statsFields.stream()
                  .filter(Objects::nonNull)
                  .sorted(Comparator.comparing(Types.NestedField::fieldId))
                  .collect(Collectors.toList())));
    }

    @Override
    public Types.NestedField list(Types.ListType list, Types.NestedField elementResult) {
      list.fields()
          .forEach(
              field -> {
                Types.NestedField result = field(field, null);
                if (null != result) {
                  statsFields.add(result);
                }
              });
      return null;
    }

    @Override
    public Types.NestedField map(
        Types.MapType map, Types.NestedField keyResult, Types.NestedField valueResult) {
      map.fields()
          .forEach(
              field -> {
                Types.NestedField result = field(field, null);
                if (null != result) {
                  statsFields.add(result);
                }
              });
      return null;
    }

    @Override
    public Types.NestedField struct(Types.StructType struct, List<Types.NestedField> fields) {
      statsFields.addAll(fields);
      return null;
    }

    @Override
    public Types.NestedField field(Types.NestedField field, Types.NestedField fieldResult) {
      if (field.type().isNestedType() || field.type().isVariantType()) {
        return null;
      }

      int fieldId = StatsUtil.statsFieldIdFor(field.fieldId());
      // don't overflow and don't overlap with the metadata ID range
      if (fieldId >= 0) {
        Types.StructType structType = contentStatsFor(field.type(), fieldId + 1);
        return optional(fieldId, Integer.toString(field.fieldId()), structType);
      }

      return null;
    }

    @Override
    public Types.NestedField variant(Types.VariantType variant) {
      return null;
    }
  }
}
