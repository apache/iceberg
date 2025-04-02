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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class StatsUtil {
  private static final int NUM_STATS_PER_COLUMN = 200;
  static final int RESERVED_FIELD_IDS = 200;
  static final int DATA_SPACE_FIELD_ID_START = 100_000;
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

    int finalId = idSpaceStart + NUM_STATS_PER_COLUMN * id;
    if (finalId < 0
        || finalId > RESERVED_FIELD_IDS_START
        || (finalId >= METADATA_SPACE_FIELD_ID_START
            && idSpaceStart != METADATA_SPACE_FIELD_ID_START)) {
      // ID overflows
      return -1;
    }

    return finalId;
  }

  public static Types.NestedField contentStatsFor(Schema schema) {
    List<Types.NestedField> structFields = Lists.newArrayList();
    for (Types.NestedField field : schema.asStruct().fields()) {
      int fieldId = statsFieldIdFor(field.fieldId());
      // don't overflow and don't overlap with the metadata ID range
      if (fieldId >= 0) {
        Types.StructType structType = contentStatsFor(field.type(), fieldId + 1);
        Types.NestedField statsField =
            optional(fieldId, Integer.toString(field.fieldId()), structType);
        structFields.add(statsField);
      }
    }

    return optional(
        DataFile.CONTENT_STATS.fieldId(),
        DataFile.CONTENT_STATS.name(),
        Types.StructType.of(structFields));
  }

  private static Types.StructType contentStatsFor(Type type, int id) {
    int fieldId = id;
    Type boundType = type;
    if (type.isNestedType()) {
      boundType = Types.BinaryType.get();
    }

    return Types.StructType.of(
        optional(fieldId++, "column_size", Types.LongType.get(), "Total size on disk"),
        optional(
            fieldId++,
            "value_count",
            Types.LongType.get(),
            "Total value count, including null and NaN"),
        optional(fieldId++, "nan_value_count", Types.LongType.get(), "Total NaN value count"),
        optional(fieldId++, "null_value_count", Types.LongType.get(), "Total null value count"),
        optional(fieldId++, "lower_bound", boundType, "Lower bound"),
        optional(fieldId, "upper_bound", boundType, "Upper bound"));
  }
}
