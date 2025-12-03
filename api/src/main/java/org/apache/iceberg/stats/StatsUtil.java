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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StatsUtil.class);
  // the number of reserved field IDs from the reserved field ID space as defined in
  // https://iceberg.apache.org/spec/#reserved-field-ids
  static final int NUM_RESERVED_FIELD_IDS = 200;
  // the starting field ID of the reserved field ID space
  static final int RESERVED_FIELD_IDS_START = Integer.MAX_VALUE - NUM_RESERVED_FIELD_IDS;
  // the number of supported stats per table column
  static final int NUM_SUPPORTED_STATS_PER_COLUMN = 200;
  // the starting field ID of the stats space for data field IDs
  static final int STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS = 10_000;
  // the starting field ID of the stats space for metadata field IDs
  static final int STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS = 2_147_000_000;
  // support stats for only up to this amount of data field IDs
  static final int MAX_DATA_FIELD_ID = 1_000_000;
  static final int MAX_DATA_STATS_FIELD_ID = 200_010_000;

  private StatsUtil() {}

  public static int statsFieldIdForField(int fieldId) {
    return fieldId >= RESERVED_FIELD_IDS_START
        ? statsFieldIdForReservedField(fieldId)
        : statsFieldIdForDataField(fieldId);
  }

  private static int statsFieldIdForDataField(int fieldId) {
    long statsFieldId =
        STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS
            + (NUM_SUPPORTED_STATS_PER_COLUMN * (long) fieldId);
    if (fieldId < 0 || fieldId > MAX_DATA_FIELD_ID) {
      return -1;
    }

    return (int) statsFieldId;
  }

  private static int statsFieldIdForReservedField(int fieldId) {
    int offset = NUM_RESERVED_FIELD_IDS - (Integer.MAX_VALUE - fieldId);

    long statsFieldId =
        STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS
            + (NUM_SUPPORTED_STATS_PER_COLUMN * (long) offset);
    if (statsFieldId < 0 || statsFieldId > RESERVED_FIELD_IDS_START) {
      // ID overflows
      return -1;
    }

    return (int) statsFieldId;
  }

  public static int fieldIdForStatsField(int statsFieldId) {
    if (statsFieldId < STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS
        || statsFieldId % NUM_SUPPORTED_STATS_PER_COLUMN != 0) {
      return -1;
    }

    return statsFieldId < STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS
        ? fieldIdForStatsFieldFromDataField(statsFieldId)
        : fieldIdForStatsFieldFromReservedField(statsFieldId);
  }

  private static int fieldIdForStatsFieldFromDataField(int statsFieldId) {
    return Math.max(
        -1,
        (statsFieldId - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
            / NUM_SUPPORTED_STATS_PER_COLUMN);
  }

  private static int fieldIdForStatsFieldFromReservedField(int statsFieldId) {
    return Math.max(
        -1,
        statsFieldId
            - NUM_RESERVED_FIELD_IDS
            + (Integer.MAX_VALUE - statsFieldId)
            + (statsFieldId - STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS)
                / NUM_SUPPORTED_STATS_PER_COLUMN);
  }

  public static Types.NestedField contentStatsFor(Schema schema) {
    ContentStatsSchemaVisitor visitor = new ContentStatsSchemaVisitor();
    Types.NestedField result = TypeUtil.visit(schema, visitor);
    if (!visitor.skippedFieldIds.isEmpty()) {
      LOG.warn("Could not create stats schema for field ids: {}", visitor.skippedFieldIds);
    }

    return result;
  }

  private static class ContentStatsSchemaVisitor extends TypeUtil.SchemaVisitor<Types.NestedField> {
    private final List<Types.NestedField> statsFields = Lists.newArrayList();
    private final Set<Integer> skippedFieldIds = Sets.newLinkedHashSet();

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

      int fieldId = StatsUtil.statsFieldIdForField(field.fieldId());
      if (fieldId >= 0) {
        Types.StructType structType = FieldStatistic.fieldStatsFor(field.type(), fieldId + 1);
        return optional(fieldId, Integer.toString(field.fieldId()), structType);
      } else {
        skippedFieldIds.add(field.fieldId());
      }

      return null;
    }

    @Override
    public Types.NestedField variant(Types.VariantType variant) {
      return null;
    }
  }
}
