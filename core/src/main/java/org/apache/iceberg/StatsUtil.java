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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StatsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StatsUtil.class);
  static final Set<Integer> SUPPORTED_METADATA_FIELD_IDS =
      ImmutableSet.of(
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), MetadataColumns.ROW_ID.fieldId());
  private static final int FIRST_SUPPORTED_METADATA_FIELD_ID =
      Collections.min(SUPPORTED_METADATA_FIELD_IDS);
  static final int NUM_SUPPORTED_STATS_PER_COLUMN = 200;
  static final int STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS = 9_000;
  static final int STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS = 10_000;
  // exclusive upper bound of the stats field ID range reserved for content_stats
  static final int STATS_SPACE_FIELD_ID_END = 200_000_000;
  static final int MAX_DATA_STATS_FIELD_ID =
      STATS_SPACE_FIELD_ID_END - NUM_SUPPORTED_STATS_PER_COLUMN;
  // the max data field ID whose stats struct fits within the reserved range
  static final int MAX_DATA_FIELD_ID =
      (MAX_DATA_STATS_FIELD_ID - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
          / NUM_SUPPORTED_STATS_PER_COLUMN;

  private StatsUtil() {}

  public static int statsFieldIdForField(int fieldId) {
    return SUPPORTED_METADATA_FIELD_IDS.contains(fieldId)
        ? statsFieldIdForReservedField(fieldId)
        : statsFieldIdForDataField(fieldId);
  }

  private static int statsFieldIdForDataField(int fieldId) {
    if (fieldId < 0 || fieldId > MAX_DATA_FIELD_ID) {
      return -1;
    }

    return STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS + (NUM_SUPPORTED_STATS_PER_COLUMN * fieldId);
  }

  private static int statsFieldIdForReservedField(int fieldId) {
    return STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS
        + (NUM_SUPPORTED_STATS_PER_COLUMN * (fieldId - FIRST_SUPPORTED_METADATA_FIELD_ID));
  }

  public static int fieldIdForStatsField(int statsFieldId) {
    if (statsFieldId < STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS
        || statsFieldId >= STATS_SPACE_FIELD_ID_END
        || statsFieldId % NUM_SUPPORTED_STATS_PER_COLUMN != 0) {
      return -1;
    }

    return statsFieldId < STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS
        ? fieldIdForStatsFieldFromReservedField(statsFieldId)
        : fieldIdForStatsFieldFromDataField(statsFieldId);
  }

  private static int fieldIdForStatsFieldFromDataField(int statsFieldId) {
    return (statsFieldId - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
        / NUM_SUPPORTED_STATS_PER_COLUMN;
  }

  private static int fieldIdForStatsFieldFromReservedField(int statsFieldId) {
    int fieldId =
        (statsFieldId - STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS)
                / NUM_SUPPORTED_STATS_PER_COLUMN
            + FIRST_SUPPORTED_METADATA_FIELD_ID;
    return SUPPORTED_METADATA_FIELD_IDS.contains(fieldId) ? fieldId : -1;
  }

  public static Types.NestedField contentStatsFor(Schema schema) {
    ContentStatsSchemaVisitor visitor = new ContentStatsSchemaVisitor(schema);
    Types.NestedField result = TypeUtil.visit(schema, visitor);
    if (!visitor.skippedFieldIds.isEmpty()) {
      LOG.warn("Could not create stats schema for field ids: {}", visitor.skippedFieldIds);
    }

    return result;
  }

  private static class ContentStatsSchemaVisitor extends TypeUtil.SchemaVisitor<Types.NestedField> {
    private final Schema tableSchema;
    private final List<Types.NestedField> statsFields = Lists.newArrayList();
    private final Set<Integer> skippedFieldIds = Sets.newLinkedHashSet();

    ContentStatsSchemaVisitor(Schema tableSchema) {
      this.tableSchema = tableSchema;
    }

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
      if (field.type().isNestedType()) {
        return null;
      }

      int fieldId = StatsUtil.statsFieldIdForField(field.fieldId());
      if (fieldId >= 0) {
        Types.StructType structType = FieldStatistic.fieldStatsFor(field, fieldId);
        String fullName = tableSchema.findColumnName(field.fieldId());
        return optional(fieldId, fullName, structType);
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
