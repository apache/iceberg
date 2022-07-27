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
package org.apache.iceberg.mr.hive.vector;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Collects the top level field names from Parquet schema. During schema visit it translates the
 * expected schema's field names to what fields the visitor can match in the file schema to support
 * column renames.
 */
class ParquetSchemaFieldNameVisitor extends TypeWithSchemaVisitor<Type> {
  private final MessageType originalFileSchema;
  private final Map<Integer, Type> typesById = Maps.newHashMap();
  private StringBuilder sb = new StringBuilder();
  private static final String DUMMY_COL_NAME = "<<DUMMY_FOR_RECREATED_FIELD_IN_FILESCHEMA>>";

  ParquetSchemaFieldNameVisitor(MessageType originalFileSchema) {
    this.originalFileSchema = originalFileSchema;
  }

  @Override
  public Type message(Types.StructType expected, MessageType prunedFileSchema, List<Type> fields) {
    return this.struct(expected, prunedFileSchema.asGroupType(), fields);
  }

  @Override
  public Type struct(Types.StructType expected, GroupType struct, List<Type> fields) {
    boolean isMessageType = struct instanceof MessageType;

    List<Types.NestedField> expectedFields =
        expected != null ? expected.fields() : ImmutableList.of();
    List<Type> types = Lists.newArrayListWithExpectedSize(expectedFields.size());

    for (Types.NestedField field : expectedFields) {
      int id = field.fieldId();
      if (MetadataColumns.metadataFieldIds().contains(id)) {
        continue;
      }

      Type fieldInPrunedFileSchema = typesById.get(id);
      if (fieldInPrunedFileSchema == null) {
        if (!originalFileSchema.containsField(field.name())) {
          // Must be a new field - it isn't in this parquet file yet, so add the new field name
          // instead of null
          appendToColNamesList(isMessageType, field.name());
        } else {
          // This field is found in the parquet file with a different ID, so it must have been
          // recreated since.
          // Inserting a dummy col name to force Hive Parquet reader returning null for this column.
          appendToColNamesList(isMessageType, DUMMY_COL_NAME);
        }
      } else {
        // Already present column in this parquet file, add the original name
        types.add(fieldInPrunedFileSchema);
        appendToColNamesList(isMessageType, fieldInPrunedFileSchema.getName());
      }
    }

    if (!isMessageType) {
      GroupType groupType = new GroupType(Type.Repetition.REPEATED, fieldNames.peek(), types);
      typesById.put(struct.getId().intValue(), groupType);
      return groupType;
    } else {
      return new MessageType("table", types);
    }
  }

  private void appendToColNamesList(boolean isMessageType, String colName) {
    if (isMessageType) {
      sb.append(colName).append(',');
    }
  }

  @Override
  public Type primitive(
      org.apache.iceberg.types.Type.PrimitiveType expected,
      org.apache.parquet.schema.PrimitiveType primitive) {
    typesById.put(primitive.getId().intValue(), primitive);
    return primitive;
  }

  @Override
  public Type list(Types.ListType iList, GroupType array, Type element) {
    typesById.put(array.getId().intValue(), array);
    return array;
  }

  @Override
  public Type map(Types.MapType iMap, GroupType map, Type key, Type value) {
    typesById.put(map.getId().intValue(), map);
    return map;
  }

  public String retrieveColumnNameList() {
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }
}
