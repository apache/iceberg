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

import java.util.Deque;
import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class ParquetWithFlinkSchemaVisitor<T> {
  private final Deque<String> fieldNames = Lists.newLinkedList();

  public static <T> T visit(
      LogicalType sType, Type type, ParquetWithFlinkSchemaVisitor<T> visitor) {
    Preconditions.checkArgument(sType != null, "Invalid DataType: null");
    if (type instanceof MessageType) {
      Preconditions.checkArgument(
          sType instanceof RowType, "Invalid struct: %s is not a struct", sType);
      RowType struct = (RowType) sType;
      return visitor.message(
          struct, (MessageType) type, visitFields(struct, type.asGroupType(), visitor));
    } else if (type.isPrimitive()) {
      return visitor.primitive(sType, type.asPrimitiveType());
    } else {
      // if not a primitive, the typeId must be a group
      GroupType group = type.asGroupType();
      OriginalType annotation = group.getOriginalType();
      if (annotation != null) {
        switch (annotation) {
          case LIST:
            Preconditions.checkArgument(
                !group.isRepetition(Type.Repetition.REPEATED),
                "Invalid list: top-level group is repeated: %s",
                group);
            Preconditions.checkArgument(
                group.getFieldCount() == 1,
                "Invalid list: does not contain single repeated field: %s",
                group);

            GroupType repeatedElement = group.getFields().get(0).asGroupType();
            Preconditions.checkArgument(
                repeatedElement.isRepetition(Type.Repetition.REPEATED),
                "Invalid list: inner group is not repeated");
            Preconditions.checkArgument(
                repeatedElement.getFieldCount() <= 1,
                "Invalid list: repeated group is not a single field: %s",
                group);

            Preconditions.checkArgument(
                sType instanceof ArrayType, "Invalid list: %s is not an array", sType);
            ArrayType array = (ArrayType) sType;
            RowType.RowField element =
                new RowField(
                    "element", array.getElementType(), "element of " + array.asSummaryString());

            visitor.fieldNames.push(repeatedElement.getName());
            try {
              T elementResult = null;
              if (repeatedElement.getFieldCount() > 0) {
                elementResult = visitField(element, repeatedElement.getType(0), visitor);
              }

              return visitor.list(array, group, elementResult);

            } finally {
              visitor.fieldNames.pop();
            }

          case MAP:
            Preconditions.checkArgument(
                !group.isRepetition(Type.Repetition.REPEATED),
                "Invalid map: top-level group is repeated: %s",
                group);
            Preconditions.checkArgument(
                group.getFieldCount() == 1,
                "Invalid map: does not contain single repeated field: %s",
                group);

            GroupType repeatedKeyValue = group.getType(0).asGroupType();
            Preconditions.checkArgument(
                repeatedKeyValue.isRepetition(Type.Repetition.REPEATED),
                "Invalid map: inner group is not repeated");
            Preconditions.checkArgument(
                repeatedKeyValue.getFieldCount() <= 2,
                "Invalid map: repeated group does not have 2 fields");

            Preconditions.checkArgument(
                sType instanceof MapType, "Invalid map: %s is not a map", sType);
            MapType map = (MapType) sType;
            RowField keyField =
                new RowField("key", map.getKeyType(), "key of " + map.asSummaryString());
            RowField valueField =
                new RowField("value", map.getValueType(), "value of " + map.asSummaryString());

            visitor.fieldNames.push(repeatedKeyValue.getName());
            try {
              T keyResult = null;
              T valueResult = null;
              switch (repeatedKeyValue.getFieldCount()) {
                case 2:
                  // if there are 2 fields, both key and value are projected
                  keyResult = visitField(keyField, repeatedKeyValue.getType(0), visitor);
                  valueResult = visitField(valueField, repeatedKeyValue.getType(1), visitor);
                  break;
                case 1:
                  // if there is just one, use the name to determine what it is
                  Type keyOrValue = repeatedKeyValue.getType(0);
                  if (keyOrValue.getName().equalsIgnoreCase("key")) {
                    keyResult = visitField(keyField, keyOrValue, visitor);
                    // value result remains null
                  } else {
                    valueResult = visitField(valueField, keyOrValue, visitor);
                    // key result remains null
                  }
                  break;
                default:
                  // both results will remain null
              }

              return visitor.map(map, group, keyResult, valueResult);

            } finally {
              visitor.fieldNames.pop();
            }

          default:
        }
      }
      Preconditions.checkArgument(
          sType instanceof RowType, "Invalid struct: %s is not a struct", sType);
      RowType struct = (RowType) sType;
      return visitor.struct(struct, group, visitFields(struct, group, visitor));
    }
  }

  private static <T> T visitField(
      RowType.RowField sField, Type field, ParquetWithFlinkSchemaVisitor<T> visitor) {
    visitor.fieldNames.push(field.getName());
    try {
      return visit(sField.getType(), field, visitor);
    } finally {
      visitor.fieldNames.pop();
    }
  }

  private static <T> List<T> visitFields(
      RowType struct, GroupType group, ParquetWithFlinkSchemaVisitor<T> visitor) {
    List<RowType.RowField> sFields = struct.getFields();
    Preconditions.checkArgument(
        sFields.size() == group.getFieldCount(), "Structs do not match: %s and %s", struct, group);
    List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
    for (int i = 0; i < sFields.size(); i += 1) {
      Type field = group.getFields().get(i);
      RowType.RowField sField = sFields.get(i);
      Preconditions.checkArgument(
          field.getName().equals(AvroSchemaUtil.makeCompatibleName(sField.getName())),
          "Structs do not match: field %s != %s",
          field.getName(),
          sField.getName());
      results.add(visitField(sField, field, visitor));
    }

    return results;
  }

  public T message(RowType sStruct, MessageType message, List<T> fields) {
    return null;
  }

  public T struct(RowType sStruct, GroupType struct, List<T> fields) {
    return null;
  }

  public T list(ArrayType sArray, GroupType array, T element) {
    return null;
  }

  public T map(MapType sMap, GroupType map, T key, T value) {
    return null;
  }

  public T primitive(LogicalType sPrimitive, PrimitiveType primitive) {
    return null;
  }

  protected String[] currentPath() {
    return Lists.newArrayList(fieldNames.descendingIterator()).toArray(new String[0]);
  }

  protected String[] path(String name) {
    List<String> list = Lists.newArrayList(fieldNames.descendingIterator());
    list.add(name);
    return list.toArray(new String[0]);
  }
}
