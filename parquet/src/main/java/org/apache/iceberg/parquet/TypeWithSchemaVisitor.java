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
package org.apache.iceberg.parquet;

import java.util.ArrayDeque;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Visitor for traversing a Parquet type with a companion Iceberg type.
 *
 * @param <T> the Java class returned by the visitor
 */
public class TypeWithSchemaVisitor<T> {
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected ArrayDeque<String> fieldNames = new ArrayDeque<>();

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static <T> T visit(
      org.apache.iceberg.types.Type iType, Type type, TypeWithSchemaVisitor<T> visitor) {
    if (type instanceof MessageType) {
      Types.StructType struct = iType != null ? iType.asStructType() : null;
      return visitor.message(
          struct, (MessageType) type, visitFields(struct, type.asGroupType(), visitor));

    } else if (type.isPrimitive()) {
      org.apache.iceberg.types.Type.PrimitiveType iPrimitive =
          iType != null ? iType.asPrimitiveType() : null;
      return visitor.primitive(iPrimitive, type.asPrimitiveType());

    } else {
      // if not a primitive, the typeId must be a group
      GroupType group = type.asGroupType();
      OriginalType annotation = group.getOriginalType();
      if (annotation != null) {
        switch (annotation) {
          case LIST:
            Preconditions.checkArgument(
                group.getFieldCount() == 1,
                "Invalid list: does not contain single repeated field: %s",
                group);

            Type repeatedElement = group.getFields().get(0);
            Preconditions.checkArgument(
                repeatedElement.isRepetition(Type.Repetition.REPEATED),
                "Invalid list: inner group is not repeated");

            Type listElement = ParquetSchemaUtil.determineListElementType(group);
            Types.ListType list = null;
            Types.NestedField element = null;
            if (iType != null) {
              list = iType.asListType();
              element = list.fields().get(0);
            }

            if (listElement.isRepetition(Type.Repetition.REPEATED)) {
              return visitTwoLevelList(list, element, group, listElement, visitor);
            } else {
              return visitThreeLevelList(list, element, group, listElement, visitor);
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

            Types.MapType map = null;
            Types.NestedField keyField = null;
            Types.NestedField valueField = null;
            if (iType != null) {
              map = iType.asMapType();
              keyField = map.fields().get(0);
              valueField = map.fields().get(1);
            }

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

      Types.StructType struct = iType != null ? iType.asStructType() : null;
      return visitor.struct(struct, group, visitFields(struct, group, visitor));
    }
  }

  private static <T> T visitTwoLevelList(
      Types.ListType iListType,
      Types.NestedField iListElement,
      GroupType pListType,
      Type pListElement,
      TypeWithSchemaVisitor<T> visitor) {
    T elementResult = visitField(iListElement, pListElement, visitor);
    return visitor.list(iListType, pListType, elementResult);
  }

  private static <T> T visitThreeLevelList(
      Types.ListType iListType,
      Types.NestedField iListElement,
      GroupType pListType,
      Type pListElement,
      TypeWithSchemaVisitor<T> visitor) {
    visitor.fieldNames.push(pListType.getFieldName(0));

    try {
      T elementResult = visitField(iListElement, pListElement, visitor);

      return visitor.list(iListType, pListType, elementResult);
    } finally {
      visitor.fieldNames.pop();
    }
  }

  private static <T> T visitField(
      Types.NestedField iField, Type field, TypeWithSchemaVisitor<T> visitor) {
    visitor.fieldNames.push(field.getName());
    try {
      return visit(iField != null ? iField.type() : null, field, visitor);
    } finally {
      visitor.fieldNames.pop();
    }
  }

  private static <T> List<T> visitFields(
      Types.StructType struct, GroupType group, TypeWithSchemaVisitor<T> visitor) {
    List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
    for (Type field : group.getFields()) {
      int id = -1;
      if (field.getId() != null) {
        id = field.getId().intValue();
      }
      Types.NestedField iField = (struct != null && id >= 0) ? struct.field(id) : null;
      results.add(visitField(iField, field, visitor));
    }

    return results;
  }

  public T message(Types.StructType iStruct, MessageType message, List<T> fields) {
    return null;
  }

  public T struct(Types.StructType iStruct, GroupType struct, List<T> fields) {
    return null;
  }

  public T list(Types.ListType iList, GroupType array, T element) {
    return null;
  }

  public T map(Types.MapType iMap, GroupType map, T key, T value) {
    return null;
  }

  public T primitive(
      org.apache.iceberg.types.Type.PrimitiveType iPrimitive, PrimitiveType primitive) {
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
