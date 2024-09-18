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

import java.util.Deque;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class ParquetTypeVisitor<T> {
  private final Deque<String> fieldNames = Lists.newLinkedList();

  public static <T> T visit(Type type, ParquetTypeVisitor<T> visitor) {
    if (type instanceof MessageType) {
      return visitor.message((MessageType) type, visitFields(type.asGroupType(), visitor));

    } else if (type.isPrimitive()) {
      return visitor.primitive(type.asPrimitiveType());

    } else {
      // if not a primitive, the typeId must be a group
      GroupType group = type.asGroupType();
      OriginalType annotation = group.getOriginalType();
      if (annotation != null) {
        switch (annotation) {
          case LIST:
            return visitList(group, visitor);

          case MAP:
            return visitMap(group, visitor);

          default:
        }
      }

      return visitor.struct(group, visitFields(group, visitor));
    }
  }

  private static <T> T visitList(GroupType list, ParquetTypeVisitor<T> visitor) {
    Preconditions.checkArgument(
        list.getFieldCount() == 1,
        "Invalid list: does not contain single repeated field: %s",
        list);

    Type repeatedElement = list.getFields().get(0);
    Preconditions.checkArgument(
        repeatedElement.isRepetition(Type.Repetition.REPEATED),
        "Invalid list: inner group is not repeated");

    Type listElement = ParquetSchemaUtil.determineListElementType(list);
    if (listElement.isRepetition(Type.Repetition.REPEATED)) {
      T elementResult = visitListElement(listElement, visitor);
      return visitor.list(list, elementResult);
    } else {
      return visitThreeLevelList(list, repeatedElement, listElement, visitor);
    }
  }

  private static <T> T visitThreeLevelList(
      GroupType list, Type repeated, Type listElement, ParquetTypeVisitor<T> visitor) {
    visitor.beforeRepeatedElement(repeated);
    try {
      T elementResult = visitListElement(listElement, visitor);
      return visitor.list(list, elementResult);
    } finally {
      visitor.afterRepeatedElement(repeated);
    }
  }

  private static <T> T visitListElement(Type listElement, ParquetTypeVisitor<T> visitor) {
    T elementResult;

    visitor.beforeElementField(listElement);
    try {
      elementResult = visit(listElement, visitor);
    } finally {
      visitor.afterElementField(listElement);
    }

    return elementResult;
  }

  private static <T> T visitMap(GroupType map, ParquetTypeVisitor<T> visitor) {
    Preconditions.checkArgument(
        !map.isRepetition(Type.Repetition.REPEATED),
        "Invalid map: top-level group is repeated: %s",
        map);
    Preconditions.checkArgument(
        map.getFieldCount() == 1, "Invalid map: does not contain single repeated field: %s", map);

    GroupType repeatedKeyValue = map.getType(0).asGroupType();
    Preconditions.checkArgument(
        repeatedKeyValue.isRepetition(Type.Repetition.REPEATED),
        "Invalid map: inner group is not repeated");
    Preconditions.checkArgument(
        repeatedKeyValue.getFieldCount() <= 2,
        "Invalid map: repeated group does not have 2 fields");

    visitor.beforeRepeatedKeyValue(repeatedKeyValue);
    try {
      T keyResult = null;
      T valueResult = null;
      switch (repeatedKeyValue.getFieldCount()) {
        case 2:
          // if there are 2 fields, both key and value are projected
          Type keyType = repeatedKeyValue.getType(0);
          visitor.beforeKeyField(keyType);
          try {
            keyResult = visit(keyType, visitor);
          } finally {
            visitor.afterKeyField(keyType);
          }
          Type valueType = repeatedKeyValue.getType(1);
          visitor.beforeValueField(valueType);
          try {
            valueResult = visit(valueType, visitor);
          } finally {
            visitor.afterValueField(valueType);
          }
          break;

        case 1:
          // if there is just one, use the name to determine what it is
          Type keyOrValue = repeatedKeyValue.getType(0);
          if (keyOrValue.getName().equalsIgnoreCase("key")) {
            visitor.beforeKeyField(keyOrValue);
            try {
              keyResult = visit(keyOrValue, visitor);
            } finally {
              visitor.afterKeyField(keyOrValue);
            }
            // value result remains null
          } else {
            visitor.beforeValueField(keyOrValue);
            try {
              valueResult = visit(keyOrValue, visitor);
            } finally {
              visitor.afterValueField(keyOrValue);
            }
            // key result remains null
          }
          break;

        default:
          // both results will remain null
      }

      return visitor.map(map, keyResult, valueResult);

    } finally {
      visitor.afterRepeatedKeyValue(repeatedKeyValue);
    }
  }

  private static <T> List<T> visitFields(GroupType group, ParquetTypeVisitor<T> visitor) {
    List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
    for (Type field : group.getFields()) {
      visitor.beforeField(field);
      try {
        results.add(visit(field, visitor));
      } finally {
        visitor.afterField(field);
      }
    }

    return results;
  }

  public T message(MessageType message, List<T> fields) {
    return null;
  }

  public T struct(GroupType struct, List<T> fields) {
    return null;
  }

  public T list(GroupType array, T element) {
    return null;
  }

  public T map(GroupType map, T key, T value) {
    return null;
  }

  public T variant(GroupType variant) {
    return null;
  }

  public T primitive(PrimitiveType primitive) {
    return null;
  }

  public void beforeField(Type type) {
    fieldNames.push(type.getName());
  }

  public void afterField(Type type) {
    fieldNames.pop();
  }

  public void beforeRepeatedElement(Type element) {
    beforeField(element);
  }

  public void afterRepeatedElement(Type element) {
    afterField(element);
  }

  public void beforeElementField(Type element) {
    beforeField(element);
  }

  public void afterElementField(Type element) {
    afterField(element);
  }

  public void beforeRepeatedKeyValue(Type keyValue) {
    beforeField(keyValue);
  }

  public void afterRepeatedKeyValue(Type keyValue) {
    afterField(keyValue);
  }

  public void beforeKeyField(Type key) {
    beforeField(key);
  }

  public void afterKeyField(Type key) {
    afterField(key);
  }

  public void beforeValueField(Type value) {
    beforeField(value);
  }

  public void afterValueField(Type value) {
    afterField(value);
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
