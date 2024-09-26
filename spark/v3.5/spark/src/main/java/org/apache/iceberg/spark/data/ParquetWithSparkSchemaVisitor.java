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
package org.apache.iceberg.spark.data;

import java.util.Deque;
import java.util.List;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;

/**
 * Visitor for traversing a Parquet type with a companion Spark type.
 *
 * @param <T> the Java class returned by the visitor
 */
public class ParquetWithSparkSchemaVisitor<T> {
  private final Deque<String> fieldNames = Lists.newLinkedList();

  public static <T> T visit(DataType sType, Type type, ParquetWithSparkSchemaVisitor<T> visitor) {
    Preconditions.checkArgument(sType != null, "Invalid DataType: null");
    if (type instanceof MessageType) {
      Preconditions.checkArgument(
          sType instanceof StructType, "Invalid struct: %s is not a struct", sType);
      StructType struct = (StructType) sType;
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
                !group.isRepetition(Repetition.REPEATED),
                "Invalid list: top-level group is repeated: %s",
                group);
            Preconditions.checkArgument(
                group.getFieldCount() == 1,
                "Invalid list: does not contain single repeated field: %s",
                group);

            GroupType repeatedElement = group.getFields().get(0).asGroupType();
            Preconditions.checkArgument(
                repeatedElement.isRepetition(Repetition.REPEATED),
                "Invalid list: inner group is not repeated");
            Preconditions.checkArgument(
                repeatedElement.getFieldCount() <= 1,
                "Invalid list: repeated group is not a single field: %s",
                group);

            Preconditions.checkArgument(
                sType instanceof ArrayType, "Invalid list: %s is not an array", sType);
            ArrayType array = (ArrayType) sType;
            StructField element =
                new StructField(
                    "element", array.elementType(), array.containsNull(), Metadata.empty());

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
                !group.isRepetition(Repetition.REPEATED),
                "Invalid map: top-level group is repeated: %s",
                group);
            Preconditions.checkArgument(
                group.getFieldCount() == 1,
                "Invalid map: does not contain single repeated field: %s",
                group);

            GroupType repeatedKeyValue = group.getType(0).asGroupType();
            Preconditions.checkArgument(
                repeatedKeyValue.isRepetition(Repetition.REPEATED),
                "Invalid map: inner group is not repeated");
            Preconditions.checkArgument(
                repeatedKeyValue.getFieldCount() <= 2,
                "Invalid map: repeated group does not have 2 fields");

            Preconditions.checkArgument(
                sType instanceof MapType, "Invalid map: %s is not a map", sType);
            MapType map = (MapType) sType;
            StructField keyField = new StructField("key", map.keyType(), false, Metadata.empty());
            StructField valueField =
                new StructField(
                    "value", map.valueType(), map.valueContainsNull(), Metadata.empty());

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

      if (sType instanceof VariantType) {
        return visitor.variant(group);
      }

      Preconditions.checkArgument(
          sType instanceof StructType, "Invalid struct: %s is not a struct", sType);
      StructType struct = (StructType) sType;
      return visitor.struct(struct, group, visitFields(struct, group, visitor));
    }
  }

  private static <T> T visitField(
      StructField sField, Type field, ParquetWithSparkSchemaVisitor<T> visitor) {
    visitor.fieldNames.push(field.getName());
    try {
      return visit(sField.dataType(), field, visitor);
    } finally {
      visitor.fieldNames.pop();
    }
  }

  private static <T> List<T> visitFields(
      StructType struct, GroupType group, ParquetWithSparkSchemaVisitor<T> visitor) {
    StructField[] sFields = struct.fields();
    Preconditions.checkArgument(
        sFields.length == group.getFieldCount(), "Structs do not match: %s and %s", struct, group);
    List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
    for (int i = 0; i < sFields.length; i += 1) {
      Type field = group.getFields().get(i);
      StructField sField = sFields[i];
      Preconditions.checkArgument(
          field.getName().equals(AvroSchemaUtil.makeCompatibleName(sField.name())),
          "Structs do not match: field %s != %s",
          field.getName(),
          sField.name());
      results.add(visitField(sField, field, visitor));
    }

    return results;
  }

  public T message(StructType sStruct, MessageType message, List<T> fields) {
    return null;
  }

  public T struct(StructType sStruct, GroupType struct, List<T> fields) {
    return null;
  }

  public T variant(GroupType variant) {
    return null;
  }

  public T list(ArrayType sArray, GroupType array, T element) {
    return null;
  }

  public T map(MapType sMap, GroupType map, T key, T value) {
    return null;
  }

  public T primitive(DataType sPrimitive, PrimitiveType primitive) {
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
