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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Deque;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.LogicalMap;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public abstract class AvroWithSparkSchemaVisitor<T> {
  public static <T> T visit(StructType struct, Schema schema, AvroWithSparkSchemaVisitor<T> visitor) {
    return visitRecord(struct, schema, visitor);
  }

  public static <T> T visit(DataType type, Schema schema, AvroWithSparkSchemaVisitor<T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        Preconditions.checkArgument(type instanceof StructType, "Invalid struct: %s is not a struct", type);
        return visitRecord((StructType) type, schema, visitor);

      case UNION:
        return visitUnion(type, schema, visitor);

      case ARRAY:
        return visitArray(type, schema, visitor);

      case MAP:
        Preconditions.checkArgument(type instanceof MapType, "Invalid map: %s is not a map", type);
        MapType map = (MapType) type;
        Preconditions.checkArgument(map.keyType() instanceof StringType,
            "Invalid map: %s is not a string", map.keyType());
        return visitor.map(map, schema, visit(map.valueType(), schema.getValueType(), visitor));

      default:
        return visitor.primitive(type, schema);
    }
  }

  private static <T> T visitRecord(StructType struct, Schema record, AvroWithSparkSchemaVisitor<T> visitor) {
    // check to make sure this hasn't been visited before
    String name = record.getFullName();
    Preconditions.checkState(!visitor.recordLevels.contains(name),
        "Cannot process recursive Avro record %s", name);
    StructField[] sFields = struct.fields();
    List<Schema.Field> fields = record.getFields();
    Preconditions.checkArgument(sFields.length == fields.size(),
        "Structs do not match: %s != %s", struct, record);

    visitor.recordLevels.push(name);

    List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (int i = 0; i < sFields.length; i += 1) {
      StructField sField = sFields[i];
      Schema.Field field = fields.get(i);
      Preconditions.checkArgument(AvroSchemaUtil.makeCompatibleName(sField.name()).equals(field.name()),
          "Structs do not match: field %s != %s", sField.name(), field.name());
      results.add(visit(sField.dataType(), field.schema(), visitor));
    }

    visitor.recordLevels.pop();

    return visitor.record(struct, record, names, results);
  }

  private static <T> T visitUnion(DataType type, Schema union, AvroWithSparkSchemaVisitor<T> visitor) {
    List<Schema> types = union.getTypes();
    Preconditions.checkArgument(AvroSchemaUtil.isOptionSchema(union),
        "Cannot visit non-option union: %s", union);
    List<T> options = Lists.newArrayListWithExpectedSize(types.size());
    for (Schema branch : types) {
      if (branch.getType() == Schema.Type.NULL) {
        options.add(visit(DataTypes.NullType, branch, visitor));
      } else {
        options.add(visit(type, branch, visitor));
      }
    }
    return visitor.union(type, union, options);
  }

  private static <T> T visitArray(DataType type, Schema array, AvroWithSparkSchemaVisitor<T> visitor) {
    if (array.getLogicalType() instanceof LogicalMap || type instanceof MapType) {
      Preconditions.checkState(
          AvroSchemaUtil.isKeyValueSchema(array.getElementType()),
          "Cannot visit invalid logical map type: %s", array);
      Preconditions.checkArgument(type instanceof MapType, "Invalid map: %s is not a map", type);
      MapType map = (MapType) type;
      List<Schema.Field> keyValueFields = array.getElementType().getFields();
      return visitor.map(map, array,
          visit(map.keyType(), keyValueFields.get(0).schema(), visitor),
          visit(map.valueType(), keyValueFields.get(1).schema(), visitor));

    } else {
      Preconditions.checkArgument(type instanceof ArrayType, "Invalid array: %s is not an array", type);
      ArrayType list = (ArrayType) type;
      return visitor.array(list, array, visit(list.elementType(), array.getElementType(), visitor));
    }
  }

  private Deque<String> recordLevels = Lists.newLinkedList();

  public T record(StructType struct, Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(DataType type, Schema union, List<T> options) {
    return null;
  }

  public T array(ArrayType sArray, Schema array, T element) {
    return null;
  }

  public T map(MapType sMap, Schema map, T key, T value) {
    return null;
  }

  public T map(MapType sMap, Schema map, T value) {
    return null;
  }

  public T primitive(DataType type, Schema primitive) {
    return null;
  }
}
