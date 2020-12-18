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

package org.apache.iceberg.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;


public final class HiveSchemaUtil {

  private HiveSchemaUtil() {
  }

  /**
   * Converts the Iceberg schema to a Hive schema (list of FieldSchema objects).
   * @param schema The original Iceberg schema to convert
   * @return The Hive column list generated from the Iceberg schema
   */
  public static List<FieldSchema> convert(Schema schema) {
    return schema.columns().stream()
        .map(col -> new FieldSchema(col.name(), convertToTypeString(col.type()), ""))
        .collect(Collectors.toList());
  }

  /**
   * Converts a Hive schema (list of FieldSchema objects) to an Iceberg schema.
   * @param fieldSchemas The list of the columns
   * @return An equivalent Iceberg Schema
   */
  public static Schema convert(List<FieldSchema> fieldSchemas) {
    List<String> names = new ArrayList<>(fieldSchemas.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
    }

    return HiveSchemaConverter.convert(names, typeInfos);
  }

  /**
   * Converts the Hive partition columns to Iceberg identity partition specification.
   * @param schema The Iceberg schema
   * @param fieldSchemas The partition column specification
   * @return The Iceberg partition specification
   */
  public static PartitionSpec spec(Schema schema, List<FieldSchema> fieldSchemas) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    fieldSchemas.forEach(fieldSchema -> builder.identity(fieldSchema.getName()));
    return builder.build();
  }

  /**
   * Converts the Hive list of column names and column types to an Iceberg schema.
   * @param names The list of the Hive column names
   * @param types The list of the Hive column types
   * @return The Iceberg schema
   */
  public static Schema convert(List<String> names, List<TypeInfo> types) {
    return HiveSchemaConverter.convert(names, types);
  }

  /**
   * Converts an Iceberg type to a Hive TypeInfo object.
   * @param type The Iceberg type
   * @return The Hive type
   */
  public static TypeInfo convert(Type type) {
    return TypeInfoUtils.getTypeInfoFromTypeString(convertToTypeString(type));
  }

  /**
   * Converts a Hive typeInfo object to an Iceberg type.
   * @param typeInfo The Hive type
   * @return The Iceberg type
   */
  public static Type convert(TypeInfo typeInfo) {
    return HiveSchemaConverter.convert(typeInfo);
  }

  private static String convertToTypeString(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
        return "string";
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        if (MetastoreUtil.hive3PresentOnClasspath() && timestampType.shouldAdjustToUTC()) {
          return "timestamp with local time zone";
        }
        return "timestamp";
      case STRING:
      case UUID:
        return "string";
      case FIXED:
        return "binary";
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream()
            .map(f -> String.format("%s:%s", f.name(), convert(f.type())))
            .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", convert(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return String.format("map<%s,%s>", convert(mapType.keyType()), convert(mapType.valueType()));
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }
}
