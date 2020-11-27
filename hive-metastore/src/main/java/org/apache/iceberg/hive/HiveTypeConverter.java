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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;


public final class HiveTypeConverter {

  private HiveTypeConverter() {

  }

  /**
   * Converts the Iceberg schema to a Hive schema.
   * @param schema The original Iceberg schema to convert
   * @return The Hive column list generated from the Iceberg schema
   */
  public static List<FieldSchema> hiveSchema(Schema schema) {
    return schema.columns().stream()
        .map(col -> new FieldSchema(col.name(), HiveTypeConverter.convert(col.type()), ""))
        .collect(Collectors.toList());
  }

  /**
   * Converts the list of Hive FieldSchemas to an Iceberg schema.
   * <p>
   * The list should contain the columns and the partition columns as well.
   * @param fieldSchemas The list of the columns
   * @return An equivalent Iceberg Schema
   */
  public static Schema icebergSchema(List<FieldSchema> fieldSchemas) {
    List<String> names = new ArrayList<>(fieldSchemas.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
    }

    return HiveSchemaConverter.convert(names, typeInfos);
  }

  /**
   * Converts the Hive properties defining the columns to an Iceberg schema.
   * @param columnNames The property containing the column names
   * @param columnTypes The property containing the column types
   * @param columnNameDelimiter The name delimiter
   * @return The Iceberg schema
   */
  public static Schema icebergSchema(String columnNames, String columnTypes, String columnNameDelimiter) {
    // Parse the configuration parameters
    List<String> names = new ArrayList<>();
    Collections.addAll(names, columnNames.split(columnNameDelimiter));

    return HiveSchemaConverter.convert(names, TypeInfoUtils.getTypeInfosFromTypeString(columnTypes));
  }

  private static String convert(Type type) {
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
        // TODO may be just decimal?
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
