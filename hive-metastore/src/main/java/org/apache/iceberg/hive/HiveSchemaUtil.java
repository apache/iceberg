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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public final class HiveSchemaUtil {

  private HiveSchemaUtil() {}

  /**
   * Converts the Iceberg schema to a Hive schema (list of FieldSchema objects).
   *
   * @param schema The original Iceberg schema to convert
   * @return The Hive column list generated from the Iceberg schema
   */
  public static List<FieldSchema> convert(Schema schema) {
    return schema.columns().stream()
        .map(col -> new FieldSchema(col.name(), convertToTypeString(col.type()), col.doc()))
        .collect(Collectors.toList());
  }

  /**
   * Converts a Hive schema (list of FieldSchema objects) to an Iceberg schema. If some of the types
   * are not convertible then exception is thrown.
   *
   * @param fieldSchemas The list of the columns
   * @return An equivalent Iceberg Schema
   */
  public static Schema convert(List<FieldSchema> fieldSchemas) {
    return convert(fieldSchemas, false);
  }

  /**
   * Converts a Hive schema (list of FieldSchema objects) to an Iceberg schema.
   *
   * @param fieldSchemas The list of the columns
   * @param autoConvert If <code>true</code> then TINYINT and SMALLINT is converted to INTEGER and
   *     VARCHAR and CHAR is converted to STRING. Otherwise if these types are used in the Hive
   *     schema then exception is thrown.
   * @return An equivalent Iceberg Schema
   */
  public static Schema convert(List<FieldSchema> fieldSchemas, boolean autoConvert) {
    List<String> names = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    List<TypeInfo> typeInfos = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    List<String> comments = Lists.newArrayListWithExpectedSize(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
      comments.add(col.getComment());
    }
    return HiveSchemaConverter.convert(names, typeInfos, comments, autoConvert);
  }

  /**
   * Converts the Hive partition columns to Iceberg identity partition specification.
   *
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
   * Converts the Hive list of column names and column types to an Iceberg schema. If some of the
   * types are not convertible then exception is thrown.
   *
   * @param names The list of the Hive column names
   * @param types The list of the Hive column types
   * @param comments The list of the Hive column comments
   * @return The Iceberg schema
   */
  public static Schema convert(List<String> names, List<TypeInfo> types, List<String> comments) {
    return HiveSchemaConverter.convert(names, types, comments, false);
  }

  /**
   * Converts the Hive list of column names and column types to an Iceberg schema.
   *
   * @param names The list of the Hive column names
   * @param types The list of the Hive column types
   * @param comments The list of the Hive column comments, can be null
   * @param autoConvert If <code>true</code> then TINYINT and SMALLINT is converted to INTEGER and
   *     VARCHAR and CHAR is converted to STRING. Otherwise if these types are used in the Hive
   *     schema then exception is thrown.
   * @return The Iceberg schema
   */
  public static Schema convert(
      List<String> names, List<TypeInfo> types, List<String> comments, boolean autoConvert) {
    return HiveSchemaConverter.convert(names, types, comments, autoConvert);
  }

  /**
   * Converts an Iceberg type to a Hive TypeInfo object.
   *
   * @param type The Iceberg type
   * @return The Hive type
   */
  public static TypeInfo convert(Type type) {
    return TypeInfoUtils.getTypeInfoFromTypeString(convertToTypeString(type));
  }

  /**
   * Converts a Hive typeInfo object to an Iceberg type.
   *
   * @param typeInfo The Hive type
   * @return The Iceberg type
   */
  public static Type convert(TypeInfo typeInfo) {
    return HiveSchemaConverter.convert(typeInfo, false);
  }

  private static String convertToTypeString(Type type) {
    StringBuilder sb = new StringBuilder();
    appendTypeString(sb, type);
    return sb.toString();
  }

  private static void appendTypeString(StringBuilder sb, Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        sb.append("boolean");
        return;
      case INTEGER:
        sb.append("int");
        return;
      case LONG:
        sb.append("bigint");
        return;
      case FLOAT:
        sb.append("float");
        return;
      case DOUBLE:
        sb.append("double");
        return;
      case DATE:
        sb.append("date");
        return;
      case TIME:
      case STRING:
      case UUID:
        sb.append("string");
        return;
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        if (HiveVersion.min(HiveVersion.HIVE_3) && timestampType.shouldAdjustToUTC()) {
          sb.append("timestamp with local time zone");
        } else {
          sb.append("timestamp");
        }
        return;
      case FIXED:
      case BINARY:
        sb.append("binary");
        return;
      case VARIANT:
        sb.append("unknown");
        return;
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        sb.append("decimal(")
            .append(decimalType.precision())
            .append(',')
            .append(decimalType.scale())
            .append(')');
        return;
      case STRUCT:
        // Recurse via appendTypeString rather than convert(Type): the latter would parse each
        // nested type string into a Hive TypeInfo only to stringify it straight back, allocating a
        // throwaway TypeInfo tree at every nesting level.
        List<Types.NestedField> fields = type.asStructType().fields();
        sb.append("struct<");
        for (int i = 0; i < fields.size(); i++) {
          if (i > 0) {
            sb.append(',');
          }
          Types.NestedField field = fields.get(i);
          sb.append(field.name()).append(':');
          appendTypeString(sb, field.type());
        }
        sb.append('>');
        return;
      case LIST:
        sb.append("array<");
        appendTypeString(sb, type.asListType().elementType());
        sb.append('>');
        return;
      case MAP:
        Types.MapType mapType = type.asMapType();
        sb.append("map<");
        appendTypeString(sb, mapType.keyType());
        sb.append(',');
        appendTypeString(sb, mapType.valueType());
        sb.append('>');
        return;
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }
}
