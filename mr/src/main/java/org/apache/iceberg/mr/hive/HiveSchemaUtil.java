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

package org.apache.iceberg.mr.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public class HiveSchemaUtil {
  private HiveSchemaUtil() {
  }

  /**
   * Converts the list of Hive FieldSchemas to an Iceberg schema.
   * <p>
   * The list should contain the columns and the partition columns as well.
   * @param fieldSchemas The list of the columns
   * @return An equivalent Iceberg Schema
   */
  public static Schema schema(List<FieldSchema> fieldSchemas) {
    List<String> names = new ArrayList<>(fieldSchemas.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
    }

    return convert(names, typeInfos);
  }

  /**
   * Converts the Hive properties defining the columns to an Iceberg schema.
   * @param columnNames The property containing the column names
   * @param columnTypes The property containing the column types
   * @param columnNameDelimiter The name delimiter
   * @return The Iceberg schema
   */
  public static Schema schema(String columnNames, String columnTypes, String columnNameDelimiter) {
    // Parse the configuration parameters
    List<String> names = new ArrayList<>();
    Collections.addAll(names, columnNames.split(columnNameDelimiter));

    return HiveSchemaUtil.convert(names, TypeInfoUtils.getTypeInfosFromTypeString(columnTypes));
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

  private static Schema convert(List<String> names, List<TypeInfo> typeInfos) {
    HiveSchemaVisitor visitor = new HiveSchemaVisitor();
    return new Schema(visitor.visit(names, typeInfos));
  }

  private static class HiveSchemaVisitor {
    private int id;

    private HiveSchemaVisitor() {
      id = 0;
    }

    private List<Types.NestedField> visit(List<String> names, List<TypeInfo> typeInfos) {
      List<Types.NestedField> result = new ArrayList<>(names.size());
      for (int i = 0; i < names.size(); ++i) {
        result.add(visit(names.get(i), typeInfos.get(i)));
      }

      return result;
    }

    private Types.NestedField visit(String name, TypeInfo typeInfo) {
      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case FLOAT:
              return Types.NestedField.optional(id++, name, Types.FloatType.get());
            case DOUBLE:
              return Types.NestedField.optional(id++, name, Types.DoubleType.get());
            case BOOLEAN:
              return Types.NestedField.optional(id++, name, Types.BooleanType.get());
            case BYTE:
            case SHORT:
            case INT:
              return Types.NestedField.optional(id++, name, Types.IntegerType.get());
            case LONG:
              return Types.NestedField.optional(id++, name, Types.LongType.get());
            case BINARY:
              return Types.NestedField.optional(id++, name, Types.BinaryType.get());
            case STRING:
            case VARCHAR:
              return Types.NestedField.optional(id++, name, Types.StringType.get());
            case CHAR:
              Types.FixedType fixedType = Types.FixedType.ofLength(((CharTypeInfo) typeInfo).getLength());
              return Types.NestedField.optional(id++, name, fixedType);
            case TIMESTAMP:
              return Types.NestedField.optional(id++, name, Types.TimestampType.withZone());
            case DATE:
              return Types.NestedField.optional(id++, name, Types.DateType.get());
            case DECIMAL:
              DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
              Types.DecimalType decimalType =
                  Types.DecimalType.of(decimalTypeInfo.precision(), decimalTypeInfo.scale());
              return Types.NestedField.optional(id++, name, decimalType);
            // TODO: In Hive3 we have TIMESTAMPLOCALTZ
            default:
              throw new IllegalArgumentException("Unknown primitive type " +
                  ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
          }
        case STRUCT:
          StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
          List<Types.NestedField> fields =
              visit(structTypeInfo.getAllStructFieldNames(), structTypeInfo.getAllStructFieldTypeInfos());
          Types.StructType structType = Types.StructType.of(fields);
          return Types.NestedField.optional(id++, name, structType);
        case MAP:
          MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
          Types.NestedField keyField = visit(name + "_key", mapTypeInfo.getMapKeyTypeInfo());
          Types.NestedField valueField = visit(name + "_value", mapTypeInfo.getMapValueTypeInfo());
          Types.MapType mapType =
              Types.MapType.ofOptional(keyField.fieldId(), valueField.fieldId(), keyField.type(), valueField.type());
          return Types.NestedField.optional(id++, name, mapType);
        case LIST:
          ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
          Types.NestedField listField = visit(name + "_element", listTypeInfo.getListElementTypeInfo());
          Types.ListType listType = Types.ListType.ofOptional(listField.fieldId(), listField.type());
          return Types.NestedField.optional(id++, name, listType);
        case UNION:
        default:
          throw new IllegalArgumentException("Unknown type " + typeInfo.getCategory());
      }
    }
  }
}
