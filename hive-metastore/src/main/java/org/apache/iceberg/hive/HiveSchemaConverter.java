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

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Package private class for converting Hive schema to Iceberg schema. Should be used only by the
 * HiveSchemaUtil. Use {@link HiveSchemaUtil} for conversion purposes.
 */
class HiveSchemaConverter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaConverter.class);

  private int id;
  private final boolean autoConvert;

  private HiveSchemaConverter(boolean autoConvert) {
    this.autoConvert = autoConvert;
    this.id = 0;
  }

  static Schema convert(
      List<String> names, List<TypeInfo> typeInfos, List<String> comments, boolean autoConvert) {
    HiveSchemaConverter converter = new HiveSchemaConverter(autoConvert);
    return new Schema(converter.convertInternal(names, typeInfos, comments));
  }

  static Type convert(TypeInfo typeInfo, boolean autoConvert) {
    HiveSchemaConverter converter = new HiveSchemaConverter(autoConvert);
    return converter.convertType(typeInfo);
  }

  List<Types.NestedField> convertInternal(
      List<String> names, List<TypeInfo> typeInfos, List<String> comments) {
    List<Types.NestedField> result = Lists.newArrayListWithExpectedSize(names.size());
    for (int i = 0; i < names.size(); ++i) {
      result.add(
          Types.NestedField.optional(
              id++,
              names.get(i),
              convertType(typeInfos.get(i)),
              (comments.isEmpty() || i >= comments.size()) ? null : comments.get(i)));
    }

    return result;
  }

  Type convertType(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
          case FLOAT:
            return Types.FloatType.get();
          case DOUBLE:
            return Types.DoubleType.get();
          case BOOLEAN:
            return Types.BooleanType.get();
          case BYTE:
          case SHORT:
            Preconditions.checkArgument(
                autoConvert,
                "Unsupported Hive type: %s, use integer instead",
                ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());

            LOG.debug("Using auto conversion from SHORT/BYTE to INTEGER");
            return Types.IntegerType.get();
          case INT:
            return Types.IntegerType.get();
          case LONG:
            return Types.LongType.get();
          case BINARY:
            return Types.BinaryType.get();
          case CHAR:
          case VARCHAR:
            Preconditions.checkArgument(
                autoConvert,
                "Unsupported Hive type: %s, use string instead",
                ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());

            LOG.debug("Using auto conversion from CHAR/VARCHAR to STRING");
            return Types.StringType.get();
          case STRING:
            return Types.StringType.get();
          case TIMESTAMP:
            return Types.TimestampType.withoutZone();
          case DATE:
            return Types.DateType.get();
          case DECIMAL:
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            return Types.DecimalType.of(decimalTypeInfo.precision(), decimalTypeInfo.scale());
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_DAY_TIME:
          default:
            // special case for Timestamp with Local TZ which is only available in Hive3
            if ("TIMESTAMPLOCALTZ"
                .equalsIgnoreCase(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory().name())) {
              return Types.TimestampType.withZone();
            }
            throw new IllegalArgumentException(
                "Unsupported Hive type ("
                    + ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()
                    + ") for Iceberg tables.");
        }
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<Types.NestedField> fields =
            convertInternal(
                structTypeInfo.getAllStructFieldNames(),
                structTypeInfo.getAllStructFieldTypeInfos(),
                Collections.emptyList());
        return Types.StructType.of(fields);
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        Type keyType = convertType(mapTypeInfo.getMapKeyTypeInfo());
        Type valueType = convertType(mapTypeInfo.getMapValueTypeInfo());
        int keyId = id++;
        int valueId = id++;
        return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
      case LIST:
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        Type listType = convertType(listTypeInfo.getListElementTypeInfo());
        return Types.ListType.ofOptional(id++, listType);
      case UNION:
      default:
        throw new IllegalArgumentException("Unknown type " + typeInfo.getCategory());
    }
  }
}
