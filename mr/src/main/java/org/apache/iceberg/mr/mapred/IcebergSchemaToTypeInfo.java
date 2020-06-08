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

package org.apache.iceberg.mr.mapred;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Class to convert Iceberg types to Hive TypeInfo
 */
final class IcebergSchemaToTypeInfo {

  private IcebergSchemaToTypeInfo() {

  }

  private static final ImmutableMap<Object, Object> primitiveTypeToTypeInfo = ImmutableMap.builder()
      .put(Types.BooleanType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME))
      .put(Types.IntegerType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME))
      .put(Types.LongType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME))
      .put(Types.FloatType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME))
      .put(Types.DoubleType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME))
      .put(Types.BinaryType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME))
      .put(Types.StringType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME))
      .put(Types.DateType.get(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME))
      .put(Types.TimestampType.withoutZone(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME))
      .put(Types.TimestampType.withZone(), TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME))
      .build();

  public static List<TypeInfo> getColumnTypes(Schema schema) throws Exception {
    List<Types.NestedField> fields = schema.columns();
    List<TypeInfo> types = new ArrayList<>(fields.size());
    for (Types.NestedField field : fields) {
      types.add(generateTypeInfo(field.type()));
    }
    return types;
  }

  private static TypeInfo generateTypeInfo(Type type) throws Exception {
    if (primitiveTypeToTypeInfo.containsKey(type)) {
      return (TypeInfo) primitiveTypeToTypeInfo.get(type);
    }
    switch (type.typeId()) {
      case UUID:
        return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
      case FIXED:
        return TypeInfoFactory.getPrimitiveTypeInfo("binary");
      case TIME:
        return TypeInfoFactory.getPrimitiveTypeInfo("long");
      case DECIMAL:
        Types.DecimalType dec = (Types.DecimalType) type;
        int scale = dec.scale();
        int precision = dec.precision();
        try {
          HiveDecimalUtils.validateParameter(precision, scale);
        } catch (Exception e) {
          //TODO Log that precision / scale isn't valid
          throw e;
        }
        return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
      case STRUCT:
        return generateStructTypeInfo((Types.StructType) type);
      case LIST:
        return generateListTypeInfo((Types.ListType) type);
      case MAP:
        return generateMapTypeInfo((Types.MapType) type);
      default:
        throw new SerDeException("Can't map Iceberg type to Hive TypeInfo: '" + type.typeId() + "'");
    }
  }

  private static TypeInfo generateMapTypeInfo(Types.MapType type) throws Exception {
    Type keyType = type.keyType();
    Type valueType = type.valueType();
    return TypeInfoFactory.getMapTypeInfo(generateTypeInfo(keyType), generateTypeInfo(valueType));
  }

  private static TypeInfo generateStructTypeInfo(Types.StructType type) throws Exception {
    List<Types.NestedField> fields = type.fields();
    List<String> fieldNames = new ArrayList<>(fields.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fields.size());

    for (Types.NestedField field : fields) {
      fieldNames.add(field.name());
      typeInfos.add(generateTypeInfo(field.type()));
    }
    return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
  }

  private static TypeInfo generateListTypeInfo(Types.ListType type) throws Exception {
    return TypeInfoFactory.getListTypeInfo(generateTypeInfo(type.elementType()));
  }
}
