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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class CometTypeUtils {
  private static final String PARQUET_COLUMN_SPEC_CLASS =
      "org.apache.comet.parquet.ParquetColumnSpec";
  private static volatile Class<?> parquetColumnSpecClass;
  private static volatile Constructor<?> parquetColumnSpecConstructor;
  private static volatile Method getFieldIdMethod;
  private static volatile Method getPathMethod;
  private static volatile Method getPhysicalTypeMethod;
  private static volatile Method getTypeLengthMethod;
  private static volatile Method getMaxDefinitionLevelMethod;
  private static volatile Method getMaxRepetitionLevelMethod;
  private static volatile Method getLogicalTypeNameMethod;
  private static volatile Method getLogicalTypeParamsMethod;

  private CometTypeUtils() {}

  private static void initializeReflection() throws Exception {
    if (parquetColumnSpecClass == null) {
      synchronized (CometTypeUtils.class) {
        if (parquetColumnSpecClass == null) {
          parquetColumnSpecClass = Class.forName(PARQUET_COLUMN_SPEC_CLASS);
          parquetColumnSpecConstructor =
              parquetColumnSpecClass.getConstructor(
                  int.class,
                  String[].class,
                  String.class,
                  int.class,
                  boolean.class,
                  int.class,
                  int.class,
                  String.class,
                  Map.class);

          getFieldIdMethod = parquetColumnSpecClass.getMethod("getFieldId");
          getPathMethod = parquetColumnSpecClass.getMethod("getPath");
          getPhysicalTypeMethod = parquetColumnSpecClass.getMethod("getPhysicalType");
          getTypeLengthMethod = parquetColumnSpecClass.getMethod("getTypeLength");
          getMaxDefinitionLevelMethod = parquetColumnSpecClass.getMethod("getMaxDefinitionLevel");
          getMaxRepetitionLevelMethod = parquetColumnSpecClass.getMethod("getMaxRepetitionLevel");
          getLogicalTypeNameMethod = parquetColumnSpecClass.getMethod("getLogicalTypeName");
          getLogicalTypeParamsMethod = parquetColumnSpecClass.getMethod("getLogicalTypeParams");
        }
      }
    }
  }

  public static Object descriptorToParquetColumnSpec(ColumnDescriptor descriptor) {
    try {
      initializeReflection();

      String[] path = descriptor.getPath();
      PrimitiveType primitiveType = descriptor.getPrimitiveType();
      String physicalType = primitiveType.getPrimitiveTypeName().name();

      int typeLength =
          primitiveType.getPrimitiveTypeName()
                  == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
              ? primitiveType.getTypeLength()
              : 0;

      boolean isRepeated = primitiveType.getRepetition() == Type.Repetition.REPEATED;

      String logicalTypeName = null;
      Map<String, String> logicalTypeParams = Maps.newHashMap();
      LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

      if (logicalType != null) {
        logicalTypeName = logicalType.getClass().getSimpleName();

        // Handle specific logical types
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
          LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
              (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
          logicalTypeParams.put("precision", String.valueOf(decimal.getPrecision()));
          logicalTypeParams.put("scale", String.valueOf(decimal.getScale()));
        } else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
          LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp =
              (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
          logicalTypeParams.put("isAdjustedToUTC", String.valueOf(timestamp.isAdjustedToUTC()));
          logicalTypeParams.put("unit", timestamp.getUnit().name());
        } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
          LogicalTypeAnnotation.TimeLogicalTypeAnnotation time =
              (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalType;
          logicalTypeParams.put("isAdjustedToUTC", String.valueOf(time.isAdjustedToUTC()));
          logicalTypeParams.put("unit", time.getUnit().name());
        } else if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
          LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
              (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
          logicalTypeParams.put("isSigned", String.valueOf(intType.isSigned()));
          logicalTypeParams.put("bitWidth", String.valueOf(intType.getBitWidth()));
        }
      }

      int id = -1;
      Type type = descriptor.getPrimitiveType();
      if (type != null && type.getId() != null) {
        id = type.getId().intValue();
      }

      return parquetColumnSpecConstructor.newInstance(
          id,
          path,
          physicalType,
          typeLength,
          isRepeated,
          descriptor.getMaxDefinitionLevel(),
          descriptor.getMaxRepetitionLevel(),
          logicalTypeName,
          logicalTypeParams);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create ParquetColumnSpec using reflection", e);
    }
  }

  public static ColumnDescriptor buildColumnDescriptor(Object columnSpecObj) {
    try {
      initializeReflection();

      String physicalType = (String) getPhysicalTypeMethod.invoke(columnSpecObj);
      PrimitiveType.PrimitiveTypeName primType =
          PrimitiveType.PrimitiveTypeName.valueOf(physicalType);

      int maxRepetitionLevel = (Integer) getMaxRepetitionLevelMethod.invoke(columnSpecObj);
      int maxDefinitionLevel = (Integer) getMaxDefinitionLevelMethod.invoke(columnSpecObj);

      Type.Repetition repetition;
      if (maxRepetitionLevel > 0) {
        repetition = Type.Repetition.REPEATED;
      } else if (maxDefinitionLevel > 0) {
        repetition = Type.Repetition.OPTIONAL;
      } else {
        repetition = Type.Repetition.REQUIRED;
      }

      String[] path = (String[]) getPathMethod.invoke(columnSpecObj);
      String name = path[path.length - 1];

      String logicalTypeName = (String) getLogicalTypeNameMethod.invoke(columnSpecObj);
      Map<String, String> logicalTypeParams =
          (Map<String, String>) getLogicalTypeParamsMethod.invoke(columnSpecObj);

      // Reconstruct the logical type from parameters
      LogicalTypeAnnotation logicalType = null;
      if (logicalTypeName != null) {
        logicalType = reconstructLogicalType(logicalTypeName, logicalTypeParams);
      }

      int fieldId = (Integer) getFieldIdMethod.invoke(columnSpecObj);
      int typeLength = (Integer) getTypeLengthMethod.invoke(columnSpecObj);

      PrimitiveType primitiveType;
      if (primType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        primitiveType =
            org.apache.parquet.schema.Types.primitive(primType, repetition)
                .length(typeLength)
                .as(logicalType)
                .id(fieldId)
                .named(name);
      } else {
        primitiveType =
            Types.primitive(primType, repetition).as(logicalType).id(fieldId).named(name);
      }

      return new ColumnDescriptor(path, primitiveType, maxRepetitionLevel, maxDefinitionLevel);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build ColumnDescriptor using reflection", e);
    }
  }

  private static LogicalTypeAnnotation reconstructLogicalType(
      String logicalTypeName, java.util.Map<String, String> params) {

    switch (logicalTypeName) {
        // MAP
      case "MapLogicalTypeAnnotation":
        return LogicalTypeAnnotation.mapType();

        // LIST
      case "ListLogicalTypeAnnotation":
        return LogicalTypeAnnotation.listType();

        // STRING
      case "StringLogicalTypeAnnotation":
        return LogicalTypeAnnotation.stringType();

        // MAP_KEY_VALUE
      case "MapKeyValueLogicalTypeAnnotation":
        return LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance();

        // ENUM
      case "EnumLogicalTypeAnnotation":
        return LogicalTypeAnnotation.enumType();

        // DECIMAL
      case "DecimalLogicalTypeAnnotation":
        if (!params.containsKey("scale") || !params.containsKey("precision")) {
          throw new IllegalArgumentException(
              "Missing required parameters for DecimalLogicalTypeAnnotation: " + params);
        }
        int scale = Integer.parseInt(params.get("scale"));
        int precision = Integer.parseInt(params.get("precision"));
        return LogicalTypeAnnotation.decimalType(scale, precision);

        // DATE
      case "DateLogicalTypeAnnotation":
        return LogicalTypeAnnotation.dateType();

        // TIME
      case "TimeLogicalTypeAnnotation":
        if (!params.containsKey("isAdjustedToUTC") || !params.containsKey("unit")) {
          throw new IllegalArgumentException(
              "Missing required parameters for TimeLogicalTypeAnnotation: " + params);
        }

        boolean isUTC = Boolean.parseBoolean(params.get("isAdjustedToUTC"));
        String timeUnitStr = params.get("unit");

        LogicalTypeAnnotation.TimeUnit timeUnit;
        switch (timeUnitStr) {
          case "MILLIS":
            timeUnit = LogicalTypeAnnotation.TimeUnit.MILLIS;
            break;
          case "MICROS":
            timeUnit = LogicalTypeAnnotation.TimeUnit.MICROS;
            break;
          case "NANOS":
            timeUnit = LogicalTypeAnnotation.TimeUnit.NANOS;
            break;
          default:
            throw new IllegalArgumentException("Unknown time unit: " + timeUnitStr);
        }
        return LogicalTypeAnnotation.timeType(isUTC, timeUnit);

        // TIMESTAMP
      case "TimestampLogicalTypeAnnotation":
        if (!params.containsKey("isAdjustedToUTC") || !params.containsKey("unit")) {
          throw new IllegalArgumentException(
              "Missing required parameters for TimestampLogicalTypeAnnotation: " + params);
        }
        boolean isAdjustedToUTC = Boolean.parseBoolean(params.get("isAdjustedToUTC"));
        String unitStr = params.get("unit");

        LogicalTypeAnnotation.TimeUnit unit;
        switch (unitStr) {
          case "MILLIS":
            unit = LogicalTypeAnnotation.TimeUnit.MILLIS;
            break;
          case "MICROS":
            unit = LogicalTypeAnnotation.TimeUnit.MICROS;
            break;
          case "NANOS":
            unit = LogicalTypeAnnotation.TimeUnit.NANOS;
            break;
          default:
            throw new IllegalArgumentException("Unknown timestamp unit: " + unitStr);
        }
        return LogicalTypeAnnotation.timestampType(isAdjustedToUTC, unit);

        // INTEGER
      case "IntLogicalTypeAnnotation":
        if (!params.containsKey("isSigned") || !params.containsKey("bitWidth")) {
          throw new IllegalArgumentException(
              "Missing required parameters for IntLogicalTypeAnnotation: " + params);
        }
        boolean isSigned = Boolean.parseBoolean(params.get("isSigned"));
        int bitWidth = Integer.parseInt(params.get("bitWidth"));
        return LogicalTypeAnnotation.intType(bitWidth, isSigned);

        // JSON
      case "JsonLogicalTypeAnnotation":
        return LogicalTypeAnnotation.jsonType();

        // BSON
      case "BsonLogicalTypeAnnotation":
        return LogicalTypeAnnotation.bsonType();

        // UUID
      case "UUIDLogicalTypeAnnotation":
        return LogicalTypeAnnotation.uuidType();

        // INTERVAL
      case "IntervalLogicalTypeAnnotation":
        return LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance();

      default:
        throw new IllegalArgumentException("Unknown logical type: " + logicalTypeName);
    }
  }
}
