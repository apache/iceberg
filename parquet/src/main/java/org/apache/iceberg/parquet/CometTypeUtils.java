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

import java.util.Map;
import org.apache.comet.parquet.ParquetColumnSpec;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class CometTypeUtils {

  private CometTypeUtils() {}

  public static ParquetColumnSpec descriptorToParquetColumnSpec(ColumnDescriptor descriptor) {

    String[] path = descriptor.getPath();
    PrimitiveType primitiveType = descriptor.getPrimitiveType();
    String physicalType = primitiveType.getPrimitiveTypeName().name();

    int typeLength =
        primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
            ? primitiveType.getTypeLength()
            : 0;

    boolean isRepeated = primitiveType.getRepetition() == Type.Repetition.REPEATED;

    // ToDo: extract this into a Util method
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

    return new ParquetColumnSpec(
        1, // ToDo: pass in the correct id
        path,
        physicalType,
        typeLength,
        isRepeated,
        descriptor.getMaxDefinitionLevel(),
        descriptor.getMaxRepetitionLevel(),
        logicalTypeName,
        logicalTypeParams);
  }

  public static ColumnDescriptor buildColumnDescriptor(ParquetColumnSpec columnSpec) {
    PrimitiveType.PrimitiveTypeName primType =
        PrimitiveType.PrimitiveTypeName.valueOf(columnSpec.getPhysicalType());

    Type.Repetition repetition;
    if (columnSpec.getMaxRepetitionLevel() > 0) {
      repetition = Type.Repetition.REPEATED;
    } else if (columnSpec.getMaxDefinitionLevel() > 0) {
      repetition = Type.Repetition.OPTIONAL;
    } else {
      repetition = Type.Repetition.REQUIRED;
    }

    String name = columnSpec.getPath()[columnSpec.getPath().length - 1];
    // Reconstruct the logical type from parameters
    LogicalTypeAnnotation logicalType = null;
    if (columnSpec.getLogicalTypeName() != null) {
      logicalType =
          reconstructLogicalType(
              columnSpec.getLogicalTypeName(), columnSpec.getLogicalTypeParams());
    }

    PrimitiveType primitiveType;
    if (primType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      primitiveType =
          org.apache.parquet.schema.Types.primitive(primType, repetition)
              .length(columnSpec.getTypeLength())
              .as(logicalType)
              .id(columnSpec.getFieldId())
              .named(name);
    } else {
      primitiveType =
          Types.primitive(primType, repetition)
              .as(logicalType)
              .id(columnSpec.getFieldId())
              .named(name);
    }

    return new ColumnDescriptor(
        columnSpec.getPath(),
        primitiveType,
        columnSpec.getMaxRepetitionLevel(),
        columnSpec.getMaxDefinitionLevel());
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
