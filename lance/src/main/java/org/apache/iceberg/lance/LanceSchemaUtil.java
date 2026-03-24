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
package org.apache.iceberg.lance;

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Utilities for converting between Iceberg schemas and Arrow schemas used by Lance.
 *
 * <p>Lance is natively Arrow-based, so the "file schema" type for Lance is Arrow's {@link Schema}.
 * This util handles bidirectional conversion, storing Iceberg field IDs in Arrow field metadata so
 * they survive round-trips.
 */
public class LanceSchemaUtil {
  static final String ICEBERG_FIELD_ID = "PARQUET:field_id";
  static final String ICEBERG_SCHEMA_KEY = "iceberg.schema";

  private LanceSchemaUtil() {}

  /** Convert an Iceberg schema to an Arrow schema, preserving field IDs in metadata. */
  public static Schema icebergToArrow(org.apache.iceberg.Schema icebergSchema) {
    List<Field> fields = Lists.newArrayList();
    for (Types.NestedField column : icebergSchema.columns()) {
      fields.add(convertField(column));
    }
    return new Schema(fields);
  }

  /** Convert an Arrow schema back to an Iceberg schema, recovering field IDs from metadata. */
  public static org.apache.iceberg.Schema arrowToIceberg(Schema arrowSchema) {
    List<Types.NestedField> columns = Lists.newArrayList();
    for (Field field : arrowSchema.getFields()) {
      columns.add(convertToIcebergField(field));
    }
    return new org.apache.iceberg.Schema(columns);
  }

  /** Extract projected column names from an Iceberg schema. */
  public static List<String> columnNames(org.apache.iceberg.Schema schema) {
    List<String> names = Lists.newArrayList();
    for (Types.NestedField field : schema.columns()) {
      names.add(field.name());
    }
    return names;
  }

  private static Field convertField(Types.NestedField icebergField) {
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(ICEBERG_FIELD_ID, String.valueOf(icebergField.fieldId()));

    ArrowType arrowType = toArrowType(icebergField.type());
    List<Field> children = Lists.newArrayList();

    if (icebergField.type().isStructType()) {
      for (Types.NestedField child : icebergField.type().asStructType().fields()) {
        children.add(convertField(child));
      }
    } else if (icebergField.type().isListType()) {
      Types.ListType listType = icebergField.type().asListType();
      Types.NestedField elementField = listType.fields().get(0);
      children.add(convertField(elementField));
    } else if (icebergField.type().isMapType()) {
      Types.MapType mapType = icebergField.type().asMapType();
      Field keyField = convertField(mapType.fields().get(0));
      Field valueField = convertField(mapType.fields().get(1));
      List<Field> entryChildren = Lists.newArrayList();
      entryChildren.add(keyField);
      entryChildren.add(valueField);
      Field entriesField =
          new Field(
              "entries", new FieldType(false, ArrowType.Struct.INSTANCE, null), entryChildren);
      children.add(entriesField);
    }

    return new Field(
        icebergField.name(),
        new FieldType(icebergField.isOptional(), arrowType, null, metadata),
        children);
  }

  private static ArrowType toArrowType(Type type) {
    if (type.isStructType()) {
      return ArrowType.Struct.INSTANCE;
    } else if (type.isListType()) {
      return ArrowType.List.INSTANCE;
    } else if (type.isMapType()) {
      return new ArrowType.Map(false);
    }

    Type.PrimitiveType primitive = type.asPrimitiveType();
    switch (primitive.typeId()) {
      case BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case INTEGER:
        return new ArrowType.Int(32, true);
      case LONG:
        return new ArrowType.Int(64, true);
      case FLOAT:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case TIME:
        return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) primitive;
        return new ArrowType.Timestamp(
            TimeUnit.MICROSECOND, tsType.shouldAdjustToUTC() ? "UTC" : null);
      case TIMESTAMP_NANO:
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) primitive;
        return new ArrowType.Timestamp(
            TimeUnit.NANOSECOND, tsNanoType.shouldAdjustToUTC() ? "UTC" : null);
      case STRING:
        return ArrowType.Utf8.INSTANCE;
      case UUID:
        return new ArrowType.FixedSizeBinary(16);
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) primitive;
        return new ArrowType.FixedSizeBinary(fixedType.length());
      case BINARY:
        return ArrowType.Binary.INSTANCE;
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) primitive;
        return new ArrowType.Decimal(decimalType.precision(), decimalType.scale(), 128);
      default:
        throw new UnsupportedOperationException("Unsupported Iceberg type: " + primitive);
    }
  }

  private static Types.NestedField convertToIcebergField(Field arrowField) {
    int fieldId = extractFieldId(arrowField);
    boolean isOptional = arrowField.isNullable();
    String name = arrowField.getName();
    Type icebergType = toIcebergType(arrowField);

    if (isOptional) {
      return Types.NestedField.optional(fieldId, name, icebergType);
    } else {
      return Types.NestedField.required(fieldId, name, icebergType);
    }
  }

  private static int extractFieldId(Field field) {
    Map<String, String> metadata = field.getMetadata();
    if (metadata != null && metadata.containsKey(ICEBERG_FIELD_ID)) {
      return Integer.parseInt(metadata.get(ICEBERG_FIELD_ID));
    }
    // Fallback: assign a negative field ID (will need proper resolution via name mapping)
    return -1;
  }

  private static Type toIcebergType(Field arrowField) {
    ArrowType arrowType = arrowField.getType();

    if (arrowType instanceof ArrowType.Struct) {
      List<Types.NestedField> fields = Lists.newArrayList();
      for (Field child : arrowField.getChildren()) {
        fields.add(convertToIcebergField(child));
      }
      return Types.StructType.of(fields);
    }

    if (arrowType instanceof ArrowType.List) {
      Preconditions.checkArgument(
          arrowField.getChildren().size() == 1, "List type must have exactly one child");
      Field elementField = arrowField.getChildren().get(0);
      Types.NestedField element = convertToIcebergField(elementField);
      if (element.isOptional()) {
        return Types.ListType.ofOptional(element.fieldId(), element.type());
      } else {
        return Types.ListType.ofRequired(element.fieldId(), element.type());
      }
    }

    if (arrowType instanceof ArrowType.Map) {
      Preconditions.checkArgument(
          arrowField.getChildren().size() == 1, "Map type must have entries child");
      Field entriesField = arrowField.getChildren().get(0);
      Preconditions.checkArgument(
          entriesField.getChildren().size() == 2, "Map entries must have key and value");
      Types.NestedField keyField = convertToIcebergField(entriesField.getChildren().get(0));
      Types.NestedField valueField = convertToIcebergField(entriesField.getChildren().get(1));
      if (valueField.isOptional()) {
        return Types.MapType.ofOptional(
            keyField.fieldId(), valueField.fieldId(), keyField.type(), valueField.type());
      } else {
        return Types.MapType.ofRequired(
            keyField.fieldId(), valueField.fieldId(), keyField.type(), valueField.type());
      }
    }

    return toIcebergPrimitive(arrowType);
  }

  private static Type toIcebergPrimitive(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Bool) {
      return Types.BooleanType.get();
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      return intType.getBitWidth() <= 32 ? Types.IntegerType.get() : Types.LongType.get();
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
      return fpType.getPrecision() == FloatingPointPrecision.SINGLE
          ? Types.FloatType.get()
          : Types.DoubleType.get();
    } else if (arrowType instanceof ArrowType.Decimal) {
      ArrowType.Decimal decType = (ArrowType.Decimal) arrowType;
      return Types.DecimalType.of(decType.getPrecision(), decType.getScale());
    } else if (arrowType instanceof ArrowType.Utf8) {
      return Types.StringType.get();
    } else if (arrowType instanceof ArrowType.Binary) {
      return Types.BinaryType.get();
    } else {
      return toIcebergTemporalOrFixedType(arrowType);
    }
  }

  private static Type toIcebergTemporalOrFixedType(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.FixedSizeBinary) {
      ArrowType.FixedSizeBinary fsbType = (ArrowType.FixedSizeBinary) arrowType;
      return fsbType.getByteWidth() == 16
          ? Types.UUIDType.get()
          : Types.FixedType.ofLength(fsbType.getByteWidth());
    } else if (arrowType instanceof ArrowType.Date) {
      return Types.DateType.get();
    } else if (arrowType instanceof ArrowType.Time) {
      return Types.TimeType.get();
    } else if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
      if (tsType.getUnit() == TimeUnit.NANOSECOND) {
        return Types.TimestampNanoType.withZone();
      }
      boolean adjustToUTC = tsType.getTimezone() != null;
      return adjustToUTC ? Types.TimestampType.withZone() : Types.TimestampType.withoutZone();
    } else {
      throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
    }
  }
}
