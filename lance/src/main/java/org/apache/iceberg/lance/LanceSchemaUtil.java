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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Utility class for converting between Iceberg Schema and Arrow Schema.
 *
 * <p>Lance format is built on top of Apache Arrow, so the schema conversion goes through Arrow as
 * an intermediate representation: Iceberg Schema <-> Arrow Schema <-> Lance Schema.
 */
public class LanceSchemaUtil {

  private LanceSchemaUtil() {}

  /**
   * Convert an Iceberg {@link Schema} to an Arrow {@link org.apache.arrow.vector.types.pojo.Schema}.
   *
   * @param icebergSchema the Iceberg schema to convert
   * @return the equivalent Arrow schema
   */
  public static org.apache.arrow.vector.types.pojo.Schema toArrow(Schema icebergSchema) {
    Preconditions.checkNotNull(icebergSchema, "Iceberg schema cannot be null");
    List<Field> fields = new ArrayList<>();
    for (Types.NestedField icebergField : icebergSchema.columns()) {
      fields.add(toArrowField(icebergField));
    }
    return new org.apache.arrow.vector.types.pojo.Schema(fields);
  }

  /**
   * Convert an Arrow {@link org.apache.arrow.vector.types.pojo.Schema} to an Iceberg {@link Schema}.
   *
   * @param arrowSchema the Arrow schema to convert
   * @return the equivalent Iceberg schema
   */
  public static Schema toIceberg(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    Preconditions.checkNotNull(arrowSchema, "Arrow schema cannot be null");
    List<Types.NestedField> icebergFields = new ArrayList<>();
    int fieldId = 1;
    for (Field arrowField : arrowSchema.getFields()) {
      icebergFields.add(toIcebergField(fieldId, arrowField));
      fieldId = nextFieldId(fieldId, arrowField);
    }
    return new Schema(icebergFields);
  }

  /**
   * Convert a single Iceberg field to an Arrow field.
   */
  static Field toArrowField(Types.NestedField icebergField) {
    ArrowType arrowType = toArrowType(icebergField.type());
    boolean nullable = icebergField.isOptional();

    if (icebergField.type().isStructType()) {
      List<Field> children = new ArrayList<>();
      for (Types.NestedField child : icebergField.type().asStructType().fields()) {
        children.add(toArrowField(child));
      }
      return new Field(icebergField.name(), new FieldType(nullable, arrowType, null), children);
    } else if (icebergField.type().isListType()) {
      Types.ListType listType = icebergField.type().asListType();
      Field elementField = toArrowField(listType.fields().get(0));
      List<Field> children = new ArrayList<>();
      children.add(elementField);
      return new Field(icebergField.name(), new FieldType(nullable, arrowType, null), children);
    } else if (icebergField.type().isMapType()) {
      Types.MapType mapType = icebergField.type().asMapType();
      Field keyField =
          new Field("key", new FieldType(false, toArrowType(mapType.keyType()), null), null);
      Field valueField = toArrowField(
          Types.NestedField.of(
              mapType.valueId(),
              mapType.isValueOptional(),
              "value",
              mapType.valueType()));
      List<Field> entryChildren = new ArrayList<>();
      entryChildren.add(keyField);
      entryChildren.add(valueField);
      Field entryField =
          new Field(
              "entries",
              new FieldType(false, new ArrowType.Struct(), null),
              entryChildren);
      List<Field> children = new ArrayList<>();
      children.add(entryField);
      return new Field(icebergField.name(), new FieldType(nullable, arrowType, null), children);
    }

    return new Field(icebergField.name(), new FieldType(nullable, arrowType, null), null);
  }

  /**
   * Convert an Iceberg type to the corresponding Arrow type.
   */
  static ArrowType toArrowType(Type type) {
    switch (type.typeId()) {
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
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        } else {
          return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        }
      case STRING:
        return ArrowType.Utf8.INSTANCE;
      case BINARY:
        return ArrowType.Binary.INSTANCE;
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) type;
        return new ArrowType.FixedSizeBinary(fixedType.length());
      case UUID:
        return new ArrowType.FixedSizeBinary(16);
      case DECIMAL:
        Types.DecimalType decType = (Types.DecimalType) type;
        return new ArrowType.Decimal(decType.precision(), decType.scale(), 128);
      case LIST:
        return ArrowType.List.INSTANCE;
      case MAP:
        return new ArrowType.Map(false);
      case STRUCT:
        return ArrowType.Struct.INSTANCE;
      default:
        throw new UnsupportedOperationException("Unsupported Iceberg type: " + type);
    }
  }

  /**
   * Convert a single Arrow field to an Iceberg nested field.
   */
  static Types.NestedField toIcebergField(int fieldId, Field arrowField) {
    Type icebergType = toIcebergType(fieldId, arrowField);
    boolean isOptional = arrowField.isNullable();
    if (isOptional) {
      return Types.NestedField.optional(fieldId, arrowField.getName(), icebergType);
    } else {
      return Types.NestedField.required(fieldId, arrowField.getName(), icebergType);
    }
  }

  /**
   * Convert an Arrow type/field to the corresponding Iceberg type.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static Type toIcebergType(int fieldId, Field arrowField) {
    ArrowType arrowType = arrowField.getType();

    if (arrowType instanceof ArrowType.Bool) {
      return Types.BooleanType.get();
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      if (intType.getBitWidth() <= 32) {
        return Types.IntegerType.get();
      } else {
        return Types.LongType.get();
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
      if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
        return Types.FloatType.get();
      } else {
        return Types.DoubleType.get();
      }
    } else if (arrowType instanceof ArrowType.Date) {
      return Types.DateType.get();
    } else if (arrowType instanceof ArrowType.Time) {
      return Types.TimeType.get();
    } else if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
      if (tsType.getTimezone() != null) {
        return Types.TimestampType.withZone();
      } else {
        return Types.TimestampType.withoutZone();
      }
    } else if (arrowType instanceof ArrowType.Utf8) {
      return Types.StringType.get();
    } else if (arrowType instanceof ArrowType.Binary) {
      return Types.BinaryType.get();
    } else if (arrowType instanceof ArrowType.FixedSizeBinary) {
      ArrowType.FixedSizeBinary fixedType = (ArrowType.FixedSizeBinary) arrowType;
      if (fixedType.getByteWidth() == 16) {
        return Types.UUIDType.get();
      }
      return Types.FixedType.ofLength(fixedType.getByteWidth());
    } else if (arrowType instanceof ArrowType.Decimal) {
      ArrowType.Decimal decType = (ArrowType.Decimal) arrowType;
      return Types.DecimalType.of(decType.getPrecision(), decType.getScale());
    } else if (arrowType instanceof ArrowType.List) {
      List<Field> children = arrowField.getChildren();
      if (children != null && !children.isEmpty()) {
        Field elementField = children.get(0);
        int elementId = fieldId + 1;
        Types.NestedField element = toIcebergField(elementId, elementField);
        return Types.ListType.ofOptional(elementId, element.type());
      }
      return Types.ListType.ofOptional(fieldId + 1, Types.StringType.get());
    } else if (arrowType instanceof ArrowType.Map) {
      List<Field> children = arrowField.getChildren();
      if (children != null && !children.isEmpty()) {
        Field entriesField = children.get(0);
        List<Field> entryChildren = entriesField.getChildren();
        if (entryChildren != null && entryChildren.size() == 2) {
          int keyId = fieldId + 1;
          int valueId = fieldId + 2;
          Type keyType = toIcebergType(keyId, entryChildren.get(0));
          Type valueType = toIcebergType(valueId, entryChildren.get(1));
          return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
        }
      }
      return Types.MapType.ofOptional(
          fieldId + 1, fieldId + 2, Types.StringType.get(), Types.StringType.get());
    } else if (arrowType instanceof ArrowType.Struct) {
      List<Field> children = arrowField.getChildren();
      List<Types.NestedField> structFields = new ArrayList<>();
      int childId = fieldId + 1;
      if (children != null) {
        for (Field child : children) {
          structFields.add(toIcebergField(childId, child));
          childId = nextFieldId(childId, child);
        }
      }
      return Types.StructType.of(structFields);
    }

    throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
  }

  /**
   * Calculate the next available field ID after processing a field (and all its descendants).
   */
  private static int nextFieldId(int currentId, Field field) {
    int nextId = currentId + 1;
    if (field.getChildren() != null) {
      for (Field child : field.getChildren()) {
        nextId = nextFieldId(nextId, child);
      }
    }
    return nextId;
  }
}
