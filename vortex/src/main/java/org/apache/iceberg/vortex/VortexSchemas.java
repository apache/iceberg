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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public final class VortexSchemas {
  /** Canonical Arrow extension name for UUIDs (matches {@code arrow.vector.extension.UuidType}). */
  static final String UUID_EXTENSION_NAME = "arrow.uuid";

  private VortexSchemas() {}

  /** Convert a Vortex file's Arrow {@link org.apache.arrow.vector.types.pojo.Schema} to Iceberg. */
  public static Schema convert(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    List<Field> fields = arrowSchema.getFields();
    List<Types.NestedField> columns = Lists.newArrayListWithExpectedSize(fields.size());
    for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
      Field field = fields.get(fieldId);
      Type icebergType = toIcebergType(field);
      if (field.isNullable()) {
        columns.add(optional(fieldId, field.getName(), icebergType));
      } else {
        columns.add(required(fieldId, field.getName(), icebergType));
      }
    }

    return new Schema(columns);
  }

  /** Convert an Iceberg Schema to an Arrow Schema suitable for {@code VortexWriter.create}. */
  public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(Schema icebergSchema) {
    ImmutableList.Builder<Field> fields = ImmutableList.builder();
    for (Types.NestedField column : icebergSchema.columns()) {
      fields.add(toArrowField(column.name(), column.type(), column.isOptional()));
    }

    return new org.apache.arrow.vector.types.pojo.Schema(fields.build());
  }

  private static Field toArrowField(String name, Type type, boolean nullable) {
    return switch (type.typeId()) {
      case BOOLEAN -> new Field(name, new FieldType(nullable, ArrowType.Bool.INSTANCE, null), null);
      case INTEGER ->
          new Field(
              name, new FieldType(nullable, new ArrowType.Int(Integer.SIZE, true), null), null);
      case LONG ->
          new Field(name, new FieldType(nullable, new ArrowType.Int(Long.SIZE, true), null), null);
      case FLOAT ->
          new Field(
              name,
              new FieldType(
                  nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null),
              null);
      case DOUBLE ->
          new Field(
              name,
              new FieldType(
                  nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
              null);
      case STRING -> new Field(name, new FieldType(nullable, ArrowType.Utf8.INSTANCE, null), null);
      case BINARY ->
          new Field(name, new FieldType(nullable, ArrowType.Binary.INSTANCE, null), null);
      case FIXED -> {
        Types.FixedType fixedType = (Types.FixedType) type;
        yield new Field(
            name,
            new FieldType(nullable, new ArrowType.FixedSizeBinary(fixedType.length()), null),
            null);
      }
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) type;
        yield new Field(
            name,
            new FieldType(
                nullable,
                new ArrowType.Decimal(decimalType.precision(), decimalType.scale(), 128),
                null),
            null);
      }
      case UUID -> {
        Map<String, String> extMetadata =
            ImmutableMap.of(
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
                UUID_EXTENSION_NAME,
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA,
                "");
        yield new Field(
            name,
            new FieldType(nullable, new ArrowType.FixedSizeBinary(16), null, extMetadata),
            null);
      }
      case DATE ->
          new Field(name, new FieldType(nullable, new ArrowType.Date(DateUnit.DAY), null), null);
      case TIME ->
          new Field(
              name,
              new FieldType(nullable, new ArrowType.Time(TimeUnit.MICROSECOND, Long.SIZE), null),
              null);
      case TIMESTAMP -> {
        Types.TimestampType tsType = (Types.TimestampType) type;
        yield new Field(
            name,
            new FieldType(
                nullable,
                new ArrowType.Timestamp(
                    TimeUnit.MICROSECOND, tsType.shouldAdjustToUTC() ? "UTC" : null),
                null),
            null);
      }
      case TIMESTAMP_NANO -> {
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) type;
        yield new Field(
            name,
            new FieldType(
                nullable,
                new ArrowType.Timestamp(
                    TimeUnit.NANOSECOND, tsNanoType.shouldAdjustToUTC() ? "UTC" : null),
                null),
            null);
      }
      case LIST -> {
        Types.ListType listType = (Types.ListType) type;
        Field elementField =
            toArrowField("element", listType.elementType(), listType.isElementOptional());
        yield new Field(
            name,
            new FieldType(nullable, ArrowType.List.INSTANCE, null),
            ImmutableList.of(elementField));
      }
      case STRUCT -> {
        Types.StructType structType = (Types.StructType) type;
        ImmutableList.Builder<Field> children = ImmutableList.builder();
        for (Types.NestedField field : structType.fields()) {
          children.add(toArrowField(field.name(), field.type(), field.isOptional()));
        }

        yield new Field(
            name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null), children.build());
      }
      default ->
          throw new UnsupportedOperationException(
              "Unsupported Iceberg type for Arrow conversion: " + type);
    };
  }

  private static Type toIcebergType(Field field) {
    // UUID is conveyed as the {@code arrow.uuid} extension over FixedSizeBinary(16). Check the
    // metadata directly so this works whether or not the extension is registered with
    // ExtensionTypeRegistry (i.e. whether arrowType deserialized to ExtensionType or stayed as
    // FixedSizeBinary).
    if (isUuidField(field)) {
      return Types.UUIDType.get();
    }
    ArrowType arrowType = field.getType();
    if (arrowType instanceof ArrowType.Int intType) {
      return intType.getBitWidth() <= Integer.SIZE ? Types.IntegerType.get() : Types.LongType.get();
    } else if (arrowType instanceof ArrowType.FloatingPoint fpType) {
      return toIcebergFloatingPoint(fpType);
    } else if (arrowType instanceof ArrowType.Decimal decType) {
      return Types.DecimalType.of(decType.getPrecision(), decType.getScale());
    } else if (arrowType instanceof ArrowType.FixedSizeBinary fixed) {
      return Types.FixedType.ofLength(fixed.getByteWidth());
    } else if (arrowType instanceof ArrowType.Timestamp tsType) {
      return toIcebergTimestamp(tsType);
    } else if (arrowType instanceof ArrowType.List) {
      return toIcebergList(field);
    }
    return toIcebergSimpleType(arrowType);
  }

  private static Type toIcebergSimpleType(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Null) {
      return Types.UnknownType.get();
    } else if (arrowType instanceof ArrowType.Bool) {
      return Types.BooleanType.get();
    } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
      return Types.StringType.get();
    } else if (arrowType instanceof ArrowType.Binary
        || arrowType instanceof ArrowType.LargeBinary) {
      return Types.BinaryType.get();
    } else if (arrowType instanceof ArrowType.Date) {
      return Types.DateType.get();
    } else if (arrowType instanceof ArrowType.Time) {
      return Types.TimeType.get();
    }
    throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
  }

  private static Type toIcebergFloatingPoint(ArrowType.FloatingPoint fpType) {
    return switch (fpType.getPrecision()) {
      case SINGLE -> Types.FloatType.get();
      case DOUBLE -> Types.DoubleType.get();
      case HALF ->
          throw new UnsupportedOperationException("Half-precision floats are not supported");
    };
  }

  private static Type toIcebergTimestamp(ArrowType.Timestamp tsType) {
    boolean isNano = tsType.getUnit() == TimeUnit.NANOSECOND;
    if (tsType.getTimezone() == null) {
      return isNano ? Types.TimestampNanoType.withoutZone() : Types.TimestampType.withoutZone();
    }
    return isNano ? Types.TimestampNanoType.withZone() : Types.TimestampType.withZone();
  }

  private static Type toIcebergList(Field field) {
    Field elementField = field.getChildren().get(0);
    Type innerType = toIcebergType(elementField);
    return elementField.isNullable()
        ? Types.ListType.ofOptional(0, innerType)
        : Types.ListType.ofRequired(0, innerType);
  }

  /**
   * True when {@code field} carries the {@code arrow.uuid} extension marker. Checking the field
   * metadata works whether or not {@link ArrowType.ExtensionType} was deserialized by the registry.
   */
  public static boolean isUuidField(Field field) {
    if (field.getType() instanceof ArrowType.ExtensionType ext) {
      return UUID_EXTENSION_NAME.equals(ext.extensionName());
    }
    return UUID_EXTENSION_NAME.equals(
        field.getMetadata().get(ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME));
  }
}
