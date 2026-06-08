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

  /** Convert a Vortex file's relocated Arrow schema to Iceberg. */
  public static Schema convert(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    List<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field> fields =
        arrowSchema.getFields();
    List<Types.NestedField> columns = Lists.newArrayListWithExpectedSize(fields.size());
    for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field = fields.get(fieldId);
      Type icebergType = toIcebergType(field);
      if (field.isNullable()) {
        columns.add(optional(fieldId, field.getName(), icebergType));
      } else {
        columns.add(required(fieldId, field.getName(), icebergType));
      }
    }

    return new Schema(columns);
  }

  /** Convert an Iceberg Schema to an Arrow Schema suitable for local Arrow vectors. */
  public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(Schema icebergSchema) {
    ImmutableList.Builder<Field> fields = ImmutableList.builder();
    for (Types.NestedField column : icebergSchema.columns()) {
      fields.add(toArrowField(column.name(), column.type(), column.isOptional()));
    }

    return new org.apache.arrow.vector.types.pojo.Schema(fields.build());
  }

  /**
   * Convert a relocated Vortex Arrow schema to an Arrow Schema suitable for local Arrow vectors.
   */
  public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema vortexSchema) {
    ImmutableList.Builder<Field> fields = ImmutableList.builder();
    for (dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field :
        vortexSchema.getFields()) {
      fields.add(toArrowField(field));
    }

    return new org.apache.arrow.vector.types.pojo.Schema(
        fields.build(), vortexSchema.getCustomMetadata());
  }

  /** Convert an Iceberg Schema to a relocated Arrow Schema suitable for {@code VortexWriter}. */
  public static dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema toVortexArrowSchema(
      Schema icebergSchema) {
    ImmutableList.Builder<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field> fields =
        ImmutableList.builder();
    for (Types.NestedField column : icebergSchema.columns()) {
      fields.add(toVortexArrowField(column.name(), column.type(), column.isOptional()));
    }

    return new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema(fields.build());
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

  private static Field toArrowField(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field) {
    List<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field> children =
        field.getChildren();
    List<Field> arrowChildren = null;
    if (!children.isEmpty()) {
      ImmutableList.Builder<Field> arrowChildBuilder = ImmutableList.builder();
      for (dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field child : children) {
        arrowChildBuilder.add(toArrowField(child));
      }

      arrowChildren = arrowChildBuilder.build();
    }

    return new Field(
        field.getName(),
        new FieldType(field.isNullable(), toArrowType(field.getType()), null, field.getMetadata()),
        arrowChildren);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static ArrowType toArrowType(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType arrowType) {
    if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Null) {
      return ArrowType.Null.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Struct) {
      return ArrowType.Struct.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.List) {
      return ArrowType.List.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeList) {
      return ArrowType.LargeList.INSTANCE;
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList list) {
      return new ArrowType.FixedSizeList(list.getListSize());
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
      return ArrowType.Bool.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Int intType) {
      return new ArrowType.Int(intType.getBitWidth(), intType.getIsSigned());
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fpType) {
      return new ArrowType.FloatingPoint(
          FloatingPointPrecision.valueOf(fpType.getPrecision().name()));
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
      return ArrowType.Utf8.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8) {
      return ArrowType.LargeUtf8.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
      return ArrowType.Binary.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary) {
      return ArrowType.LargeBinary.INSTANCE;
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary fixed) {
      return new ArrowType.FixedSizeBinary(fixed.getByteWidth());
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Decimal decimal) {
      return new ArrowType.Decimal(
          decimal.getPrecision(), decimal.getScale(), decimal.getBitWidth());
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Date date) {
      return new ArrowType.Date(DateUnit.valueOf(date.getUnit().name()));
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Time time) {
      return new ArrowType.Time(TimeUnit.valueOf(time.getUnit().name()), time.getBitWidth());
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestamp) {
      return new ArrowType.Timestamp(
          TimeUnit.valueOf(timestamp.getUnit().name()), timestamp.getTimezone());
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType ext) {
      return toArrowType(ext.storageType());
    }

    throw new UnsupportedOperationException(
        "Unsupported Vortex Arrow type for Arrow conversion: " + arrowType);
  }

  private static dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field toVortexArrowField(
      String name, Type type, boolean nullable) {
    return switch (type.typeId()) {
      case BOOLEAN ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Bool(),
              nullable);
      case INTEGER ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Int(
                  Integer.SIZE, true),
              nullable);
      case LONG ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Int(
                  Long.SIZE, true),
              nullable);
      case FLOAT ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                  dev.vortex.relocated.org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE),
              nullable);
      case DOUBLE ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                  dev.vortex.relocated.org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
              nullable);
      case STRING ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Utf8(),
              nullable);
      case BINARY ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary(),
              nullable);
      case FIXED -> {
        Types.FixedType fixedType = (Types.FixedType) type;
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary(
                fixedType.length()),
            nullable);
      }
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) type;
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Decimal(
                decimalType.precision(), decimalType.scale(), 128),
            nullable);
      }
      case UUID -> {
        Map<String, String> extMetadata =
            ImmutableMap.of(
                dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                    .EXTENSION_METADATA_KEY_NAME,
                UUID_EXTENSION_NAME,
                dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                    .EXTENSION_METADATA_KEY_METADATA,
                "");
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary(
                16),
            nullable,
            extMetadata,
            null);
      }
      case DATE ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Date(
                  dev.vortex.relocated.org.apache.arrow.vector.types.DateUnit.DAY),
              nullable);
      case TIME ->
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Time(
                  dev.vortex.relocated.org.apache.arrow.vector.types.TimeUnit.MICROSECOND,
                  Long.SIZE),
              nullable);
      case TIMESTAMP -> {
        Types.TimestampType tsType = (Types.TimestampType) type;
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(
                dev.vortex.relocated.org.apache.arrow.vector.types.TimeUnit.MICROSECOND,
                tsType.shouldAdjustToUTC() ? "UTC" : null),
            nullable);
      }
      case TIMESTAMP_NANO -> {
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) type;
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(
                dev.vortex.relocated.org.apache.arrow.vector.types.TimeUnit.NANOSECOND,
                tsNanoType.shouldAdjustToUTC() ? "UTC" : null),
            nullable);
      }
      case LIST -> {
        Types.ListType listType = (Types.ListType) type;
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field elementField =
            toVortexArrowField("element", listType.elementType(), listType.isElementOptional());
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.List(),
            nullable,
            null,
            ImmutableList.of(elementField));
      }
      case STRUCT -> {
        Types.StructType structType = (Types.StructType) type;
        ImmutableList.Builder<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field>
            children = ImmutableList.builder();
        for (Types.NestedField field : structType.fields()) {
          children.add(toVortexArrowField(field.name(), field.type(), field.isOptional()));
        }

        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Struct(),
            nullable,
            null,
            children.build());
      }
      default ->
          throw new UnsupportedOperationException(
              "Unsupported Iceberg type for Arrow conversion: " + type);
    };
  }

  private static dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field toVortexArrowField(
      String name,
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType arrowType,
      boolean nullable) {
    return toVortexArrowField(name, arrowType, nullable, null, null);
  }

  private static dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field toVortexArrowField(
      String name,
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType arrowType,
      boolean nullable,
      Map<String, String> metadata,
      List<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field> children) {
    return new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field(
        name,
        new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.FieldType(
            nullable, arrowType, null, metadata),
        children);
  }

  private static Type toIcebergType(Field field) {
    // UUID is conveyed as the {@code arrow.uuid} extension over
    // FixedSizeBinary(16). Check metadata directly so this works whether or not
    // the extension is registered with ExtensionTypeRegistry.
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

  private static Type toIcebergType(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field) {
    if (isUuidField(field)) {
      return Types.UUIDType.get();
    }
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType arrowType = field.getType();
    if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Int intType) {
      return intType.getBitWidth() <= Integer.SIZE ? Types.IntegerType.get() : Types.LongType.get();
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fpType) {
      return toIcebergFloatingPoint(fpType);
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Decimal decType) {
      return Types.DecimalType.of(decType.getPrecision(), decType.getScale());
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary fixed) {
      return Types.FixedType.ofLength(fixed.getByteWidth());
    } else if (arrowType
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Timestamp tsType) {
      return toIcebergTimestamp(tsType);
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.List) {
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

  private static Type toIcebergSimpleType(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType arrowType) {
    if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Null) {
      return Types.UnknownType.get();
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
      return Types.BooleanType.get();
    } else if (arrowType
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Utf8
        || arrowType
            instanceof
            dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8) {
      return Types.StringType.get();
    } else if (arrowType
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary
        || arrowType
            instanceof
            dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary) {
      return Types.BinaryType.get();
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Date) {
      return Types.DateType.get();
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Time) {
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

  private static Type toIcebergFloatingPoint(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fpType) {
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

  private static Type toIcebergTimestamp(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Timestamp tsType) {
    boolean isNano =
        tsType.getUnit() == dev.vortex.relocated.org.apache.arrow.vector.types.TimeUnit.NANOSECOND;
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

  private static Type toIcebergList(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field) {
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field elementField =
        field.getChildren().get(0);
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

  public static boolean isUuidField(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field) {
    if (field.getType()
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType ext) {
      return UUID_EXTENSION_NAME.equals(ext.extensionName());
    }
    return UUID_EXTENSION_NAME.equals(
        field
            .getMetadata()
            .get(
                dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                    .EXTENSION_METADATA_KEY_NAME));
  }
}
