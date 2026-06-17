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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public final class VortexSchemas {
  /** Canonical Arrow extension name for UUIDs (matches {@code arrow.vector.extension.UuidType}). */
  static final String UUID_EXTENSION_NAME = "arrow.uuid";

  /**
   * Canonical Arrow extension name for Parquet variant (matches {@code
   * arrow.vector.extension.ParquetVariant}).
   */
  static final String VARIANT_EXTENSION_NAME = "arrow.parquet.variant";

  private VortexSchemas() {}

  /** Convert a Vortex file's Arrow {@link org.apache.arrow.vector.types.pojo.Schema} to Iceberg. */
  public static Schema convert(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    return new Schema(convertFields(arrowSchema.getFields(), new AtomicInteger(0)));
  }

  // Arrow/Vortex schemas carry no Iceberg field ids, so ids are synthesized here. A single shared
  // counter assigns each field (including nested struct fields and list elements) a unique id in
  // pre-order, which is all Iceberg requires for a valid schema; binding/projection happens by
  // name.
  private static List<Types.NestedField> convertFields(List<Field> fields, AtomicInteger nextId) {
    List<Types.NestedField> columns = Lists.newArrayListWithExpectedSize(fields.size());
    for (Field field : fields) {
      int fieldId = nextId.getAndIncrement();
      Type icebergType = toIcebergType(field, nextId);
      if (field.isNullable()) {
        columns.add(optional(fieldId, field.getName(), icebergType));
      } else {
        columns.add(required(fieldId, field.getName(), icebergType));
      }
    }

    return columns;
  }

  /** Convert a Vortex file's relocated Arrow schema to Iceberg. */
  public static Schema convert(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    return new Schema(convertVortexFields(arrowSchema.getFields(), new AtomicInteger(0)));
  }

  // Counterpart of convertFields for relocated Vortex Arrow fields (see that method for details). A
  // distinct name is required because both overloads would otherwise erase to convert(List, ...).
  private static List<Types.NestedField> convertVortexFields(
      List<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field> fields,
      AtomicInteger nextId) {
    List<Types.NestedField> columns = Lists.newArrayListWithExpectedSize(fields.size());
    for (dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field : fields) {
      int fieldId = nextId.getAndIncrement();
      Type icebergType = toIcebergType(field, nextId);
      if (field.isNullable()) {
        columns.add(optional(fieldId, field.getName(), icebergType));
      } else {
        columns.add(required(fieldId, field.getName(), icebergType));
      }
    }

    return columns;
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
      case VARIANT -> {
        Map<String, String> extMetadata =
            ImmutableMap.of(
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
                VARIANT_EXTENSION_NAME,
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA,
                "");

        ImmutableList.Builder<Field> children = ImmutableList.builder();
        children.add(
            new Field("metadata", new FieldType(false, ArrowType.Binary.INSTANCE, null), null));
        children.add(
            new Field("value", new FieldType(true, ArrowType.Binary.INSTANCE, null), null));

        yield new Field(
            name,
            new FieldType(nullable, ArrowType.Struct.INSTANCE, null, extMetadata),
            children.build());
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
      case VARIANT -> {
        yield toVortexVariantArrowField(name, nullable);
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

  private static dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field
      toVortexVariantArrowField(String name, boolean nullable) {
    Map<String, String> extMetadata =
        ImmutableMap.of(
            dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                .EXTENSION_METADATA_KEY_NAME,
            VARIANT_EXTENSION_NAME,
            dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                .EXTENSION_METADATA_KEY_METADATA,
            "");

    ImmutableList.Builder<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field> children =
        ImmutableList.builder();
    children.add(
        toVortexArrowField(
            "metadata",
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary(),
            false));
    children.add(
        toVortexArrowField(
            "value",
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary(),
            true));

    return toVortexArrowField(
        name,
        new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Struct(),
        nullable,
        extMetadata,
        children.build());
  }

  private static Type toIcebergType(Field field, AtomicInteger nextId) {
    // UUID is conveyed as the {@code arrow.uuid} extension over
    // FixedSizeBinary(16). Check metadata directly so this works whether or not
    // the extension is registered with ExtensionTypeRegistry.
    Type extensionType = toIcebergExtensionType(field);
    if (extensionType != null) {
      return extensionType;
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
    } else if (arrowType instanceof ArrowType.List
        || arrowType instanceof ArrowType.LargeList
        || arrowType instanceof ArrowType.FixedSizeList) {
      return toIcebergList(field, nextId);
    } else if (arrowType instanceof ArrowType.Struct) {
      return Types.StructType.of(convertFields(field.getChildren(), nextId));
    }
    return toIcebergSimpleType(arrowType);
  }

  private static Type toIcebergExtensionType(Field field) {
    if (isUuidField(field)) {
      return Types.UUIDType.get();
    }

    if (isVariantField(field)) {
      validateVariantField(field);
      return Types.VariantType.get();
    }

    return null;
  }

  private static Type toIcebergType(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field, AtomicInteger nextId) {
    Type extensionType = toIcebergExtensionType(field);
    if (extensionType != null) {
      return extensionType;
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
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.List
        || arrowType
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeList
        || arrowType
            instanceof
            dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList) {
      return toIcebergList(field, nextId);
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Struct) {
      return Types.StructType.of(convertVortexFields(field.getChildren(), nextId));
    }
    return toIcebergSimpleType(arrowType);
  }

  private static Type toIcebergExtensionType(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field) {
    if (isUuidField(field)) {
      return Types.UUIDType.get();
    }

    if (isVariantField(field)) {
      return Types.VariantType.get();
    }

    return null;
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

  private static void validateVariantField(Field field) {
    Preconditions.checkArgument(
        field.getType() instanceof ArrowType.Struct,
        "Invalid Arrow variant field %s: expected struct storage type, found %s",
        field.getName(),
        field.getType());

    Field metadata = findChild(field, "metadata");
    Preconditions.checkArgument(
        metadata != null,
        "Invalid Arrow variant field %s: missing metadata child",
        field.getName());
    Preconditions.checkArgument(
        !metadata.isNullable(),
        "Invalid Arrow variant field %s: metadata child must be non-nullable",
        field.getName());
    Preconditions.checkArgument(
        isBinaryLike(metadata.getType()),
        "Invalid Arrow variant field %s: metadata child must be binary, found %s",
        field.getName(),
        metadata.getType());

    Field value = findChild(field, "value");
    if (value != null) {
      Preconditions.checkArgument(
          value.isNullable(),
          "Invalid Arrow variant field %s: value child must be nullable",
          field.getName());
      Preconditions.checkArgument(
          isBinaryLike(value.getType()),
          "Invalid Arrow variant field %s: value child must be binary, found %s",
          field.getName(),
          value.getType());
    }

    Field typedValue = findChild(field, "typed_value");
    if (typedValue != null) {
      Preconditions.checkArgument(
          typedValue.isNullable(),
          "Invalid Arrow variant field %s: typed_value child must be nullable",
          field.getName());
    }

    Preconditions.checkArgument(
        value != null || typedValue != null,
        "Invalid Arrow variant field %s: expected value or typed_value child",
        field.getName());
  }

  private static Field findChild(Field field, String name) {
    for (Field child : field.getChildren()) {
      if (name.equals(child.getName())) {
        return child;
      }
    }

    return null;
  }

  private static boolean isBinaryLike(ArrowType arrowType) {
    return arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary;
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

  private static Type toIcebergList(Field field, AtomicInteger nextId) {
    Field elementField = field.getChildren().get(0);
    int elementId = nextId.getAndIncrement();
    Type innerType = toIcebergType(elementField, nextId);
    return elementField.isNullable()
        ? Types.ListType.ofOptional(elementId, innerType)
        : Types.ListType.ofRequired(elementId, innerType);
  }

  private static Type toIcebergList(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field, AtomicInteger nextId) {
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field elementField =
        field.getChildren().get(0);
    int elementId = nextId.getAndIncrement();
    Type innerType = toIcebergType(elementField, nextId);
    return elementField.isNullable()
        ? Types.ListType.ofOptional(elementId, innerType)
        : Types.ListType.ofRequired(elementId, innerType);
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

  public static boolean isVariantField(Field field) {
    if (field.getType() instanceof ArrowType.ExtensionType ext) {
      return VARIANT_EXTENSION_NAME.equals(ext.extensionName());
    }
    return VARIANT_EXTENSION_NAME.equals(
        field.getMetadata().get(ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME));
  }

  public static boolean isVariantField(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field) {
    if (field.getType()
        instanceof
        dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType ext) {
      return VARIANT_EXTENSION_NAME.equals(ext.extensionName());
    }
    return VARIANT_EXTENSION_NAME.equals(
        field
            .getMetadata()
            .get(
                dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                    .EXTENSION_METADATA_KEY_NAME));
  }
}
