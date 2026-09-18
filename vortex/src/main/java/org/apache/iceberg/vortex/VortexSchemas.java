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
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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

  // Canonical Arrow child names for the Map layout: a map field has a single non-nullable
  // "entries" struct child holding "key" and "value" (see MapVector.DATA_VECTOR_NAME etc.).
  public static final String MAP_ENTRIES_NAME = "entries";
  public static final String MAP_KEY_NAME = "key";
  public static final String MAP_VALUE_NAME = "value";

  /**
   * Vortex file-metadata key holding the JSON Iceberg schema the file was written with. Vortex
   * drops Arrow field and schema metadata, so this file-level channel is the only way to persist
   * Iceberg field ids.
   */
  public static final String ICEBERG_SCHEMA_KEY = "iceberg.schema";

  /**
   * Arrow field-metadata key carrying a field's Iceberg id. Only ever set in memory, by {@link
   * #withFieldIds}, from the schema stored under {@link #ICEBERG_SCHEMA_KEY}; it is never written
   * to a file.
   */
  public static final String FIELD_ID_KEY = "PARQUET:field_id";

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
    for (Types.NestedField column : writtenFields(icebergSchema.columns())) {
      fields.add(toArrowField(column.name(), column.type(), column.isOptional()));
    }

    return new org.apache.arrow.vector.types.pojo.Schema(fields.build());
  }

  /**
   * Drops the {@code unknown} fields from {@code fields}. Unknown columns hold nothing but nulls,
   * so they are left out of the file entirely and readers fill them back in as null. Writers use
   * this to line their columns up with the Arrow vectors that were actually created.
   */
  public static List<Types.NestedField> writtenFields(List<Types.NestedField> fields) {
    ImmutableList.Builder<Types.NestedField> written = ImmutableList.builder();
    for (Types.NestedField field : fields) {
      if (field.type().typeId() != Type.TypeID.UNKNOWN) {
        written.add(field);
      }
    }

    return written.build();
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
    for (Types.NestedField column : writtenFields(icebergSchema.columns())) {
      fields.add(toVortexArrowField(column.name(), column.type(), column.isOptional()));
    }

    return new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema(fields.build());
  }

  private static Field toArrowField(String name, Type type, boolean nullable) {
    return switch (type.typeId()) {
      case UNKNOWN ->
          // Reached only for a list element or a map key/value: struct fields of type unknown are
          // dropped before conversion (see writtenFields). Those positions have no slot to drop,
          // so they are stored as an Arrow null column, which holds no values and is exactly what
          // unknown means.
          new Field(name, new FieldType(nullable, ArrowType.Null.INSTANCE, null), null);
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
      case BINARY, GEOMETRY, GEOGRAPHY ->
          // Geometry and geography are stored as WKB binary (see the geospatial appendix of the
          // spec). The Iceberg schema in the file's metadata is what tells them apart from BINARY
          // on read; a file read without one surfaces them as BINARY.
          new Field(name, new FieldType(nullable, ArrowType.Binary.INSTANCE, null), null);
      case FIXED -> throw unsupportedFixed(name);
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
        // arrow.uuid is a metadata-less canonical extension: an ARROW:extension:metadata entry
        // (even an empty one) makes strict validators such as arrow-rs reject the field.
        Map<String, String> extMetadata =
            ImmutableMap.of(
                ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME, UUID_EXTENSION_NAME);
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
      case MAP -> {
        Types.MapType mapType = (Types.MapType) type;
        Field keyField = toArrowField(MAP_KEY_NAME, mapType.keyType(), false);
        Field valueField =
            toArrowField(MAP_VALUE_NAME, mapType.valueType(), mapType.isValueOptional());
        Field entriesField =
            new Field(
                MAP_ENTRIES_NAME,
                new FieldType(false, ArrowType.Struct.INSTANCE, null),
                ImmutableList.of(keyField, valueField));
        yield new Field(
            name,
            new FieldType(nullable, new ArrowType.Map(false), null),
            ImmutableList.of(entriesField));
      }
      case STRUCT -> {
        Types.StructType structType = (Types.StructType) type;
        ImmutableList.Builder<Field> children = ImmutableList.builder();
        for (Types.NestedField field : writtenFields(structType.fields())) {
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
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Map map) {
      return new ArrowType.Map(map.getKeysSorted());
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
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Utf8View) {
      // Preserve view-ness so readers can bind view-specialized accessors up front.
      return ArrowType.Utf8View.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8) {
      return ArrowType.LargeUtf8.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary) {
      return ArrowType.Binary.INSTANCE;
    } else if (arrowType
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.BinaryView) {
      return ArrowType.BinaryView.INSTANCE;
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
      case UNKNOWN ->
          // See toArrowField: a list element or map key/value of type unknown is stored as an
          // Arrow null column.
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Null(),
              nullable);
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
      case BINARY, GEOMETRY, GEOGRAPHY ->
          // See toArrowField: geometry and geography are stored as WKB binary.
          toVortexArrowField(
              name,
              new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary(),
              nullable);
      case FIXED -> throw unsupportedFixed(name);
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) type;
        yield toVortexArrowField(
            name,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Decimal(
                decimalType.precision(), decimalType.scale(), 128),
            nullable);
      }
      case UUID -> {
        // arrow.uuid is a metadata-less canonical extension: an ARROW:extension:metadata entry
        // (even an empty one) makes strict validators such as arrow-rs reject the field.
        Map<String, String> extMetadata =
            ImmutableMap.of(
                dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
                    .EXTENSION_METADATA_KEY_NAME,
                UUID_EXTENSION_NAME);
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
      case MAP -> toVortexMapArrowField(name, (Types.MapType) type, nullable);
      case STRUCT -> {
        Types.StructType structType = (Types.StructType) type;
        ImmutableList.Builder<dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field>
            children = ImmutableList.builder();
        for (Types.NestedField field : writtenFields(structType.fields())) {
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
      toVortexMapArrowField(String name, Types.MapType mapType, boolean nullable) {
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field keyField =
        toVortexArrowField(MAP_KEY_NAME, mapType.keyType(), false);
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field valueField =
        toVortexArrowField(MAP_VALUE_NAME, mapType.valueType(), mapType.isValueOptional());
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field entriesField =
        toVortexArrowField(
            MAP_ENTRIES_NAME,
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Struct(),
            false,
            null,
            ImmutableList.of(keyField, valueField));

    return toVortexArrowField(
        name,
        new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Map(false),
        nullable,
        null,
        ImmutableList.of(entriesField));
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

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
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
    } else if (arrowType instanceof ArrowType.Map) {
      return toIcebergMap(field, nextId);
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

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
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
        instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Map) {
      return toIcebergMap(field, nextId);
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
    } else if (arrowType instanceof ArrowType.Utf8
        || arrowType instanceof ArrowType.LargeUtf8
        || arrowType instanceof ArrowType.Utf8View) {
      return Types.StringType.get();
    } else if (arrowType instanceof ArrowType.Binary
        || arrowType instanceof ArrowType.LargeBinary
        || arrowType instanceof ArrowType.BinaryView) {
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
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8
        || arrowType
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Utf8View) {
      return Types.StringType.get();
    } else if (arrowType
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Binary
        || arrowType
            instanceof dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary
        || arrowType
            instanceof
            dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.BinaryView) {
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
   * Vortex has no fixed-width binary type: it rejects every Arrow FixedSizeBinary field that is not
   * tagged as the {@code arrow.uuid} extension, and the rejection surfaces as an opaque native
   * failure when the writer is created. Iceberg FIXED columns are therefore refused here, while the
   * file is still being described, so callers get a message naming the column and the reason.
   */
  private static UnsupportedOperationException unsupportedFixed(String name) {
    return new UnsupportedOperationException(
        "Cannot write Iceberg FIXED column "
            + name
            + ": Vortex has no fixed-width binary type. Use BINARY instead.");
  }

  private static Type toIcebergMap(Field field, AtomicInteger nextId) {
    Field entries = field.getChildren().get(0);
    Field keyField = entries.getChildren().get(0);
    Field valueField = entries.getChildren().get(1);
    int keyId = nextId.getAndIncrement();
    Type keyType = toIcebergType(keyField, nextId);
    int valueId = nextId.getAndIncrement();
    Type valueType = toIcebergType(valueField, nextId);
    return valueField.isNullable()
        ? Types.MapType.ofOptional(keyId, valueId, keyType, valueType)
        : Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
  }

  private static Type toIcebergMap(
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field field, AtomicInteger nextId) {
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field entries =
        field.getChildren().get(0);
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field keyField =
        entries.getChildren().get(0);
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field valueField =
        entries.getChildren().get(1);
    int keyId = nextId.getAndIncrement();
    Type keyType = toIcebergType(keyField, nextId);
    int valueId = nextId.getAndIncrement();
    Type valueType = toIcebergType(valueField, nextId);
    return valueField.isNullable()
        ? Types.MapType.ofOptional(keyId, valueId, keyType, valueType)
        : Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
  }

  /**
   * Returns a copy of {@code arrowSchema} with every field annotated with the Iceberg id it carries
   * in {@code icebergSchema}, so readers can bind columns by id instead of by name.
   *
   * <p>Both schemas describe the same file, so they are walked in parallel: struct children match
   * by name, and list elements and map keys/values match positionally. A subtree whose names do not
   * line up is left unannotated and falls back to name-based binding.
   */
  public static org.apache.arrow.vector.types.pojo.Schema withFieldIds(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema, Schema icebergSchema) {
    return new org.apache.arrow.vector.types.pojo.Schema(
        withFieldIds(arrowSchema.getFields(), icebergSchema.asStruct()),
        arrowSchema.getCustomMetadata());
  }

  private static List<Field> withFieldIds(List<Field> arrowFields, Types.StructType struct) {
    ImmutableList.Builder<Field> annotated = ImmutableList.builder();
    for (Field arrowField : arrowFields) {
      Types.NestedField icebergField = struct.field(arrowField.getName());
      annotated.add(
          icebergField == null
              ? arrowField
              : withFieldId(arrowField, icebergField.fieldId(), icebergField.type()));
    }

    return annotated.build();
  }

  private static Field withFieldId(Field arrowField, int fieldId, Type icebergType) {
    Map<String, String> metadata =
        ImmutableMap.<String, String>builder()
            .putAll(arrowField.getMetadata())
            .put(FIELD_ID_KEY, String.valueOf(fieldId))
            .buildKeepingLast();

    return new Field(
        arrowField.getName(),
        new FieldType(
            arrowField.isNullable(), arrowField.getType(), arrowField.getDictionary(), metadata),
        annotatedChildren(arrowField, icebergType));
  }

  // Variant storage is an Iceberg-level encoding rather than nested Iceberg fields, so its Arrow
  // children carry no ids and are left alone.
  private static List<Field> annotatedChildren(Field arrowField, Type icebergType) {
    List<Field> children = arrowField.getChildren();
    if (children.isEmpty() || !icebergType.isNestedType()) {
      return children;
    }

    if (icebergType.isStructType()) {
      return withFieldIds(children, icebergType.asStructType());
    }

    if (icebergType.isListType()) {
      Types.ListType list = icebergType.asListType();
      return ImmutableList.of(withFieldId(children.get(0), list.elementId(), list.elementType()));
    }

    Types.MapType map = icebergType.asMapType();
    List<Field> entries = children.get(0).getChildren();
    if (entries.size() != 2) {
      return children;
    }

    Field annotatedEntries =
        new Field(
            children.get(0).getName(),
            children.get(0).getFieldType(),
            ImmutableList.of(
                withFieldId(entries.get(0), map.keyId(), map.keyType()),
                withFieldId(entries.get(1), map.valueId(), map.valueType())));
    return ImmutableList.of(annotatedEntries);
  }

  /**
   * Returns a copy of {@code arrowSchema} with every field annotated with the Iceberg id the {@code
   * mapping} gives it, for files that do not carry an Iceberg schema of their own.
   *
   * <p>Fields the mapping does not name are left unannotated, along with everything below them, and
   * fall back to name-based binding.
   */
  public static org.apache.arrow.vector.types.pojo.Schema withFieldIds(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema, NameMapping mapping) {
    return new org.apache.arrow.vector.types.pojo.Schema(
        withMappedIds(arrowSchema.getFields(), mapping.asMappedFields()),
        arrowSchema.getCustomMetadata());
  }

  private static List<Field> withMappedIds(List<Field> arrowFields, MappedFields mapping) {
    if (mapping == null) {
      return arrowFields;
    }

    ImmutableList.Builder<Field> annotated = ImmutableList.builder();
    for (Field arrowField : arrowFields) {
      annotated.add(withMappedId(arrowField, mapping));
    }

    return annotated.build();
  }

  private static Field withMappedId(Field arrowField, MappedFields mapping) {
    Integer id = mapping.id(arrowField.getName());
    if (id == null) {
      return arrowField;
    }

    MappedFields nested = mapping.field(id).nestedMapping();
    Map<String, String> metadata =
        ImmutableMap.<String, String>builder()
            .putAll(arrowField.getMetadata())
            .put(FIELD_ID_KEY, String.valueOf(id))
            .buildKeepingLast();

    return new Field(
        arrowField.getName(),
        new FieldType(
            arrowField.isNullable(), arrowField.getType(), arrowField.getDictionary(), metadata),
        mappedChildren(arrowField, nested));
  }

  /**
   * Name mappings name list elements {@code element} and map entries {@code key} and {@code value},
   * matching the Arrow child names, except that Arrow nests a map's key and value one level deeper
   * inside its {@code entries} struct.
   */
  private static List<Field> mappedChildren(Field arrowField, MappedFields nested) {
    List<Field> children = arrowField.getChildren();
    if (children.isEmpty() || nested == null || isVariantField(arrowField)) {
      return children;
    }

    if (arrowField.getType() instanceof ArrowType.Map) {
      Field entries = children.get(0);
      if (entries.getChildren().size() != 2) {
        return children;
      }

      return ImmutableList.of(
          new Field(
              entries.getName(),
              entries.getFieldType(),
              withMappedIds(entries.getChildren(), nested)));
    }

    return withMappedIds(children, nested);
  }

  /** Returns the Iceberg id {@link #withFieldIds} attached to {@code field}, or null. */
  public static Integer fieldId(Field field) {
    String id = field.getMetadata().get(FIELD_ID_KEY);
    if (id == null) {
      return null;
    }

    try {
      return Integer.valueOf(id);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Binds expected Iceberg fields to a file's Arrow fields.
   *
   * <p>When the file's fields carry Iceberg ids, binding is by id alone, so a column renamed since
   * the file was written still resolves and a newly added column that reuses an old name does not.
   * A file that carries no ids is bound by name instead.
   */
  public static final class FieldBinding {
    private final Map<Integer, Field> byId;
    private final Map<String, Field> byName;

    private FieldBinding(Map<Integer, Field> byId, Map<String, Field> byName) {
      this.byId = byId;
      this.byName = byName;
    }

    public static FieldBinding of(List<Field> fileFields) {
      Map<Integer, Field> byId = Maps.newHashMapWithExpectedSize(fileFields.size());
      Map<String, Field> byName = Maps.newHashMapWithExpectedSize(fileFields.size());
      for (Field field : fileFields) {
        byName.put(field.getName(), field);
        Integer id = fieldId(field);
        if (id != null) {
          byId.put(id, field);
        }
      }

      return new FieldBinding(byId, byName);
    }

    /** Returns the file field backing {@code expected}, or null when the file has no such field. */
    public Field resolve(Types.NestedField expected) {
      if (byId.isEmpty()) {
        return byName.get(expected.name());
      }

      Field matched = byId.get(expected.fieldId());
      if (matched != null) {
        return matched;
      }

      // Fields the file's Iceberg schema does not describe carry no id, so they can only be matched
      // by name: the synthetic _pos column the scan materializes, and any subtree withFieldIds
      // could not annotate. An id-carrying field is never matched by name, so a column added under
      // a name that used to belong to another column reads as missing rather than as that column.
      Field named = byName.get(expected.name());
      return named != null && fieldId(named) == null ? named : null;
    }

    /** Returns the file field with {@code name}, ignoring ids. For metadata columns. */
    public Field resolveByName(String name) {
      return byName.get(name);
    }
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
