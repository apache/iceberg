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

import dev.vortex.api.DType;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public final class VortexSchemas {
  private VortexSchemas() {}

  /**
   * Given a projection schema, and a Vortex DType, return a resolved Iceberg schema that represents
   * how to read the Vortex data as Iceberg rows.
   */
  public static Schema convert(DType fileSchema) {
    Preconditions.checkArgument(
        fileSchema.getVariant() == DType.Variant.STRUCT,
        "only Vortex STRUCT types can be converted to Iceberg Schema, received: " + fileSchema);
    List<String> fieldNames = fileSchema.getFieldNames();
    List<DType> fieldTypes = fileSchema.getFieldTypes();

    List<Types.NestedField> targetSchema = Lists.newArrayList();

    for (int fieldId = 0; fieldId < fieldNames.size(); fieldId++) {
      String fieldName = fieldNames.get(fieldId);
      DType fieldType = fieldTypes.get(fieldId);
      Type icebergType = toIcebergType(fieldType);
      if (fieldType.isNullable()) {
        targetSchema.add(optional(fieldId, fieldName, icebergType));
      } else {
        targetSchema.add(required(fieldId, fieldName, icebergType));
      }
    }

    return new Schema(targetSchema);
  }

  /** Convert an Iceberg Schema to a Vortex DType suitable for {@code VortexWriter.create}. */
  public static DType toDType(Schema icebergSchema) {
    List<Types.NestedField> columns = icebergSchema.columns();
    String[] names = new String[columns.size()];
    DType[] types = new DType[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Types.NestedField field = columns.get(i);
      names[i] = field.name();
      types[i] = toVortexDType(field.type(), field.isOptional());
    }

    return DType.newStruct(names, types, false);
  }

  private static DType toVortexDType(Type type, boolean nullable) {
    return switch (type.typeId()) {
      case BOOLEAN -> DType.newBool(nullable);
      case INTEGER -> DType.newInt(nullable);
      case LONG -> DType.newLong(nullable);
      case FLOAT -> DType.newFloat(nullable);
      case DOUBLE -> DType.newDouble(nullable);
      case STRING -> DType.newUtf8(nullable);
      case BINARY, FIXED -> DType.newBinary(nullable);
      case DECIMAL -> {
        Types.DecimalType decimal = (Types.DecimalType) type;
        yield DType.newDecimal(decimal.precision(), decimal.scale(), nullable);
      }
      case DATE -> DType.newDate(DType.TimeUnit.DAYS, nullable);
      case TIME -> DType.newTime(DType.TimeUnit.MICROSECONDS, nullable);
      case TIMESTAMP -> {
        Types.TimestampType ts = (Types.TimestampType) type;
        yield DType.newTimestamp(
            DType.TimeUnit.MICROSECONDS,
            ts.shouldAdjustToUTC() ? Optional.of("UTC") : Optional.empty(),
            nullable);
      }
      case TIMESTAMP_NANO -> {
        Types.TimestampNanoType tsNano = (Types.TimestampNanoType) type;
        yield DType.newTimestamp(
            DType.TimeUnit.NANOSECONDS,
            tsNano.shouldAdjustToUTC() ? Optional.of("UTC") : Optional.empty(),
            nullable);
      }
      case LIST -> {
        Types.ListType listType = (Types.ListType) type;
        DType elementDType = toVortexDType(listType.elementType(), listType.isElementOptional());
        yield DType.newList(elementDType, nullable);
      }
      case STRUCT -> {
        Types.StructType structType = (Types.StructType) type;
        List<Types.NestedField> fields = structType.fields();
        String[] fieldNames = new String[fields.size()];
        DType[] fieldTypes = new DType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          fieldNames[i] = fields.get(i).name();
          fieldTypes[i] = toVortexDType(fields.get(i).type(), fields.get(i).isOptional());
        }

        yield DType.newStruct(fieldNames, fieldTypes, nullable);
      }
      default ->
          throw new UnsupportedOperationException(
              "Unsupported Iceberg type for Vortex write: " + type);
    };
  }

  /** Convert an Iceberg Schema to an Arrow Schema for writing via Arrow IPC. */
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

  private static Type toIcebergType(DType dataType) {
    switch (dataType.getVariant()) {
      case NULL:
        return Types.UnknownType.get();
      case BOOL:
        return Types.BooleanType.get();
      case PRIMITIVE_U8:
      case PRIMITIVE_U16:
      case PRIMITIVE_U32:
      case PRIMITIVE_I8:
      case PRIMITIVE_I16:
      case PRIMITIVE_I32:
        return Types.IntegerType.get();
      case PRIMITIVE_U64:
      case PRIMITIVE_I64:
        return Types.LongType.get();
      case PRIMITIVE_F32:
        return Types.FloatType.get();
      case PRIMITIVE_F64:
        return Types.DoubleType.get();
      case DECIMAL:
        return Types.DecimalType.of(dataType.getPrecision(), dataType.getScale());
      case UTF8:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      case LIST:
        {
          DType elementType = dataType.getElementType();
          Type innerType = toIcebergType(elementType);
          if (elementType.isNullable()) {
            return Types.ListType.ofOptional(0, innerType);
          } else {
            return Types.ListType.ofRequired(0, innerType);
          }
        }
      case EXTENSION:
        {
          if (dataType.isDate()) {
            return Types.DateType.get();
          } else if (dataType.isTime()) {
            return Types.TimeType.get();
          } else if (dataType.isTimestamp()) {
            if (dataType.getTimeZone().isPresent()) {
              return Types.TimestampType.withZone();
            } else {
              return Types.TimestampType.withoutZone();
            }
          } else {
            throw new UnsupportedOperationException(
                "Unsupported Vortex extension type: " + dataType);
          }
        }
        // TODO(aduffy): add nested struct support
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type in Vortex -> Iceberg conversion: " + dataType.getVariant());
    }
  }
}
