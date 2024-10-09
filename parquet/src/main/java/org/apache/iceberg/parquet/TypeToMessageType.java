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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type.NestedType;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class TypeToMessageType {
  public static final int DECIMAL_INT32_MAX_DIGITS = 9;
  public static final int DECIMAL_INT64_MAX_DIGITS = 18;
  private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();
  private static final LogicalTypeAnnotation DATE = LogicalTypeAnnotation.dateType();
  private static final LogicalTypeAnnotation TIME_MICROS =
      LogicalTypeAnnotation.timeType(false /* not adjusted to UTC */, TimeUnit.MICROS);
  private static final LogicalTypeAnnotation TIMESTAMP_MICROS =
      LogicalTypeAnnotation.timestampType(false /* not adjusted to UTC */, TimeUnit.MICROS);
  private static final LogicalTypeAnnotation TIMESTAMPTZ_MICROS =
      LogicalTypeAnnotation.timestampType(true /* adjusted to UTC */, TimeUnit.MICROS);

  public MessageType convert(Schema schema, String name) {
    Types.MessageTypeBuilder builder = Types.buildMessage();

    for (NestedField field : schema.columns()) {
      builder.addField(field(field));
    }

    return builder.named(AvroSchemaUtil.makeCompatibleName(name));
  }

  public GroupType struct(StructType struct, Type.Repetition repetition, int id, String name) {
    Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);

    for (NestedField field : struct.fields()) {
      builder.addField(field(field));
    }

    return builder.id(id).named(AvroSchemaUtil.makeCompatibleName(name));
  }

  public Type field(NestedField field) {
    Type.Repetition repetition =
        field.isOptional() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    int id = field.fieldId();
    String name = field.name();

    if (field.type().isPrimitiveType()) {
      return primitive(field.type().asPrimitiveType(), repetition, id, name);

    } else {
      NestedType nested = field.type().asNestedType();
      if (nested.isStructType()) {
        return struct(nested.asStructType(), repetition, id, name);
      } else if (nested.isMapType()) {
        return map(nested.asMapType(), repetition, id, name);
      } else if (nested.isListType()) {
        return list(nested.asListType(), repetition, id, name);
      }
      throw new UnsupportedOperationException("Can't convert unknown type: " + nested);
    }
  }

  public GroupType list(ListType list, Type.Repetition repetition, int id, String name) {
    NestedField elementField = list.fields().get(0);
    return Types.list(repetition)
        .element(field(elementField))
        .id(id)
        .named(AvroSchemaUtil.makeCompatibleName(name));
  }

  public GroupType map(MapType map, Type.Repetition repetition, int id, String name) {
    NestedField keyField = map.fields().get(0);
    NestedField valueField = map.fields().get(1);
    return Types.map(repetition)
        .key(field(keyField))
        .value(field(valueField))
        .id(id)
        .named(AvroSchemaUtil.makeCompatibleName(name));
  }

  public Type primitive(
      PrimitiveType primitive, Type.Repetition repetition, int id, String originalName) {
    String name = AvroSchemaUtil.makeCompatibleName(originalName);
    switch (primitive.typeId()) {
      case BOOLEAN:
        return Types.primitive(BOOLEAN, repetition).id(id).named(name);
      case INTEGER:
        return Types.primitive(INT32, repetition).id(id).named(name);
      case LONG:
        return Types.primitive(INT64, repetition).id(id).named(name);
      case FLOAT:
        return Types.primitive(FLOAT, repetition).id(id).named(name);
      case DOUBLE:
        return Types.primitive(DOUBLE, repetition).id(id).named(name);
      case DATE:
        return Types.primitive(INT32, repetition).as(DATE).id(id).named(name);
      case TIME:
        return Types.primitive(INT64, repetition).as(TIME_MICROS).id(id).named(name);
      case TIMESTAMP:
        if (((TimestampType) primitive).shouldAdjustToUTC()) {
          return Types.primitive(INT64, repetition).as(TIMESTAMPTZ_MICROS).id(id).named(name);
        } else {
          return Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).id(id).named(name);
        }
      case STRING:
        return Types.primitive(BINARY, repetition).as(STRING).id(id).named(name);
      case BINARY:
        return Types.primitive(BINARY, repetition).id(id).named(name);
      case FIXED:
        FixedType fixed = (FixedType) primitive;

        return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .length(fixed.length())
            .id(id)
            .named(name);

      case DECIMAL:
        DecimalType decimal = (DecimalType) primitive;

        if (decimal.precision() <= DECIMAL_INT32_MAX_DIGITS) {
          // store as an int
          return Types.primitive(INT32, repetition)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .id(id)
              .named(name);

        } else if (decimal.precision() <= DECIMAL_INT64_MAX_DIGITS) {
          // store as a long
          return Types.primitive(INT64, repetition)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .id(id)
              .named(name);

        } else {
          // store as a fixed-length array
          int minLength = TypeUtil.decimalRequiredBytes(decimal.precision());
          return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
              .length(minLength)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .id(id)
              .named(name);
        }

      case UUID:
        return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .id(id)
            .named(name);

      case GEOMETRY:
        GeometryType geomType = (GeometryType) primitive;
        switch (geomType.encoding()) {
          case EWKB:
          case WKB:
            return Types.primitive(BINARY, repetition).id(id).named(name);
          case GEOJSON:
          case WKT:
            return Types.primitive(BINARY, repetition)
                .as(LogicalTypeAnnotation.stringType())
                .id(id)
                .named(name);
          default:
            throw new UnsupportedOperationException("Unsupported geometry encoding: " + geomType);
        }

      default:
        throw new UnsupportedOperationException("Unsupported type for Parquet: " + primitive);
    }
  }

  private static LogicalTypeAnnotation decimalAnnotation(int precision, int scale) {
    return LogicalTypeAnnotation.decimalType(scale, precision);
  }
}
