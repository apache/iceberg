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

import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class RemoveIds {
  public static final int DECIMAL_INT32_MAX_DIGITS = 9;
  public static final int DECIMAL_INT64_MAX_DIGITS = 18;
  private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();
  private static final LogicalTypeAnnotation DATE = LogicalTypeAnnotation.dateType();
  private static final LogicalTypeAnnotation TIME_MICROS = LogicalTypeAnnotation
      .timeType(false /* not adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.MICROS);
  private static final LogicalTypeAnnotation TIMESTAMP_MICROS = LogicalTypeAnnotation
      .timestampType(false /* not adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.MICROS);
  private static final LogicalTypeAnnotation TIMESTAMPTZ_MICROS = LogicalTypeAnnotation
      .timestampType(true /* adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.MICROS);

  GroupType struct(StructType struct, Type.Repetition repetition, String name) {
    Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);

    for (NestedField field : struct.fields()) {
      builder.addField(field(field));
    }

    return builder.named(AvroSchemaUtil.makeCompatibleName(name));
  }

  GroupType list(ListType list, Type.Repetition repetition, String name) {
    NestedField elementField = list.fields().get(0);
    return Types.list(repetition)
        .element(field(elementField))
        .named(AvroSchemaUtil.makeCompatibleName(name));
  }

  GroupType map(MapType map, Type.Repetition repetition, String name) {
    NestedField keyField = map.fields().get(0);
    NestedField valueField = map.fields().get(1);
    return Types.map(repetition)
        .key(field(keyField))
        .value(field(valueField))
        .named(AvroSchemaUtil.makeCompatibleName(name));
  }

  Type primitive(PrimitiveType primitive, Type.Repetition repetition, String originalName) {
    String name = AvroSchemaUtil.makeCompatibleName(originalName);
    switch (primitive.typeId()) {
      case BOOLEAN:
        return Types.primitive(BOOLEAN, repetition).named(name);
      case INTEGER:
        return Types.primitive(INT32, repetition).named(name);
      case LONG:
        return Types.primitive(INT64, repetition).named(name);
      case FLOAT:
        return Types.primitive(FLOAT, repetition).named(name);
      case DOUBLE:
        return Types.primitive(DOUBLE, repetition).named(name);
      case DATE:
        return Types.primitive(INT32, repetition).as(DATE).named(name);
      case TIME:
        return Types.primitive(INT64, repetition).as(TIME_MICROS).named(name);
      case TIMESTAMP:
        if (((TimestampType) primitive).shouldAdjustToUTC()) {
          return Types.primitive(INT64, repetition).as(TIMESTAMPTZ_MICROS).named(name);
        } else {
          return Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).named(name);
        }
      case STRING:
        return Types.primitive(BINARY, repetition).as(STRING).named(name);
      case BINARY:
        return Types.primitive(BINARY, repetition).named(name);
      case FIXED:
        org.apache.iceberg.types.Types.FixedType fixed = (org.apache.iceberg.types.Types.FixedType) primitive;

        return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(fixed.length()).named(name);

      case DECIMAL:
        org.apache.iceberg.types.Types.DecimalType decimal = (org.apache.iceberg.types.Types.DecimalType) primitive;

        if (decimal.precision() <= DECIMAL_INT32_MAX_DIGITS) {
          // store as an int
          return Types.primitive(INT32, repetition)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .named(name);

        } else if (decimal.precision() <= DECIMAL_INT64_MAX_DIGITS) {
          // store as a long
          return Types.primitive(INT64, repetition)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .named(name);

        } else {
          // store as a fixed-length array
          int minLength = TypeUtil.decimalRequiredBytes(decimal.precision());
          return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(minLength)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .named(name);
        }

      case UUID:
        return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(16).named(name);

      default:
        throw new UnsupportedOperationException("Unsupported type for Parquet: " + primitive);
    }
  }

  Type field(org.apache.iceberg.types.Types.NestedField field) {
    Type.Repetition repetition = field.isOptional() ?
        Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    String name = field.name();

    if (field.type().isPrimitiveType()) {
      return primitive(field.type().asPrimitiveType(), repetition, name);

    } else {
      org.apache.iceberg.types.Type.NestedType nested = field.type().asNestedType();
      if (nested.isStructType()) {
        return struct(nested.asStructType(), repetition, name);
      } else if (nested.isMapType()) {
        return map(nested.asMapType(), repetition, name);
      } else if (nested.isListType()) {
        return list(nested.asListType(), repetition, name);
      }
      throw new UnsupportedOperationException("Can't convert unknown type: " + nested);
    }
  }

  private static LogicalTypeAnnotation decimalAnnotation(int precision, int scale) {
    return LogicalTypeAnnotation.decimalType(scale, precision);
  }

  public MessageType removeIds(org.apache.iceberg.Schema schema, String name) {
    Types.MessageTypeBuilder builder = Types.buildMessage();

    for (org.apache.iceberg.types.Types.NestedField field : schema.columns()) {
      builder.addField(field(field));
    }

    return builder.named(AvroSchemaUtil.makeCompatibleName(name));
  }

}
