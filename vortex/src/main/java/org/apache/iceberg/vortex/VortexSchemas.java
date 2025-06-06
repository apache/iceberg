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
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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

  private static Type toIcebergType(DType data_type) {
    switch (data_type.getVariant()) {
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
        return Types.DecimalType.of(data_type.getPrecision(), data_type.getScale());
      case UTF8:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      case LIST:
        {
          DType elementType = data_type.getElementType();
          Type innerType = toIcebergType(elementType);
          if (elementType.isNullable()) {
            return Types.ListType.ofOptional(0, innerType);
          } else {
            return Types.ListType.ofRequired(0, innerType);
          }
        }
      case EXTENSION:
        {
          if (data_type.isDate()) {
            return Types.DateType.get();
          } else if (data_type.isTime()) {
            return Types.TimeType.get();
          } else if (data_type.isTimestamp()) {
            if (data_type.getTimeZone().isPresent()) {
              return Types.TimestampType.withZone();
            } else {
              return Types.TimestampType.withoutZone();
            }
          } else {
            throw new UnsupportedOperationException(
                "Unsupported Vortex extension type: " + data_type);
          }
        }
        // TODO(aduffy): add nested struct support
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type in Vortex -> Iceberg conversion: " + data_type.getVariant());
    }
  }
}
