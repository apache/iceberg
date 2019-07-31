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

package org.apache.iceberg.orc;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

public class TypeConversion {

  /**
   * Convert a given Iceberg schema to ORC.
   * @param schema the Iceberg schema to convert
   * @param columnIds an output with the column ids
   * @return the ORC schema
   */
  public static TypeDescription toOrc(Schema schema,
                                      ColumnIdMap columnIds) {
    return toOrc(null, schema.asStruct(), columnIds);
  }

  static TypeDescription toOrc(Integer fieldId,
                               Type type,
                               ColumnIdMap columnIds) {
    TypeDescription result;
    switch (type.typeId()) {
      case BOOLEAN:
        result = TypeDescription.createBoolean();
        break;
      case INTEGER:
        result = TypeDescription.createInt();
        break;
      case LONG:
        result = TypeDescription.createLong();
        break;
      case FLOAT:
        result = TypeDescription.createFloat();
        break;
      case DOUBLE:
        result = TypeDescription.createDouble();
        break;
      case DATE:
        result = TypeDescription.createDate();
        break;
      case TIME:
        result = TypeDescription.createInt();
        break;
      case TIMESTAMP:
        result = TypeDescription.createTimestamp();
        break;
      case STRING:
        result = TypeDescription.createString();
        break;
      case UUID:
        result = TypeDescription.createBinary();
        break;
      case FIXED:
        result = TypeDescription.createBinary();
        break;
      case BINARY:
        result = TypeDescription.createBinary();
        break;
      case DECIMAL: {
        Types.DecimalType decimal = (Types.DecimalType) type;
        result = TypeDescription.createDecimal()
            .withScale(decimal.scale())
            .withPrecision(decimal.precision());
        break;
      }
      case STRUCT: {
        result = TypeDescription.createStruct();
        for (Types.NestedField field : type.asStructType().fields()) {
          result.addField(field.name(), toOrc(field.fieldId(), field.type(), columnIds));
        }
        break;
      }
      case LIST: {
        Types.ListType list = (Types.ListType) type;
        result = TypeDescription.createList(toOrc(list.elementId(), list.elementType(),
            columnIds));
        break;
      }
      case MAP: {
        Types.MapType map = (Types.MapType) type;
        TypeDescription key = toOrc(map.keyId(), map.keyType(), columnIds);
        result = TypeDescription.createMap(key,
            toOrc(map.valueId(), map.valueType(), columnIds));
        break;
      }
      default:
        throw new IllegalArgumentException("Unhandled type " + type.typeId());
    }
    if (fieldId != null) {
      columnIds.put(result, fieldId);
    }
    return result;
  }

  /**
   * Convert an ORC schema to an Iceberg schema.
   * @param schema the ORC schema
   * @param columnIds the column ids
   * @return the Iceberg schema
   */
  public Schema fromOrc(TypeDescription schema, ColumnIdMap columnIds) {
    return new Schema(convertOrcToType(schema, columnIds).asStructType().fields());
  }

  Type convertOrcToType(TypeDescription schema, ColumnIdMap columnIds) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case BYTE:
      case SHORT:
      case INT:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
      case CHAR:
      case VARCHAR:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      case DATE:
        return Types.DateType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case DECIMAL:
        return Types.DecimalType.of(schema.getPrecision(), schema.getScale());
      case STRUCT: {
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> fieldTypes = schema.getChildren();
        List<Types.NestedField> fields = new ArrayList<>(fieldNames.size());
        for (int c = 0; c < fieldNames.size(); ++c) {
          String name = fieldNames.get(c);
          TypeDescription type = fieldTypes.get(c);
          fields.add(Types.NestedField.optional(columnIds.get(type), name,
              convertOrcToType(type, columnIds)));
        }
        return Types.StructType.of(fields);
      }
      case LIST: {
        TypeDescription child = schema.getChildren().get(0);
        return Types.ListType.ofOptional(columnIds.get(child),
            convertOrcToType(child, columnIds));
      }
      case MAP: {
        TypeDescription key = schema.getChildren().get(0);
        TypeDescription value = schema.getChildren().get(1);
        return Types.MapType.ofOptional(columnIds.get(key), columnIds.get(value),
            convertOrcToType(key, columnIds), convertOrcToType(value, columnIds));
      }
      default:
        // We don't have an answer for union types.
        throw new IllegalArgumentException("Can't handle " + schema);
    }
  }
}
