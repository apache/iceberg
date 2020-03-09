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

package org.apache.iceberg.flink.connector.sink;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

public class MapAvroSerializer implements AvroSerializer<Map<String, Object>> {

  private static final Schema STRINGS = Schema.create(Schema.Type.STRING);

  private static final MapAvroSerializer INSTANCE = new MapAvroSerializer();

  public static MapAvroSerializer getInstance() {
    return INSTANCE;
  }

  @Override
  public GenericRecord serialize(Map<String, Object> record, Schema avroSchema) {
    return convert(record, avroSchema);
  }

  private GenericRecord convert(Map<String, Object> record, Schema avroSchema) throws AvroTypeException {
    final GenericRecord avroRecord = new GenericData.Record(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      final String fieldName = field.name();
      final Schema actualSchema = AvroUtils.getActualSchema(field.schema());
      final boolean isOptional = AvroUtils.isOptional(field.schema());
      final Object value = record.get(fieldName);
      if (value == null) {
        if (isOptional) {
          continue;
        } else {
          throw new AvroTypeException("value is null for required field: " + fieldName);
        }
      }
      final Object fieldValue = convertFieldValue(value, fieldName, actualSchema);
      avroRecord.put(fieldName, fieldValue);
    }
    return avroRecord;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  // modeled after GenericData#deepCopyRaw method
  private Object convertFieldValue(Object value, String fieldName, Schema fieldSchema) {
    Object fieldValue;
    switch (fieldSchema.getType()) {
      case INT:
        if (value instanceof Integer) {
          fieldValue = value;
          break;
        } else {
          throw new AvroTypeException(String.format(
              "incompatible type for field %s: expected = %s, actual = %s",
              fieldName, fieldSchema.getType().getName(), value.getClass().getSimpleName()));
        }
      case LONG:
        if (value instanceof Integer) {
          fieldValue = Long.valueOf(((Integer) value).longValue());
          break;
        } else if (value instanceof Long) {
          fieldValue = value;
          break;
        } else {
          throw new AvroTypeException(String.format(
              "incompatible type for field %s: expected = %s, actual = %s",
              fieldName, fieldSchema.getType().getName(), value.getClass().getSimpleName()));
        }
      case FLOAT:
        if (value instanceof Integer) {
          fieldValue = ((Integer) value).floatValue();
          break;
        } else if (value instanceof Long) {
          fieldValue = ((Long) value).floatValue();
          break;
        } else if (value instanceof Float) {
          fieldValue = value;
          break;
        } else {
          throw new AvroTypeException(String.format(
              "incompatible type for field %s: expected = %s, actual = %s",
              fieldName, fieldSchema.getType().getName(), value.getClass().getSimpleName()));
        }
      case DOUBLE:
        if (value instanceof Integer) {
          fieldValue = ((Integer) value).doubleValue();
          break;
        } else if (value instanceof Long) {
          fieldValue = ((Long) value).doubleValue();
          break;
        } else if (value instanceof Float) {
          fieldValue = ((Float) value).doubleValue();
          break;
        } else if (value instanceof Double) {
          fieldValue = value;
          break;
        } else {
          throw new AvroTypeException(String.format(
              "incompatible type for field %s: expected = %s, actual = %s",
              fieldName, fieldSchema.getType().getName(), value.getClass().getSimpleName()));
        }
      case BOOLEAN:
        if (value instanceof Boolean) {
          fieldValue = value;
          break;
        } else {
          throw new AvroTypeException(String.format(
              "incompatible type for field %s: expected = %s, actual = %s",
              fieldName, fieldSchema.getType().getName(), value.getClass().getSimpleName()));
        }
      case NULL:
        // primitive types
        fieldValue = value;
        break;
      case STRING:
        if (value instanceof String) {
          fieldValue = new Utf8((String) value);
        } else if (value instanceof CharSequence) {
          fieldValue = new Utf8(value.toString());
        } else {
          throw new AvroTypeException(String.format(
              "incompatible type for field %s: expected = %s, actual = %s",
              fieldName, fieldSchema.getType().getName(), value.getClass().getSimpleName()));
        }
        break;
      case RECORD:
        if (!(value instanceof Map)) {
          throw new AvroTypeException(String.format("field %s has non-map value type: %s",
              fieldName, value.getClass().getCanonicalName()));
        }
        Map<String, Object> recordMap = (Map<String, Object>) value;
        fieldValue = convert(recordMap, fieldSchema);
        break;
      case ARRAY:
        if (!(value instanceof List)) {
          throw new AvroTypeException(String.format("array field %s has non-list value type: %s",
              fieldName, value.getClass().getCanonicalName()));
        }
        final Schema actualSchema = AvroUtils.getActualSchema(fieldSchema.getElementType());
        final boolean isOptional = AvroUtils.isOptional(fieldSchema.getElementType());
        final List<Object> valueList = (List<Object>) value;
        final GenericArray<Object> avroArray = new GenericData.Array<>(valueList.size(), fieldSchema);
        for (Object v : valueList) {
          final Object elemValue;
          if (v == null) {
            if (isOptional) {
              elemValue = null;
            } else {
              throw new AvroTypeException("array element is null: " + fieldName);
            }
          } else {
            elemValue = convertFieldValue(v, fieldName, actualSchema);
          }
          avroArray.add(elemValue);
        }
        fieldValue = avroArray;
        break;
      case ENUM:
        fieldValue = GenericData.get().createEnum(value.toString(), fieldSchema);
        break;
      case BYTES:
        byte[] bytesArray = (byte[]) value;
        ByteBuffer bb = ByteBuffer.wrap(bytesArray);
        fieldValue = bb;
        break;
      case FIXED:
        fieldValue = GenericData.get().createFixed(null, (byte[]) value, fieldSchema);
        break;
      case MAP:
        Map<CharSequence, Object> mapValue = (Map) value;
        Map<CharSequence, Object> mapCopy =
            new HashMap<>(mapValue.size());
        for (Map.Entry<CharSequence, Object> entry : mapValue.entrySet()) {
          mapCopy.put(
              GenericData.get().deepCopy(STRINGS, entry.getKey()),
              GenericData.get().deepCopy(fieldSchema.getValueType(), entry.getValue()));
        }
        fieldValue = mapCopy;
        break;
      default:
        throw new AvroTypeException(String.format("field %s has non-supported  type: %s",
            fieldName, fieldSchema.getType()));
    }
    return fieldValue;
  }
}
