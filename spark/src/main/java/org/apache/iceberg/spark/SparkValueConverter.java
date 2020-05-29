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

package org.apache.iceberg.spark;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

/**
 * A utility class that converts Spark values to Iceberg's internal representation.
 */
public class SparkValueConverter {

  private SparkValueConverter() {}

  public static Record convert(Schema schema, Row row) {
    return convert(schema.asStruct(), row);
  }

  public static Object convert(Type type, Object object) {
    if (object == null) {
      return null;
    }

    switch (type.typeId()) {
      case STRUCT:
        return convert(type.asStructType(), (Row) object);

      case LIST:
        List<Object> convertedList = Lists.newArrayList();
        List<?> list = (List<?>) object;
        for (Object element : list) {
          convertedList.add(convert(type.asListType().elementType(), element));
        }
        return convertedList;

      case MAP:
        Map<Object, Object> convertedMap = Maps.newLinkedHashMap();
        Map<?, ?> map = (Map<?, ?>) object;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          convertedMap.put(
              convert(type.asMapType().keyType(), entry.getKey()),
              convert(type.asMapType().valueType(), entry.getValue()));
        }
        return convertedMap;

      case DATE:
        return DateTimeUtils.fromJavaDate((Date) object);
      case TIMESTAMP:
        return DateTimeUtils.fromJavaTimestamp((Timestamp) object);
      case BINARY:
        return ByteBuffer.wrap((byte[]) object);
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case STRING:
      case FIXED:
        return object;
      default:
        throw new UnsupportedOperationException("Not a supported type: " + type);
    }
  }

  private static Record convert(Types.StructType struct, Row row) {
    Record record = GenericRecord.create(struct);
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);

      Type fieldType = field.type();

      switch (fieldType.typeId()) {
        case STRUCT:
          record.set(i, convert(fieldType.asStructType(), row.getStruct(i)));
          break;
        case LIST:
          record.set(i, convert(fieldType.asListType(), row.getList(i)));
          break;
        case MAP:
          record.set(i, convert(fieldType.asMapType(), row.getJavaMap(i)));
          break;
        default:
          record.set(i, convert(fieldType, row.get(i)));
      }
    }
    return record;
  }
}
