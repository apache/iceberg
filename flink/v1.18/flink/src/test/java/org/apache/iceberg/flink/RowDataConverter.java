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
package org.apache.iceberg.flink;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RowDataConverter {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private RowDataConverter() {}

  public static RowData convert(Schema iSchema, Record record) {
    return convert(iSchema.asStruct(), record);
  }

  private static RowData convert(Types.StructType struct, Record record) {
    GenericRowData rowData = new GenericRowData(struct.fields().size());
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);

      Type fieldType = field.type();
      rowData.setField(i, convert(fieldType, record.get(i)));
    }
    return rowData;
  }

  private static Object convert(Type type, Object object) {
    if (object == null) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case FIXED:
        return object;
      case DATE:
        return (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) object);
      case TIME:
        // Iceberg's time is in microseconds, while flink's time is in milliseconds.
        LocalTime localTime = (LocalTime) object;
        return (int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay());
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return TimestampData.fromInstant(((OffsetDateTime) object).toInstant());
        } else {
          return TimestampData.fromLocalDateTime((LocalDateTime) object);
        }
      case STRING:
        return StringData.fromString((String) object);
      case UUID:
        UUID uuid = (UUID) object;
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
      case BINARY:
        ByteBuffer buffer = (ByteBuffer) object;
        return Arrays.copyOfRange(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            buffer.arrayOffset() + buffer.remaining());
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return DecimalData.fromBigDecimal(
            (BigDecimal) object, decimalType.precision(), decimalType.scale());
      case STRUCT:
        return convert(type.asStructType(), (Record) object);
      case LIST:
        List<?> list = (List<?>) object;
        Object[] convertedArray = new Object[list.size()];
        for (int i = 0; i < convertedArray.length; i++) {
          convertedArray[i] = convert(type.asListType().elementType(), list.get(i));
        }
        return new GenericArrayData(convertedArray);
      case MAP:
        Map<Object, Object> convertedMap = Maps.newLinkedHashMap();
        Map<?, ?> map = (Map<?, ?>) object;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          convertedMap.put(
              convert(type.asMapType().keyType(), entry.getKey()),
              convert(type.asMapType().valueType(), entry.getValue()));
        }
        return new GenericMapData(convertedMap);
      default:
        throw new UnsupportedOperationException("Not a supported type: " + type);
    }
  }
}
