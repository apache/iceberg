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
package org.apache.iceberg.spark.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/** Converts Iceberg Record to Spark InternalRow for testing. */
public class InternalRowConverter {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private InternalRowConverter() {}

  public static InternalRow convert(Schema schema, Record record) {
    return convert(schema.asStruct(), record);
  }

  private static InternalRow convert(Types.StructType struct, Record record) {
    GenericInternalRow internalRow = new GenericInternalRow(struct.fields().size());
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);

      Type fieldType = field.type();
      internalRow.update(i, convert(fieldType, record.get(i)));
    }

    return internalRow;
  }

  private static Object convert(Type type, Object value) {
    if (value == null) {
      return null;
    }

    return switch (type.typeId()) {
      case BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE -> value;
      case DATE -> (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) value);
      case TIMESTAMP ->
          ((Types.TimestampType) type).shouldAdjustToUTC()
              ? ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) value)
              : ChronoUnit.MICROS.between(EPOCH, ((LocalDateTime) value).atZone(ZoneId.of("UTC")));
      case STRING -> UTF8String.fromString((String) value);
      case UUID -> UTF8String.fromString(value.toString());
      case FIXED, BINARY -> {
        ByteBuffer buffer = (ByteBuffer) value;
        yield Arrays.copyOfRange(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            buffer.arrayOffset() + buffer.remaining());
      }
      case DECIMAL -> Decimal.apply((BigDecimal) value);
      case STRUCT -> convert((Types.StructType) type, (Record) value);
      case LIST ->
          new GenericArrayData(
              ((List<?>) value)
                  .stream()
                      .map(element -> convert(type.asListType().elementType(), element))
                      .toArray());
      case MAP ->
          new ArrayBasedMapData(
              new GenericArrayData(
                  ((Map<?, ?>) value)
                      .keySet().stream()
                          .map(o -> convert(type.asMapType().keyType(), o))
                          .toArray()),
              new GenericArrayData(
                  ((Map<?, ?>) value)
                      .values().stream()
                          .map(o -> convert(type.asMapType().valueType(), o))
                          .toArray()));
        // TIME is not supported by Spark, VARIANT not yet implemented
      default ->
          throw new UnsupportedOperationException(
              "Unsupported type for conversion to InternalRow: " + type);
    };
  }
}
