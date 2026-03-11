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
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.TypeUtil.CustomOrderSchemaVisitor;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/** Converts Iceberg Record to Spark InternalRow for testing. */
public class InternalRowConverter extends CustomOrderSchemaVisitor<Object> {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private Object currentValue;

  public static InternalRow convert(Schema schema, Record record) {
    InternalRowConverter internalRowConverter = new InternalRowConverter();
    internalRowConverter.currentValue = record;
    return (InternalRow) TypeUtil.visit(schema, internalRowConverter);
  }

  @Override
  public Object schema(Schema schema, Supplier<Object> structResult) {
    return structResult.get();
  }

  @Override
  public Object struct(Types.StructType struct, Iterable<Object> fieldResults) {
    GenericInternalRow internalRow = new GenericInternalRow(struct.fields().size());
    int i = 0;
    for (Object fieldResult : fieldResults) {
      internalRow.update(i, fieldResult);
      i++;
    }

    return internalRow;
  }

  @Override
  public Object field(Types.NestedField field, Supplier<Object> fieldResult) {
    Record record = (Record) currentValue;
    int position = record.struct().fields().indexOf(field);
    Object value = record.get(position);

    if (value == null) {
      return null;
    }
    currentValue = value;
    Object result = fieldResult.get();
    currentValue = record;
    return result;
  }

  @Override
  public Object list(Types.ListType list, Supplier<Object> elementResult) {
    List<?> listValue = (List<?>) currentValue;

    Object[] result = new Object[listValue.size()];
    for (int i = 0; i < listValue.size(); i += 1) {
      currentValue = listValue.get(i);
      result[i] = elementResult.get();
    }

    return new GenericArrayData(result);
  }

  @Override
  public Object map(Types.MapType map, Supplier<Object> keyResult, Supplier<Object> valueResult) {
    Map<?, ?> mapValue = (Map<?, ?>) currentValue;
    Object[] keysArray = new Object[mapValue.size()];
    Object[] valuesArray = new Object[mapValue.size()];
    int idx = 0;
    for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
      currentValue = entry.getKey();
      keysArray[idx] = keyResult.get();

      currentValue = entry.getValue();
      valuesArray[idx] = valueResult.get();
      idx++;
    }
    return new ArrayBasedMapData(
        new GenericArrayData(keysArray), new GenericArrayData(valuesArray));
  }

  @Override
  public Object primitive(Type.PrimitiveType primitive) {
    Object value = currentValue;

    if (value == null) {
      return null;
    }

    switch (primitive.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value;
      case DATE:
        return (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) value);
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) primitive;
        if (timestampType.shouldAdjustToUTC()) {
          return ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) value);
        } else {
          return ChronoUnit.MICROS.between(EPOCH, ((LocalDateTime) value).atZone(ZoneId.of("UTC")));
        }
      case STRING:
        return UTF8String.fromString((String) value);
      case UUID:
        return UTF8String.fromString(value.toString());
      case FIXED:
      case BINARY:
        ByteBuffer buffer = (ByteBuffer) value;
        return Arrays.copyOfRange(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            buffer.arrayOffset() + buffer.remaining());
      case DECIMAL:
        return Decimal.apply((BigDecimal) value);
      case VARIANT:
      case TIME:
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for conversion to InternalRow: " + primitive);
    }
  }
}
