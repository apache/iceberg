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
package org.apache.iceberg.flink.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

@Internal
public class StructRowData implements RowData {
  private final Types.StructType type;
  private RowKind kind;
  private StructLike struct;

  public StructRowData(Types.StructType type) {
    this(type, RowKind.INSERT);
  }

  public StructRowData(Types.StructType type, RowKind kind) {
    this(type, null, kind);
  }

  private StructRowData(Types.StructType type, StructLike struct) {
    this(type, struct, RowKind.INSERT);
  }

  private StructRowData(Types.StructType type, StructLike struct, RowKind kind) {
    this.type = type;
    this.struct = struct;
    this.kind = kind;
  }

  public StructRowData setStruct(StructLike newStruct) {
    this.struct = newStruct;
    return this;
  }

  @Override
  public int getArity() {
    return struct.size();
  }

  @Override
  public RowKind getRowKind() {
    return kind;
  }

  @Override
  public void setRowKind(RowKind newKind) {
    Preconditions.checkNotNull(newKind, "kind can not be null");
    this.kind = newKind;
  }

  @Override
  public boolean isNullAt(int pos) {
    return struct.get(pos, Object.class) == null;
  }

  @Override
  public boolean getBoolean(int pos) {
    return struct.get(pos, Boolean.class);
  }

  @Override
  public byte getByte(int pos) {
    return (byte) (int) struct.get(pos, Integer.class);
  }

  @Override
  public short getShort(int pos) {
    return (short) (int) struct.get(pos, Integer.class);
  }

  @Override
  public int getInt(int pos) {
    Object integer = struct.get(pos, Object.class);

    if (integer instanceof Integer) {
      return (int) integer;
    } else if (integer instanceof LocalDate) {
      return (int) ((LocalDate) integer).toEpochDay();
    } else if (integer instanceof LocalTime) {
      return (int) (((LocalTime) integer).toNanoOfDay() / 1000_000);
    } else {
      throw new IllegalStateException(
          "Unknown type for int field. Type name: " + integer.getClass().getName());
    }
  }

  @Override
  public long getLong(int pos) {
    Object longVal = struct.get(pos, Object.class);

    if (longVal instanceof Long) {
      return (long) longVal;
    } else if (longVal instanceof OffsetDateTime) {
      return Duration.between(Instant.EPOCH, (OffsetDateTime) longVal).toNanos() / 1000;
    } else if (longVal instanceof LocalDate) {
      return ((LocalDate) longVal).toEpochDay();
    } else if (longVal instanceof LocalTime) {
      return ((LocalTime) longVal).toNanoOfDay();
    } else if (longVal instanceof LocalDateTime) {
      return Duration.between(Instant.EPOCH, ((LocalDateTime) longVal).atOffset(ZoneOffset.UTC))
              .toNanos()
          / 1000;
    } else {
      throw new IllegalStateException(
          "Unknown type for long field. Type name: " + longVal.getClass().getName());
    }
  }

  @Override
  public float getFloat(int pos) {
    return struct.get(pos, Float.class);
  }

  @Override
  public double getDouble(int pos) {
    return struct.get(pos, Double.class);
  }

  @Override
  public StringData getString(int pos) {
    return isNullAt(pos) ? null : getStringDataInternal(pos);
  }

  private StringData getStringDataInternal(int pos) {
    CharSequence seq = struct.get(pos, CharSequence.class);
    return StringData.fromString(seq.toString());
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return isNullAt(pos)
        ? null
        : DecimalData.fromBigDecimal(getDecimalInternal(pos), precision, scale);
  }

  private BigDecimal getDecimalInternal(int pos) {
    return struct.get(pos, BigDecimal.class);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    long timeLong = getLong(pos);
    return TimestampData.fromEpochMillis(timeLong / 1000, (int) (timeLong % 1000) * 1000);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public byte[] getBinary(int pos) {
    return isNullAt(pos) ? null : getBinaryInternal(pos);
  }

  private byte[] getBinaryInternal(int pos) {
    Object bytes = struct.get(pos, Object.class);

    // should only be either ByteBuffer or byte[]
    if (bytes instanceof ByteBuffer) {
      return ByteBuffers.toByteArray((ByteBuffer) bytes);
    } else if (bytes instanceof byte[]) {
      return (byte[]) bytes;
    } else if (bytes instanceof UUID) {
      UUID uuid = (UUID) bytes;
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    } else {
      throw new IllegalStateException(
          "Unknown type for binary field. Type name: " + bytes.getClass().getName());
    }
  }

  @Override
  public ArrayData getArray(int pos) {
    return isNullAt(pos)
        ? null
        : (ArrayData)
            convertValue(type.fields().get(pos).type().asListType(), struct.get(pos, List.class));
  }

  @Override
  public MapData getMap(int pos) {
    return isNullAt(pos)
        ? null
        : (MapData)
            convertValue(type.fields().get(pos).type().asMapType(), struct.get(pos, Map.class));
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return isNullAt(pos) ? null : getStructRowData(pos, numFields);
  }

  private StructRowData getStructRowData(int pos, int numFields) {
    return new StructRowData(
        type.fields().get(pos).type().asStructType(), struct.get(pos, StructLike.class));
  }

  private Object convertValue(Type elementType, Object value) {
    switch (elementType.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case DATE:
      case TIME:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return value;
      case TIMESTAMP:
        long millisecond = (long) value / 1000;
        int nanoOfMillisecond = (int) ((Long) value % 1000) * 1000;
        return TimestampData.fromEpochMillis(millisecond, nanoOfMillisecond);
      case STRING:
        return StringData.fromString(value.toString());
      case FIXED:
      case BINARY:
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case STRUCT:
        return new StructRowData(elementType.asStructType(), (StructLike) value);
      case LIST:
        List<Object> list = (List<Object>) value;
        Object[] array = new Object[list.size()];

        int index = 0;
        for (Object element : list) {
          if (element == null) {
            array[index] = null;
          } else {
            array[index] = convertValue(elementType.asListType().elementType(), element);
          }

          index += 1;
        }
        return new GenericArrayData(array);
      case MAP:
        Types.MapType mapType = elementType.asMapType();
        Set<? extends Map.Entry<?, ?>> entries = ((Map<?, ?>) value).entrySet();
        Map<Object, Object> result = Maps.newHashMap();
        for (Map.Entry<?, ?> entry : entries) {
          final Object keyValue = convertValue(mapType.keyType(), entry.getKey());
          final Object valueValue = convertValue(mapType.valueType(), entry.getValue());
          result.put(keyValue, valueValue);
        }

        return new GenericMapData(result);
      default:
        throw new UnsupportedOperationException("Unsupported element type: " + elementType);
    }
  }
}
