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
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
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
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
    if (precision == 6) {
      int nanosOfMillisecond = (int) (timeLong % 1000);
      return TimestampData.fromEpochMillis(timeLong / 1000, nanosOfMillisecond);
    } else {
      return TimestampData.fromEpochMillis(timeLong);
    }
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
    } else {
      throw new IllegalStateException(
          "Unknown type for binary field. Type name: " + bytes.getClass().getName());
    }
  }

  @Override
  public ArrayData getArray(int pos) {
    return isNullAt(pos) ? null : getArrayInternal(pos);
  }

  private ArrayData getArrayInternal(int pos) {
    return collectionToArrayData(
        type.fields().get(pos).type().asListType().elementType(),
        struct.get(pos, List.class));
  }

  @Override
  public MapData getMap(int pos) {
    return isNullAt(pos) ? null : getMapInternal(pos);
  }

  private MapData getMapInternal(int pos) {
    Types.MapType mapType = type.fields().get(pos).type().asMapType();
    // make a defensive copy to ensure entries do not change
    List<Map.Entry<?, ?>> entries =
        ImmutableList.copyOf(((Map<?, ?>) struct.get(pos, Map.class)).entrySet());

    ArrayData keyArray =
        collectionToArrayData(mapType.keyType(), Lists.transform(entries, Map.Entry::getKey));

    ArrayData valueArray =
        collectionToArrayData(mapType.valueType(), Lists.transform(entries, Map.Entry::getValue));

    ArrayData.ElementGetter keyGetter =
        ArrayData.createElementGetter(FlinkSchemaUtil.convert(mapType.keyType()));
    ArrayData.ElementGetter valueGetter =
        ArrayData.createElementGetter(FlinkSchemaUtil.convert(mapType.valueType()));

    int length = keyArray.size();
    Map<Object, Object> result = Maps.newHashMap();
    for (int pos1 = 0; pos1 < length; pos1++) {
      final Object keyValue = keyGetter.getElementOrNull(keyArray, pos1);
      final Object valueValue = valueGetter.getElementOrNull(valueArray, pos1);

      result.put(keyValue, valueValue);
    }
    return new GenericMapData(result);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return isNullAt(pos) ? null : getStructRowData(pos, numFields);
  }

  private StructRowData getStructRowData(int pos, int numFields) {
    return new StructRowData(
        type.fields().get(pos).type().asStructType(), struct.get(pos, StructLike.class));
  }

  private ArrayData collectionToArrayData(Type elementType, Collection<?> values) {
    switch (elementType.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case DATE:
      case TIME:
      case LONG:
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
        return fillArray(values, array -> (pos, value) -> array[pos] = value);
      case STRING:
        return fillArray(
            values,
            array ->
                (BiConsumer<Integer, CharSequence>)
                    (pos, seq) -> array[pos] = StringData.fromString(seq.toString()));
      case FIXED:
      case BINARY:
        return fillArray(
            values,
            array ->
                (BiConsumer<Integer, ByteBuffer>)
                    (pos, buf) -> array[pos] = ByteBuffers.toByteArray(buf));
      case DECIMAL:
        return fillArray(
            values, array -> (BiConsumer<Integer, BigDecimal>) (pos, dec) -> array[pos] = dec);
      case STRUCT:
        return fillArray(
            values,
            array ->
                (BiConsumer<Integer, StructLike>)
                    (pos, tuple) ->
                        array[pos] = new StructRowData(elementType.asStructType(), tuple));
      case LIST:
        return fillArray(
            values,
            array ->
                (BiConsumer<Integer, Collection<?>>)
                    (pos, list) ->
                        array[pos] =
                            collectionToArrayData(elementType.asListType().elementType(), list));
      case MAP:
        return fillArray(
            values,
            array ->
                (BiConsumer<Integer, Map<?, ?>>)
                    (pos, map) -> array[pos] = new GenericMapData(map));
      default:
        throw new UnsupportedOperationException("Unsupported array element type: " + elementType);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> GenericArrayData fillArray(
      Collection<?> values, Function<Object[], BiConsumer<Integer, T>> makeSetter) {
    Object[] array = new Object[values.size()];
    BiConsumer<Integer, T> setter = makeSetter.apply(array);

    int index = 0;
    for (Object value : values) {
      if (value == null) {
        array[index] = null;
      } else {
        setter.accept(index, (T) value);
      }

      index += 1;
    }

    return new GenericArrayData(array);
  }
}
