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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

public class RowDataWrapper implements StructLike {

  private final LogicalType[] types;
  private final PositionalGetter<?>[] getters;
  private RowData rowData = null;

  public RowDataWrapper(RowType rowType, Types.StructType struct) {
    int size = rowType.getFieldCount();

    types = (LogicalType[]) Array.newInstance(LogicalType.class, size);
    getters = (PositionalGetter[]) Array.newInstance(PositionalGetter.class, size);

    for (int i = 0; i < size; i++) {
      types[i] = rowType.getTypeAt(i);
      PositionalGetter<?> getter = buildGetter(types[i], struct.fields().get(i).type());
      if (getter == null) {
        // Pre-build the Flink field getter once instead of recreating it on every access.
        RowData.FieldGetter fieldGetter = FlinkRowData.createFieldGetter(types[i], i);
        getter = (row, pos) -> fieldGetter.getFieldOrNull(row);
      }

      getters[i] = getter;
    }
  }

  public RowDataWrapper wrap(RowData data) {
    this.rowData = data;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (rowData.isNullAt(pos)) {
      return null;
    }

    return javaClass.cast(getters[pos].get(rowData, pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException(
        "Could not set a field in the RowDataWrapper because rowData is read-only");
  }

  private interface PositionalGetter<T> {
    T get(RowData data, int pos);
  }

  private static PositionalGetter<?> buildGetter(LogicalType logicalType, Type type) {
    switch (logicalType.getTypeRoot()) {
      case TINYINT:
        return (row, pos) -> (int) row.getByte(pos);
      case SMALLINT:
        return (row, pos) -> (int) row.getShort(pos);
      case CHAR:
      case VARCHAR:
        return (row, pos) -> row.getString(pos).toString();

      case BINARY:
      case VARBINARY:
        if (Type.TypeID.UUID == type.typeId()) {
          return (row, pos) -> UUIDUtil.convert(row.getBinary(pos));
        } else {
          return (row, pos) -> ByteBuffer.wrap(row.getBinary(pos));
        }

      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        return (row, pos) ->
            row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale()).toBigDecimal();

      case TIME_WITHOUT_TIME_ZONE:
        // Time in RowData is in milliseconds (Integer), while iceberg's time is microseconds
        // (Long).
        return (row, pos) -> ((long) row.getInt(pos)) * 1_000;

      case TIMESTAMP_WITHOUT_TIME_ZONE:
        TimestampType timestampType = (TimestampType) logicalType;
        if (type.typeId() == Type.TypeID.TIMESTAMP_NANO) {
          return (row, pos) -> {
            LocalDateTime localDateTime =
                row.getTimestamp(pos, timestampType.getPrecision()).toLocalDateTime();
            return DateTimeUtil.nanosFromTimestamp(localDateTime);
          };
        } else {
          return (row, pos) -> {
            LocalDateTime localDateTime =
                row.getTimestamp(pos, timestampType.getPrecision()).toLocalDateTime();
            return DateTimeUtil.microsFromTimestamp(localDateTime);
          };
        }

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        LocalZonedTimestampType lzTs = (LocalZonedTimestampType) logicalType;
        if (type.typeId() == Type.TypeID.TIMESTAMP_NANO) {
          return (row, pos) -> {
            TimestampData timestampData = row.getTimestamp(pos, lzTs.getPrecision());
            return timestampData.getMillisecond() * 1_000_000L
                + timestampData.getNanoOfMillisecond();
          };
        } else {
          return (row, pos) -> {
            TimestampData timestampData = row.getTimestamp(pos, lzTs.getPrecision());
            return timestampData.getMillisecond() * 1000L
                + timestampData.getNanoOfMillisecond() / 1000;
          };
        }

      case ROW:
        RowType rowType = (RowType) logicalType;
        Types.StructType structType = (Types.StructType) type;

        RowDataWrapper nestedWrapper = new RowDataWrapper(rowType, structType);
        return (row, pos) -> nestedWrapper.wrap(row.getRow(pos, rowType.getFieldCount()));

      case ARRAY:
        Function<Object, Object> listConverter = buildConverter(logicalType, type);
        return (row, pos) -> listConverter.apply(row.getArray(pos));

      case MAP:
        Function<Object, Object> mapConverter = buildConverter(logicalType, type);
        return (row, pos) -> mapConverter.apply(row.getMap(pos));

      default:
        return null;
    }
  }

  private static Function<Object, Object> buildConverter(LogicalType logicalType, Type type) {
    return switch (logicalType.getTypeRoot()) {
      case TINYINT -> value -> ((Byte) value).intValue();
      case SMALLINT -> value -> ((Short) value).intValue();
      case CHAR, VARCHAR -> Object::toString;
      case BINARY, VARBINARY ->
          Type.TypeID.UUID == type.typeId()
              ? value -> UUIDUtil.convert((byte[]) value)
              : value -> ByteBuffer.wrap((byte[]) value);
      case DECIMAL -> value -> ((DecimalData) value).toBigDecimal();
      case TIME_WITHOUT_TIME_ZONE -> value -> ((Integer) value).longValue() * 1_000;
      case TIMESTAMP_WITHOUT_TIME_ZONE ->
          type.typeId() == Type.TypeID.TIMESTAMP_NANO
              ? value -> DateTimeUtil.nanosFromTimestamp(((TimestampData) value).toLocalDateTime())
              : value ->
                  DateTimeUtil.microsFromTimestamp(((TimestampData) value).toLocalDateTime());
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          type.typeId() == Type.TypeID.TIMESTAMP_NANO
              ? value -> {
                TimestampData timestampData = (TimestampData) value;
                return timestampData.getMillisecond() * 1_000_000L
                    + timestampData.getNanoOfMillisecond();
              }
              : value -> {
                TimestampData timestampData = (TimestampData) value;
                return timestampData.getMillisecond() * 1000L
                    + timestampData.getNanoOfMillisecond() / 1000;
              };
      case ROW -> {
        RowType rowType = (RowType) logicalType;
        Types.StructType structType = type.asStructType();
        // Collection elements are read together, so each element needs its own wrapper.
        yield value -> new RowDataWrapper(rowType, structType).wrap((RowData) value);
      }
      case ARRAY -> {
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        ArrayData.ElementGetter elementGetter = elementGetter(elementType);
        Function<Object, Object> elementConverter =
            buildConverter(elementType, type.asListType().elementType());
        yield value -> toList((ArrayData) value, elementGetter, elementConverter);
      }
      case MAP -> {
        MapType mapType = (MapType) logicalType;
        Types.MapType icebergMapType = type.asMapType();
        ArrayData.ElementGetter keyGetter = elementGetter(mapType.getKeyType());
        ArrayData.ElementGetter valueGetter = elementGetter(mapType.getValueType());
        Function<Object, Object> keyConverter =
            buildConverter(mapType.getKeyType(), icebergMapType.keyType());
        Function<Object, Object> valueConverter =
            buildConverter(mapType.getValueType(), icebergMapType.valueType());
        yield value -> toMap((MapData) value, keyGetter, keyConverter, valueGetter, valueConverter);
      }
      default -> Function.identity();
    };
  }

  private static ArrayData.ElementGetter elementGetter(LogicalType elementType) {
    // Iceberg's unknown type is Flink's NULL type, which createElementGetter rejects.
    if (elementType instanceof NullType) {
      return (array, pos) -> null;
    }

    return ArrayData.createElementGetter(elementType);
  }

  private static List<Object> toList(
      ArrayData array, ArrayData.ElementGetter getter, Function<Object, Object> converter) {
    // Copied rather than viewed because readers may reuse the array for the next row.
    List<Object> list = Lists.newArrayListWithCapacity(array.size());
    for (int i = 0; i < array.size(); i += 1) {
      list.add(convertOrNull(getter.getElementOrNull(array, i), converter));
    }

    return list;
  }

  private static Map<Object, Object> toMap(
      MapData map,
      ArrayData.ElementGetter keyGetter,
      Function<Object, Object> keyConverter,
      ArrayData.ElementGetter valueGetter,
      Function<Object, Object> valueConverter) {
    ArrayData keys = map.keyArray();
    ArrayData values = map.valueArray();
    Map<Object, Object> result = Maps.newHashMapWithExpectedSize(map.size());
    for (int i = 0; i < map.size(); i += 1) {
      result.put(
          convertOrNull(keyGetter.getElementOrNull(keys, i), keyConverter),
          convertOrNull(valueGetter.getElementOrNull(values, i), valueConverter));
    }

    return result;
  }

  private static Object convertOrNull(Object value, Function<Object, Object> converter) {
    return value == null ? null : converter.apply(value);
  }
}
