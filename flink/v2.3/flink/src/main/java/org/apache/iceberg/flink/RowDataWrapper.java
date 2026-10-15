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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

public class RowDataWrapper implements StructLike {

  private final LogicalType[] types;
  private final RowData.FieldGetter[] getters;
  private RowData rowData = null;

  public RowDataWrapper(RowType rowType, Types.StructType struct) {
    int size = rowType.getFieldCount();

    types = (LogicalType[]) Array.newInstance(LogicalType.class, size);
    getters = new RowData.FieldGetter[size];

    for (int i = 0; i < size; i++) {
      types[i] = rowType.getTypeAt(i);
      getters[i] = buildGetter(types[i], struct.fields().get(i).type(), i);
    }
  }

  private RowDataWrapper(LogicalType[] types, RowData.FieldGetter[] getters) {
    this.types = types;
    this.getters = getters;
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

    return javaClass.cast(getters[pos].getFieldOrNull(rowData));
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException(
        "Could not set a field in the RowDataWrapper because rowData is read-only");
  }

  private RowDataWrapper copyFor(RowData data) {
    return new RowDataWrapper(types, getters).wrap(data);
  }

  private static RowData.FieldGetter buildGetter(LogicalType logicalType, Type type, int pos) {
    RowData.FieldGetter fieldGetter = FlinkRowData.createFieldGetter(logicalType, pos);
    Function<Object, Object> converter = buildConverter(logicalType, type);
    return row -> convertOrNull(fieldGetter.getFieldOrNull(row), converter);
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
      case TIME_WITHOUT_TIME_ZONE ->
          // Flink's time is milliseconds (int), Iceberg's time is microseconds (long)
          value -> ((Integer) value).longValue() * 1_000;
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
        // Each value gets its own wrapper, so values read together, like list elements, don't
        // overwrite each other. The copies share the template's getters, which hold no state.
        RowDataWrapper template = new RowDataWrapper((RowType) logicalType, type.asStructType());
        yield value -> template.copyFor((RowData) value);
      }
      case ARRAY -> {
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        ArrayData.ElementGetter elementGetter = FlinkRowData.createElementGetter(elementType);
        Function<Object, Object> elementConverter =
            buildConverter(elementType, type.asListType().elementType());
        yield value -> toList((ArrayData) value, elementGetter, elementConverter);
      }
      case MAP -> {
        MapType mapType = (MapType) logicalType;
        Types.MapType icebergMapType = type.asMapType();
        ArrayData.ElementGetter keyGetter = FlinkRowData.createElementGetter(mapType.getKeyType());
        ArrayData.ElementGetter valueGetter =
            FlinkRowData.createElementGetter(mapType.getValueType());
        Function<Object, Object> keyConverter =
            buildConverter(mapType.getKeyType(), icebergMapType.keyType());
        Function<Object, Object> valueConverter =
            buildConverter(mapType.getValueType(), icebergMapType.valueType());
        yield value -> toMap((MapData) value, keyGetter, keyConverter, valueGetter, valueConverter);
      }
      default -> Function.identity();
    };
  }

  private static List<Object> toList(
      ArrayData array, ArrayData.ElementGetter getter, Function<Object, Object> converter) {
    // Copied rather than viewed because readers may reuse containers across rows.
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
