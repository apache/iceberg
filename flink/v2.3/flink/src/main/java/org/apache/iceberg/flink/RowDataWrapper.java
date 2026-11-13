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
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.Set;
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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

public class RowDataWrapper implements StructLike {

  private final LogicalType[] types;
  private final RowData.FieldGetter[] getters;
  private RowData rowData = null;

  public RowDataWrapper(RowType rowType, Types.StructType struct) {
    this(rowType, struct, true /* reuse struct wrappers */);
  }

  private RowDataWrapper(RowType rowType, Types.StructType struct, boolean reuseStructWrappers) {
    int size = rowType.getFieldCount();

    types = (LogicalType[]) Array.newInstance(LogicalType.class, size);
    getters = new RowData.FieldGetter[size];

    for (int i = 0; i < size; i++) {
      types[i] = rowType.getTypeAt(i);
      getters[i] = buildGetter(types[i], struct.fields().get(i).type(), i, reuseStructWrappers);
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

  private static RowData.FieldGetter buildGetter(
      LogicalType logicalType, Type type, int pos, boolean reuseStructWrappers) {
    RowData.FieldGetter fieldGetter = FlinkRowData.createFieldGetter(logicalType, pos);
    if (reuseStructWrappers && logicalType instanceof RowType rowType) {
      // A struct field is read one row at a time, so its wrapper can be reused.
      RowDataWrapper structWrapper = new RowDataWrapper(rowType, type.asStructType());
      return row -> structWrapper.wrap((RowData) fieldGetter.getFieldOrNull(row));
    }

    Function<Object, Object> converter = buildConverter(logicalType, type);
    if (converter == null) {
      return fieldGetter;
    }

    // get() returns null fields before calling a getter, so values are never null here.
    return row -> converter.apply(fieldGetter.getFieldOrNull(row));
  }

  // Returns null when values are already in Iceberg's representation.
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
        // Values read together, like list elements, need their own wrappers. The template
        // doesn't reuse struct wrappers, so its getters can be shared by the copies.
        RowDataWrapper template =
            new RowDataWrapper(
                (RowType) logicalType, type.asStructType(), false /* new wrapper per value */);
        yield value -> template.copyFor((RowData) value);
      }
      case ARRAY -> {
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        ArrayData.ElementGetter elementGetter = FlinkRowData.createElementGetter(elementType);
        Function<Object, Object> elementConverter =
            buildConverter(elementType, type.asListType().elementType());
        yield value -> new ArrayDataList((ArrayData) value, elementGetter, elementConverter);
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
        yield value ->
            new MapDataMap((MapData) value, keyGetter, keyConverter, valueGetter, valueConverter);
      }
      default -> null;
    };
  }

  private static Object convertElement(
      ArrayData array,
      int pos,
      ArrayData.ElementGetter getter,
      Function<Object, Object> converter) {
    if (array.isNullAt(pos)) {
      return null;
    }

    Object value = getter.getElementOrNull(array, pos);
    return converter != null ? converter.apply(value) : value;
  }

  private static class ArrayDataList extends AbstractList<Object> implements RandomAccess {
    private final ArrayData array;
    private final ArrayData.ElementGetter getter;
    private final Function<Object, Object> converter;

    private ArrayDataList(
        ArrayData array, ArrayData.ElementGetter getter, Function<Object, Object> converter) {
      this.array = array;
      this.getter = getter;
      this.converter = converter;
    }

    @Override
    public Object get(int index) {
      Preconditions.checkElementIndex(index, array.size());
      return convertElement(array, index, getter, converter);
    }

    @Override
    public int size() {
      return array.size();
    }
  }

  private static class MapDataMap extends AbstractMap<Object, Object> {
    private final MapData map;
    private final ArrayData.ElementGetter keyGetter;
    private final Function<Object, Object> keyConverter;
    private final ArrayData.ElementGetter valueGetter;
    private final Function<Object, Object> valueConverter;

    private MapDataMap(
        MapData map,
        ArrayData.ElementGetter keyGetter,
        Function<Object, Object> keyConverter,
        ArrayData.ElementGetter valueGetter,
        Function<Object, Object> valueConverter) {
      this.map = map;
      this.keyGetter = keyGetter;
      this.keyConverter = keyConverter;
      this.valueGetter = valueGetter;
      this.valueConverter = valueConverter;
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
      return new AbstractSet<>() {
        @Override
        public Iterator<Entry<Object, Object>> iterator() {
          return new EntryIterator();
        }

        @Override
        public int size() {
          return map.size();
        }
      };
    }

    private class EntryIterator implements Iterator<Entry<Object, Object>> {
      private final ArrayData keys = map.keyArray();
      private final ArrayData values = map.valueArray();
      private int pos = 0;

      @Override
      public boolean hasNext() {
        return pos < map.size();
      }

      @Override
      public Entry<Object, Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        Entry<Object, Object> entry =
            new SimpleImmutableEntry<>(
                convertElement(keys, pos, keyGetter, keyConverter),
                convertElement(values, pos, valueGetter, valueConverter));
        pos += 1;
        return entry;
      }
    }
  }
}
