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

import java.util.List;
import org.apache.iceberg.data.RowTransformer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class InternalRowTransformer implements RowTransformer<InternalRow> {
  private static final int ROOT_POSITION = -1;
  private final PositionalGetter<InternalRow> getter;

  public InternalRowTransformer(StructType sparkType) {
    this.getter =
        (PositionalGetter<InternalRow>)
            InternalRowVisitor.visit(sparkType, new InternalRowVisitor());
  }

  @Override
  public InternalRow transform(InternalRow row) {
    return getter.get(row, ROOT_POSITION);
  }

  private interface PositionalGetter<T> {
    T get(SpecializedGetters data, int pos);
  }

  private static class InternalRowAccessor extends InternalRow {
    private final PositionalGetter<?>[] getters;
    private SpecializedGetters row;

    private InternalRowAccessor(PositionalGetter<?>[] getters) {
      this.getters = getters;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return row.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return (boolean) getters[ordinal].get(row, ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public short getShort(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public int getInt(int ordinal) {
      return (int) getters[ordinal].get(row, ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return (long) getters[ordinal].get(row, ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return (float) getters[ordinal].get(row, ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return (double) getters[ordinal].get(row, ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return (Decimal) getters[ordinal].get(row, ordinal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return (UTF8String) getters[ordinal].get(row, ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) getters[ordinal].get(row, ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return (InternalRow) getters[ordinal].get(row, ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return (ArrayData) getters[ordinal].get(row, ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      return (MapData) getters[ordinal].get(row, ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      return getters[ordinal].get(row, ordinal);
    }

    @Override
    public int numFields() {
      return getters.length;
    }

    @Override
    public void setNullAt(int i) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public void update(int i, Object value) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public InternalRow copy() {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }
  }

  private static class ArrayDataAccessor extends ArrayData {
    private final PositionalGetter<?> elementGetter;
    private ArrayData arrayData;

    private ArrayDataAccessor(PositionalGetter<?> elementGetter) {
      this.elementGetter = elementGetter;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return (boolean) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public short getShort(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public int getInt(int ordinal) {
      return (int) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return (long) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return (float) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return (double) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return (Decimal) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return (UTF8String) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return (InternalRow) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return (ArrayData) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      return (MapData) elementGetter.get(arrayData, ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      return elementGetter.get(arrayData, ordinal);
    }

    @Override
    public int numElements() {
      return arrayData.numElements();
    }

    @Override
    public ArrayData copy() {
      throw new UnsupportedOperationException("Not supported for ArrayDataAccessor");
    }

    @Override
    public Object[] array() {
      throw new UnsupportedOperationException("Not supported for ArrayDataAccessor");
    }

    @Override
    public void setNullAt(int i) {
      throw new UnsupportedOperationException("Not supported for ArrayDataAccessor");
    }

    @Override
    public void update(int i, Object value) {
      throw new UnsupportedOperationException("Not supported for ArrayDataAccessor");
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return arrayData.isNullAt(ordinal);
    }
  }

  private static class MapDataAccessor extends MapData {
    private final ArrayDataAccessor key;
    private final ArrayDataAccessor value;

    private MapDataAccessor(ArrayDataAccessor key, ArrayDataAccessor value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int numElements() {
      return key.numElements();
    }

    @Override
    public ArrayData keyArray() {
      return key;
    }

    @Override
    public ArrayData valueArray() {
      return value;
    }

    @Override
    public MapData copy() {
      throw new UnsupportedOperationException("Not supported for MapDataAccessor");
    }
  }

  private static class InternalRowVisitor extends SparkTypeVisitor<PositionalGetter<?>> {
    @Override
    public PositionalGetter<InternalRow> struct(
        StructType struct, List<PositionalGetter<?>> fieldResults) {
      InternalRowAccessor accessor =
          new InternalRowAccessor(fieldResults.toArray(new PositionalGetter[0]));
      return (data, pos) -> {
        accessor.row = pos == ROOT_POSITION ? data : (SpecializedGetters) data.get(pos, struct);
        return accessor.row != null ? accessor : null;
      };
    }

    @Override
    public PositionalGetter<?> field(StructField field, PositionalGetter<?> typeResult) {
      return typeResult;
    }

    @Override
    public PositionalGetter<ArrayData> array(ArrayType array, PositionalGetter<?> elementResult) {
      ArrayDataAccessor accessor = new ArrayDataAccessor(elementResult);
      return (data, pos) -> {
        accessor.arrayData = data.getArray(pos);
        return accessor.arrayData != null ? accessor : null;
      };
    }

    @Override
    public PositionalGetter<MapData> map(
        MapType map, PositionalGetter<?> keyResult, PositionalGetter<?> valueResult) {
      MapDataAccessor mapDataAccessor =
          new MapDataAccessor(new ArrayDataAccessor(keyResult), new ArrayDataAccessor(valueResult));
      return (data, pos) -> {
        MapData mapData = data.getMap(pos);
        if (mapData == null) {
          return null;
        }

        mapDataAccessor.key.arrayData = mapData.keyArray();
        mapDataAccessor.value.arrayData = mapData.valueArray();
        return mapDataAccessor;
      };
    }

    @Override
    public PositionalGetter<?> atomic(DataType atomic) {
      if (atomic instanceof ShortType) {
        return (data, pos) -> (int) data.getShort(pos);

      } else if (atomic instanceof ByteType) {
        return (data, pos) -> (int) data.getByte(pos);

      } else {
        return (data, pos) -> data.get(pos, atomic);
      }
    }
  }
}
