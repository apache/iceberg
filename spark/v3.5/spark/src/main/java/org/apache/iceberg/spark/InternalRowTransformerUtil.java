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
import java.util.function.Function;
import org.apache.iceberg.deletes.PositionDelete;
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

public class InternalRowTransformerUtil {
  private static final int ROOT_POSITION = -1;
  private static final Getter<PositionDelete<InternalRow>, ?> DELETE_PATH_GETTER =
      (PositionDelete<InternalRow> data, int pos) -> UTF8String.fromString(data.path().toString());
  private static final Getter<PositionDelete<InternalRow>, ?> DELETE_POS_GETTER =
      (PositionDelete<InternalRow> data, int pos) -> data.pos();
  private static final Getter<PositionDelete<InternalRow>, ?> DELETE_ROW_GETTER =
      (PositionDelete<InternalRow> data, int pos) -> data.row();

  private InternalRowTransformerUtil() {}

  @SuppressWarnings("unchecked")
  public static Function<InternalRow, InternalRow> transformer(StructType structType) {
    if (containsSpecialType(structType)) {
      // Only create a transformer if the structType contains ShortType or ByteType
      // to avoid unnecessary overhead.
      Getter<SpecializedGetters, InternalRow> getter =
          (Getter<SpecializedGetters, InternalRow>)
              InternalRowVisitor.visit(structType, new InternalRowVisitor());
      DataAccessor accessor = new DataAccessor(new Getter[] {getter});
      return accessor::transform;
    } else {
      return row -> row;
    }
  }

  @SuppressWarnings("unchecked")
  public static Function<PositionDelete<InternalRow>, InternalRow> deleteTransformer() {
    DeleteAccessor accessor =
        new DeleteAccessor(new Getter[] {DELETE_PATH_GETTER, DELETE_POS_GETTER, DELETE_ROW_GETTER});
    return accessor::transform;
  }

  /**
   * Checks if the given DataType is or contains ShortType or ByteType.
   *
   * @param dataType the DataType to check
   * @return true if the DataType is or contains ShortType or ByteType
   */
  private static boolean containsSpecialType(DataType dataType) {
    if (dataType == null) {
      return false;
    }

    if (dataType instanceof ShortType || dataType instanceof ByteType) {
      return true;
    }

    if (dataType instanceof StructType) {
      StructType structType = (StructType) dataType;
      for (StructField field : structType.fields()) {
        if (containsSpecialType(field.dataType())) {
          return true;
        }
      }

      return false;
    }

    if (dataType instanceof ArrayType) {
      return containsSpecialType(((ArrayType) dataType).elementType());
    }

    if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      return containsSpecialType(mapType.keyType()) || containsSpecialType(mapType.valueType());
    }

    return false;
  }

  private interface Getter<D, T> {
    T get(D data, int pos);
  }

  private static class DataAccessor extends InternalRowAccessor<SpecializedGetters> {
    private DataAccessor(Getter<SpecializedGetters, ?>[] getters) {
      super(getters);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return data().isNullAt(ordinal);
    }

    InternalRow transform(InternalRow row) {
      return (InternalRow) getters()[0].get(row, ROOT_POSITION);
    }
  }

  private static class DeleteAccessor extends InternalRowAccessor<PositionDelete<InternalRow>> {
    private DeleteAccessor(Getter<PositionDelete<InternalRow>, ?>[] getters) {
      super(getters);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return ordinal == 2 && data().row() == null;
    }

    public InternalRow transform(PositionDelete<InternalRow> delete) {
      this.data(delete);
      return this;
    }
  }

  private abstract static class InternalRowAccessor<D> extends InternalRow {
    private final Getter<D, ?>[] getters;
    private D data;

    private InternalRowAccessor(Getter<D, ?>[] getters) {
      this.getters = getters;
    }

    Getter<D, ?>[] getters() {
      return getters;
    }

    D data() {
      return data;
    }

    void data(D newData) {
      this.data = newData;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return (boolean) getters[ordinal].get(data, ordinal);
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
      return (int) getters[ordinal].get(data, ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return (long) getters[ordinal].get(data, ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return (float) getters[ordinal].get(data, ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return (double) getters[ordinal].get(data, ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return (Decimal) getters[ordinal].get(data, ordinal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return (UTF8String) getters[ordinal].get(data, ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) getters[ordinal].get(data, ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new UnsupportedOperationException("Not supported for InternalRowAccessor");
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return (InternalRow) getters[ordinal].get(data, ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return (ArrayData) getters[ordinal].get(data, ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      return (MapData) getters[ordinal].get(data, ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      return getters[ordinal].get(data, ordinal);
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
    private final Getter<SpecializedGetters, ?> elementGetter;
    private ArrayData arrayData;

    private ArrayDataAccessor(Getter<SpecializedGetters, ?> elementGetter) {
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

  private static class InternalRowVisitor extends SparkTypeVisitor<Getter<SpecializedGetters, ?>> {
    @Override
    @SuppressWarnings("unchecked")
    public Getter<SpecializedGetters, InternalRow> struct(
        StructType struct, List<Getter<SpecializedGetters, ?>> fieldResults) {
      DataAccessor accessor = new DataAccessor(fieldResults.toArray(new Getter[0]));
      return (data, pos) -> {
        accessor.data(pos == ROOT_POSITION ? data : (SpecializedGetters) data.get(pos, struct));
        return accessor.data() != null ? accessor : null;
      };
    }

    @Override
    public Getter<SpecializedGetters, ?> field(
        StructField field, Getter<SpecializedGetters, ?> typeResult) {
      return typeResult;
    }

    @Override
    public Getter<SpecializedGetters, ArrayData> array(
        ArrayType array, Getter<SpecializedGetters, ?> elementResult) {
      ArrayDataAccessor accessor = new ArrayDataAccessor(elementResult);
      return (data, pos) -> {
        accessor.arrayData = data.getArray(pos);
        return accessor.arrayData != null ? accessor : null;
      };
    }

    @Override
    public Getter<SpecializedGetters, MapData> map(
        MapType map,
        Getter<SpecializedGetters, ?> keyResult,
        Getter<SpecializedGetters, ?> valueResult) {
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
    public Getter<SpecializedGetters, ?> atomic(DataType atomic) {
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
