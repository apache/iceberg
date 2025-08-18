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
package org.apache.iceberg.flink.sink;

import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.data.FlinkSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RowDataTransformerUtil {
  private static final int ROOT_POSITION = -1;
  private static final Getter<PositionDelete<RowData>, ?> DELETE_PATH_GETTER =
      (PositionDelete<RowData> data, int pos) -> StringData.fromString(data.path().toString());
  private static final Getter<PositionDelete<RowData>, ?> DELETE_POS_GETTER =
      (PositionDelete<RowData> data, int pos) -> data.pos();
  private static final Getter<PositionDelete<RowData>, ?> DELETE_ROW_GETTER =
      (PositionDelete<RowData> data, int pos) -> data.row();

  private RowDataTransformerUtil() {}

  public static Function<RowData, RowData> transformer(RowType rowType, Types.StructType struct) {
    if (containsSpecialType(rowType)) {
      // Only create a transformer if the rowType contains types to convert
      DataAccessor accessor = accessor(rowType, struct);
      return accessor::transform;
    } else {
      return row -> row;
    }
  }

  public static Function<PositionDelete<RowData>, RowData> deleteTransformer() {
    DeleteAccessor accessor =
        new DeleteAccessor(new Getter[] {DELETE_PATH_GETTER, DELETE_POS_GETTER, DELETE_ROW_GETTER});
    return accessor::transform;
  }

  @VisibleForTesting
  static Function<RowData, RowData> forcedTransformer(RowType rowType, Types.StructType struct) {
    DataAccessor accessor = accessor(rowType, struct);
    return accessor::transform;
  }

  private static DataAccessor accessor(RowType rowType, Types.StructType struct) {
    Getter<?, RowData> getter =
        (Getter<?, RowData>)
            RowDataVisitor.visit(rowType, new Schema(struct.fields()), new RowDataVisitor());
    return new DataAccessor(new Getter[] {getter});
  }

  /**
   * Checks if the given LogicalType is or contains TINYINT, SMALLINT, DECIMAL, or TIMESTAMP type.
   *
   * @param logicalType the LogicalType to check
   * @return true if the LogicalType is or contains the specified types
   */
  @SuppressWarnings("CyclomaticComplexity")
  private static boolean containsSpecialType(LogicalType logicalType) {
    if (logicalType == null) {
      return false;
    }

    LogicalTypeRoot typeRoot = logicalType.getTypeRoot();

    // Check for direct match
    if (typeRoot == LogicalTypeRoot.TINYINT
        || typeRoot == LogicalTypeRoot.SMALLINT
        || typeRoot == LogicalTypeRoot.DECIMAL
        || typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
        || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return true;
    }

    // Check nested types
    if (typeRoot == LogicalTypeRoot.ROW) {
      // For RowType, check each field
      RowType rowType = (RowType) logicalType;
      for (RowType.RowField field : rowType.getFields()) {
        if (containsSpecialType(field.getType())) {
          return true;
        }
      }

      return false;
    }

    if (typeRoot == LogicalTypeRoot.ARRAY) {
      return containsSpecialType(((ArrayType) logicalType).getElementType());
    }

    if (typeRoot == LogicalTypeRoot.MAP) {
      MapType mapType = (MapType) logicalType;
      return containsSpecialType(mapType.getKeyType())
          || containsSpecialType(mapType.getValueType());
    }

    return false;
  }

  private interface Getter<D, T> {
    T get(D data, int pos);
  }

  private static class DataAccessor extends RowDataAccessor<RowData> {
    private DataAccessor(Getter<RowData, ?>[] getters) {
      super(getters);
    }

    @Override
    public RowKind getRowKind() {
      return data().getRowKind();
    }

    @Override
    public boolean isNullAt(int pos) {
      return data().isNullAt(pos);
    }

    RowData transform(RowData row) {
      return (RowData) getters()[0].get(row, ROOT_POSITION);
    }
  }

  private static class DeleteAccessor extends RowDataAccessor<PositionDelete<RowData>> {
    private DeleteAccessor(Getter<PositionDelete<RowData>, ?>[] getters) {
      super(getters);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return ordinal == 2 && data().row() == null;
    }

    @Override
    public RowKind getRowKind() {
      return RowKind.INSERT;
    }

    RowData transform(PositionDelete<RowData> delete) {
      this.data(delete);
      return this;
    }
  }

  private abstract static class RowDataAccessor<D> implements RowData {
    private final Getter<D, ?>[] getters;
    private D data = null;

    private RowDataAccessor(Getter<D, ?>[] getters) {
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
    public int getArity() {
      return getters.length;
    }

    @Override
    public void setRowKind(RowKind kind) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public boolean getBoolean(int pos) {
      return (boolean) getters[pos].get(data, pos);
    }

    @Override
    public byte getByte(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public short getShort(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public int getInt(int pos) {
      return (int) getters[pos].get(data, pos);
    }

    @Override
    public long getLong(int pos) {
      return (long) getters[pos].get(data, pos);
    }

    @Override
    public float getFloat(int pos) {
      return (float) getters[pos].get(data, pos);
    }

    @Override
    public double getDouble(int pos) {
      return (double) getters[pos].get(data, pos);
    }

    @Override
    public StringData getString(int pos) {
      return (StringData) getters[pos].get(data, pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
      return (DecimalData) getters[pos].get(data, pos);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
      return (TimestampData) getters[pos].get(data, pos);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public byte[] getBinary(int pos) {
      return (byte[]) getters[pos].get(data, pos);
    }

    @Override
    public ArrayData getArray(int pos) {
      return (ArrayData) getters[pos].get(data, pos);
    }

    @Override
    public MapData getMap(int pos) {
      return (MapData) getters[pos].get(data, pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
      return (RowData) getters[pos].get(data, pos);
    }
  }

  private static class ArrayDataAccessor implements ArrayData {
    private final Getter<ArrayData, ?> getter;
    private ArrayData arrayData = null;

    private ArrayDataAccessor(Getter<ArrayData, ?> getter) {
      this.getter = getter;
    }

    @Override
    public int size() {
      return arrayData.size();
    }

    @Override
    public boolean[] toBooleanArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public byte[] toByteArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public short[] toShortArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public int[] toIntArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public long[] toLongArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public float[] toFloatArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public double[] toDoubleArray() {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public boolean isNullAt(int pos) {
      return arrayData.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
      return (boolean) getter.get(arrayData, pos);
    }

    @Override
    public byte getByte(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public short getShort(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public int getInt(int pos) {
      return (int) getter.get(arrayData, pos);
    }

    @Override
    public long getLong(int pos) {
      return (long) getter.get(arrayData, pos);
    }

    @Override
    public float getFloat(int pos) {
      return (float) getter.get(arrayData, pos);
    }

    @Override
    public double getDouble(int pos) {
      return (double) getter.get(arrayData, pos);
    }

    @Override
    public StringData getString(int pos) {
      return (StringData) getter.get(arrayData, pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
      return (DecimalData) getter.get(arrayData, pos);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
      return (TimestampData) getter.get(arrayData, pos);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public byte[] getBinary(int pos) {
      return (byte[]) getter.get(arrayData, pos);
    }

    @Override
    public ArrayData getArray(int pos) {
      return (ArrayData) getter.get(arrayData, pos);
    }

    @Override
    public MapData getMap(int pos) {
      return (MapData) getter.get(arrayData, pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
      return (RowData) getter.get(arrayData, pos);
    }
  }

  private static class MapDataAccessor implements MapData {
    private final ArrayDataAccessor key;
    private final ArrayDataAccessor value;

    private MapDataAccessor(ArrayDataAccessor key, ArrayDataAccessor value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int size() {
      return key.size();
    }

    @Override
    public ArrayData keyArray() {
      return key;
    }

    @Override
    public ArrayData valueArray() {
      return value;
    }
  }

  private static class RowDataVisitor extends FlinkSchemaVisitor<Getter<?, ?>> {
    private final Deque<Boolean> isInList = Queues.newArrayDeque();

    private RowDataVisitor() {
      isInList.push(false);
    }

    @Override
    public void beforeStruct(Types.StructType type) {
      isInList.push(false);
      super.beforeStruct(type);
    }

    @Override
    public void afterStruct(Types.StructType type) {
      isInList.pop();
      super.afterStruct(type);
    }

    @Override
    public void beforeListElement(Types.NestedField elementField) {
      isInList.push(true);
      super.beforeListElement(elementField);
    }

    @Override
    public void afterListElement(Types.NestedField elementField) {
      isInList.pop();
      super.afterListElement(elementField);
    }

    @Override
    public void beforeMapKey(Types.NestedField keyField) {
      isInList.push(true);
      super.beforeMapKey(keyField);
    }

    @Override
    public void afterMapKey(Types.NestedField keyField) {
      isInList.pop();
      super.afterMapKey(keyField);
    }

    @Override
    public void beforeMapValue(Types.NestedField valueField) {
      isInList.push(true);
      super.beforeMapValue(valueField);
    }

    @Override
    public void afterMapValue(Types.NestedField valueField) {
      isInList.pop();
      super.afterMapValue(valueField);
    }

    @Override
    public Getter<?, RowData> record(
        Types.StructType iStruct, List<Getter<?, ?>> results, List<LogicalType> fieldTypes) {
      DataAccessor accessor = new DataAccessor(results.toArray(new Getter[0]));
      boolean isInListFlag = Boolean.TRUE.equals(isInList.peek());
      return (data, pos) -> {
        if (isInListFlag) {
          accessor.data(((ArrayData) data).getRow(pos, iStruct.fields().size()));
        } else {
          if (pos == ROOT_POSITION) {
            accessor.data((RowData) data);
          } else {
            accessor.data(((RowData) data).getRow(pos, iStruct.fields().size()));
          }
        }

        return accessor;
      };
    }

    @Override
    public Getter<?, ArrayData> list(
        Types.ListType iList, Getter<?, ?> getter, LogicalType elementType) {
      ArrayDataAccessor accessor = new ArrayDataAccessor((Getter<ArrayData, ?>) getter);
      boolean isInListFlag = Boolean.TRUE.equals(isInList.peek());
      return (data, pos) -> {
        accessor.arrayData =
            isInListFlag ? ((ArrayData) data).getArray(pos) : ((RowData) data).getArray(pos);
        return accessor;
      };
    }

    @Override
    public Getter<?, MapData> map(
        Types.MapType iMap,
        Getter<?, ?> keyGetter,
        Getter<?, ?> valueGetter,
        LogicalType keyType,
        LogicalType valueType) {
      MapDataAccessor mapDataAccessor =
          new MapDataAccessor(
              new ArrayDataAccessor((Getter<ArrayData, ?>) keyGetter),
              new ArrayDataAccessor((Getter<ArrayData, ?>) valueGetter));
      boolean isInListFlag = Boolean.TRUE.equals(isInList.peek());
      return (data, pos) -> {
        if (isInListFlag) {
          mapDataAccessor.key.arrayData = ((ArrayData) data).getMap(pos).keyArray();
          mapDataAccessor.value.arrayData = ((ArrayData) data).getMap(pos).valueArray();
        } else {
          mapDataAccessor.key.arrayData = ((RowData) data).getMap(pos).keyArray();
          mapDataAccessor.value.arrayData = ((RowData) data).getMap(pos).valueArray();
        }

        return mapDataAccessor;
      };
    }

    @Override
    public Getter<?, ?> primitive(Type.PrimitiveType type, LogicalType logicalType) {
      if (Boolean.TRUE.equals(isInList.peek())) {
        return arrayPrimitive(type, logicalType);
      } else {
        return rowDataPrimitive(type, logicalType);
      }
    }

    private Getter<RowData, ?> rowDataPrimitive(Type type, LogicalType logicalType) {
      switch (type.typeId()) {
        case BOOLEAN:
          return RowData::getBoolean;
        case INTEGER:
          if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
            return (row, pos) -> (int) row.getByte(pos);
          } else if (logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
            return (row, pos) -> (int) row.getShort(pos);
          }

          return RowData::getInt;
        case LONG:
          return RowData::getLong;
        case FLOAT:
          return RowData::getFloat;
        case DOUBLE:
          return RowData::getDouble;
        case STRING:
          return RowData::getString;
        case DECIMAL:
          DecimalType decimalType = (DecimalType) logicalType;
          return (row, pos) ->
              row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
        case DATE:
        case TIME:
          return RowData::getInt;
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          switch (logicalType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
              return (row, pos) ->
                  row.getTimestamp(pos, ((TimestampType) logicalType).getPrecision());

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              return (row, pos) ->
                  row.getTimestamp(pos, ((LocalZonedTimestampType) logicalType).getPrecision());
            default:
              throw new UnsupportedOperationException(
                  "Not supported logical type: " + logicalType.getTypeRoot());
          }
        case UUID:
        case BINARY:
        case FIXED:
          return RowData::getBinary;
        default:
          throw new UnsupportedOperationException("Not supported type: " + type.typeId());
      }
    }

    private Getter<ArrayData, ?> arrayPrimitive(Type type, LogicalType logicalType) {
      switch (type.typeId()) {
        case BOOLEAN:
          return ArrayData::getBoolean;
        case INTEGER:
          if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
            return (row, pos) -> (int) row.getByte(pos);
          } else if (logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
            return (row, pos) -> (int) row.getShort(pos);
          }

          return ArrayData::getInt;
        case LONG:
          return ArrayData::getLong;
        case FLOAT:
          return ArrayData::getFloat;
        case DOUBLE:
          return ArrayData::getDouble;
        case STRING:
          return ArrayData::getString;
        case DECIMAL:
          DecimalType decimalType = (DecimalType) logicalType;
          return (row, pos) ->
              row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
        case DATE:
        case TIME:
          return ArrayData::getInt;
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          switch (logicalType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
              return (row, pos) ->
                  row.getTimestamp(pos, ((TimestampType) logicalType).getPrecision());

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
              return (row, pos) ->
                  row.getTimestamp(pos, ((LocalZonedTimestampType) logicalType).getPrecision());
            default:
              throw new UnsupportedOperationException(
                  "Not supported logical type: " + logicalType.getTypeRoot());
          }
        case UUID:
        case BINARY:
        case FIXED:
          return ArrayData::getBinary;

        default:
          throw new UnsupportedOperationException("Not supported type: " + type.typeId());
      }
    }
  }
}
