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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.data.FlinkSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RowDataTransformer {
  private static final int ROOT_POSITION = -1;
  private final PositionalGetter<RowData, RowData> getter;
  private final PositionDelete<RowData> positionDelete = PositionDelete.create();

  public RowDataTransformer(RowType rowType, Types.StructType struct) {
    this.getter =
        (PositionalGetter<RowData, RowData>)
            RowDataVisitor.visit(rowType, new Schema(struct.fields()), new RowDataVisitor());
  }

  public RowData wrap(RowData data) {
    return getter.get(data, ROOT_POSITION);
  }

  public PositionDelete<RowData> wrap(PositionDelete<RowData> data) {
    return positionDelete.set(data.path(), data.pos(), getter.get(data.row(), ROOT_POSITION));
  }

  private interface PositionalGetter<T, I> {
    T get(I data, int pos);
  }

  private static class RowDataAccessor implements RowData {
    private final PositionalGetter<?, RowData>[] getters;
    private RowData rowData = null;

    private RowDataAccessor(PositionalGetter<?, RowData>[] getters) {
      this.getters = getters;
    }

    private RowData wrap(RowData data) {
      this.rowData = data;
      return this;
    }

    @Override
    public int getArity() {
      return rowData.getArity();
    }

    @Override
    public RowKind getRowKind() {
      return rowData.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public boolean isNullAt(int pos) {
      return rowData.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
      return (boolean) getters[pos].get(rowData, pos);
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
      return (int) getters[pos].get(rowData, pos);
    }

    @Override
    public long getLong(int pos) {
      return (long) getters[pos].get(rowData, pos);
    }

    @Override
    public float getFloat(int pos) {
      return (float) getters[pos].get(rowData, pos);
    }

    @Override
    public double getDouble(int pos) {
      return (double) getters[pos].get(rowData, pos);
    }

    @Override
    public StringData getString(int pos) {
      return (StringData) getters[pos].get(rowData, pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
      return (DecimalData) getters[pos].get(rowData, pos);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
      return (TimestampData) getters[pos].get(rowData, pos);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
    }

    @Override
    public byte[] getBinary(int pos) {
      return (byte[]) getters[pos].get(rowData, pos);
    }

    @Override
    public ArrayData getArray(int pos) {
      return (ArrayData) getters[pos].get(rowData, pos);
    }

    @Override
    public MapData getMap(int pos) {
      return (MapData) getters[pos].get(rowData, pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
      return (RowData) getters[pos].get(rowData, pos);
    }
  }

  private static class ArrayDataAccessor implements ArrayData {
    private final PositionalGetter<?, ArrayData> getter;
    private ArrayData arrayData = null;
    private int size;

    private ArrayDataAccessor(PositionalGetter<?, ArrayData> getter) {
      this.getter = getter;
    }

    private ArrayData wrap(ArrayData data) {
      this.arrayData = data;
      this.size = data.size();
      return this;
    }

    @Override
    public int size() {
      return size;
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

  private static class RowDataVisitor extends FlinkSchemaVisitor<PositionalGetter<?, ?>> {
    private final Deque<Boolean> isInList = Queues.newArrayDeque();

    private RowDataVisitor() {
      isInList.push(false);
    }

    @Override
    public void beforeStructElement(Types.StructType type) {
      isInList.push(false);
      super.beforeStructElement(type);
    }

    @Override
    public void afterStructElement(Types.StructType type) {
      isInList.pop();
      super.afterStructElement(type);
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
    public PositionalGetter<?, ?> record(
        Types.StructType iStruct,
        List<PositionalGetter<?, ?>> results,
        List<LogicalType> fieldTypes) {
      RowDataAccessor accessor = new RowDataAccessor(results.toArray(new PositionalGetter[0]));
      boolean isInListFlag = Boolean.TRUE.equals(isInList.peek());
      return ((data, pos) -> {
        if (isInListFlag) {
          return accessor.wrap(((ArrayData) data).getRow(pos, iStruct.fields().size()));
        } else {
          if (pos == ROOT_POSITION) {
            return accessor.wrap((RowData) data);
          } else {
            return accessor.wrap(((RowData) data).getRow(pos, iStruct.fields().size()));
          }
        }
      });
    }

    @Override
    public PositionalGetter<?, ?> list(
        Types.ListType iList, PositionalGetter<?, ?> getter, LogicalType elementType) {
      ArrayDataAccessor accessor = new ArrayDataAccessor((PositionalGetter<?, ArrayData>) getter);
      boolean isInListFlag = Boolean.TRUE.equals(isInList.peek());
      return (data, pos) ->
          accessor.wrap(
              isInListFlag ? ((ArrayData) data).getArray(pos) : ((RowData) data).getArray(pos));
    }

    @Override
    public PositionalGetter<?, ?> map(
        Types.MapType iMap,
        PositionalGetter<?, ?> keyGetter,
        PositionalGetter<?, ?> valueGetter,
        LogicalType keyType,
        LogicalType valueType) {
      boolean isInListFlag = Boolean.TRUE.equals(isInList.peek());
      return (data, pos) ->
          isInListFlag ? ((ArrayData) data).getMap(pos) : ((RowData) data).getMap(pos);
    }

    @Override
    public PositionalGetter<?, ?> primitive(Type.PrimitiveType type, LogicalType logicalType) {
      if (Boolean.TRUE.equals(isInList.peek())) {
        return arrayPrimitive(type, logicalType);
      } else {
        return rowDataPrimitive(type, logicalType);
      }
    }

    private PositionalGetter<?, RowData> rowDataPrimitive(Type type, LogicalType logicalType) {
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

    private PositionalGetter<?, ArrayData> arrayPrimitive(Type type, LogicalType logicalType) {
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
