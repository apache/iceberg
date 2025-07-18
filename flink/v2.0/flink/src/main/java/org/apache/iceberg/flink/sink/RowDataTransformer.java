/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import java.util.List;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.data.FlinkSchemaVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RowDataTransformer {
    private final PositionalGetter<?, RowData> getter;

    public RowDataTransformer(RowType rowType, Types.StructType struct) {
        this.getter = RowDataVisitor.visit(rowType, new Schema(struct.fields()), new RowDataVisitor());
    }

    public RowData wrap(RowData data) {
        return (RowData) getter.get(data, 0);
    }

    private interface PositionalGetter<T, I> {
        T get(I data, int pos);
    }

    public static class RowDataAccessor implements RowData {
        private final PositionalGetter<?, RowData>[] getters;
        private RowData rowData = null;

        public RowDataAccessor(PositionalGetter<?, RowData>[] getters) {
            this.getters = getters;
        }

        public RowData wrap(RowData data) {
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
            return (byte) getters[pos].get(rowData, pos);
        }

        @Override
        public short getShort(int pos) {
            return (short) getters[pos].get(rowData, pos);

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
            return (RawValueData<T>) getters[pos].get(rowData, pos);
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

        public ArrayDataAccessor(PositionalGetter<?, ArrayData> getter) {
            this.getter = getter;
        }

        public ArrayData wrap(ArrayData data) {
            this.arrayData = data;
            return this;
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
            return (byte) getter.get(arrayData, pos);
        }

        @Override
        public short getShort(int pos) {
            return (short) getter.get(arrayData, pos);
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
            return (RawValueData<T>) getter.get(arrayData, pos);
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
        private final ArrayDataAccessor keyAccessor;
        private final ArrayDataAccessor valueAccessor;
        private ArrayData keyData = null;
        private ArrayData valueData = null;

        public MapDataAccessor(PositionalGetter<?, ArrayData> keyGetter,
                               PositionalGetter<?, ArrayData> valueGetter) {
            this.keyAccessor = new ArrayDataAccessor(keyGetter);
            this.valueAccessor = new ArrayDataAccessor(valueGetter);
        }

        public MapData wrap(ArrayData keyData, ArrayData valueData) {
            this.keyData = keyAccessor.wrap(keyData);
            this.valueData = valueAccessor.wrap(valueData);
            return this;
        }

        @Override
        public int size() {
            return keyData.size();
        }

        @Override
        public ArrayData keyArray() {
            return keyAccessor.wrap(keyData);
        }

        @Override
        public ArrayData valueArray() {
            return valueAccessor.wrap(valueData);
        }
    }

    private static class RowDataVisitor extends FlinkSchemaVisitor<PositionalGetter<?, RowData>> {
        private RowDataVisitor() {
        }

        @Override
        public PositionalGetter<?, RowData> record(Types.StructType iStruct, List<PositionalGetter<?, RowData>> results, List<LogicalType> fieldTypes) {
            RowDataAccessor accessor = new RowDataAccessor(results.toArray(new PositionalGetter[0]));
            return ((data, pos) -> {
//                RowData row = data.getRow(pos, iStruct.fields().size());
                return accessor.wrap(data);
            });
        }

        @Override
        public PositionalGetter<?, RowData> list(Types.ListType iList, PositionalGetter<?, RowData> getter, LogicalType elementType) {
            ArrayDataAccessor accessor = new ArrayDataAccessor(arrayPrimitive(iList.elementType(), getter, elementType));
            return (data, pos) -> {
                ArrayData arrayData = data.getArray(pos);
                return accessor.wrap(arrayData);
            };
        }

        @Override
        public PositionalGetter<?, RowData> map(Types.MapType iMap, PositionalGetter<?, RowData> keyGetter, PositionalGetter<?, RowData> valueGetter, LogicalType keyType, LogicalType valueType) {
            MapDataAccessor accessor = new MapDataAccessor(arrayPrimitive(iMap.keyType(), keyGetter, keyType), arrayPrimitive(iMap.valueType(), valueGetter, valueType));
            return (data, pos) -> {
                MapData mapData = data.getMap(pos);
                ArrayData keyData = mapData.keyArray();
                ArrayData valueData = mapData.valueArray();
                return accessor.wrap(keyData, valueData);
            };
        }

        @Override
        public PositionalGetter<?, RowData> primitive(Type.PrimitiveType type, LogicalType logicalType) {
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
                    return (row, pos) -> row.getInt(pos);
                case TIMESTAMP:
                    TimestampType timestampType = (TimestampType) logicalType;
                    return (row, pos) -> row.getTimestamp(pos, timestampType.getPrecision());
                case TIMESTAMP_NANO:
                    TimestampType nanoTimestampType = (TimestampType) logicalType;
                    return (row, pos) ->
                            row.getTimestamp(pos, nanoTimestampType.getPrecision());
                default:
                    throw new UnsupportedOperationException("Not supported type: " + type.typeId());
            }
        }

        private PositionalGetter<?, ArrayData> arrayPrimitive(Type type, PositionalGetter<?, RowData> getter, LogicalType logicalType) {
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
                    return (row, pos) -> row.getInt(pos);
                case TIMESTAMP:
                    TimestampType timestampType = (TimestampType) logicalType;
                    return (row, pos) -> row.getTimestamp(pos, timestampType.getPrecision());
                case TIMESTAMP_NANO:
                    TimestampType nanoTimestampType = (TimestampType) logicalType;
                    return (row, pos) ->
                            row.getTimestamp(pos, nanoTimestampType.getPrecision());
                case LIST:
                    Types.ListType listType = type.asListType();
                    ArrayType arrayType = (ArrayType) logicalType;
                    ArrayDataAccessor arrayDataAccessor = new ArrayDataAccessor(arrayPrimitive(listType.elementType(), getter, arrayType.getElementType()));
                    return (row, pos) -> {
                        ArrayData arrayData = row.getArray(pos);
                        return arrayDataAccessor.wrap(arrayData);
                    };
                case STRUCT:
                    Types.StructType structType = type.asStructType();
                    RowType rowType = (RowType) logicalType;
//                    RowDataAccessor rowDataAccessor = new RowDataAccessor(arrayPrimitive(listType.elementType(), getter, arrayType.getElementType()));
                    return (row, pos) -> {
                        ArrayData arrayData = row.getArray(pos);
                        if (rowType == null) {
                            return null;
                        }
                        if (structType == null) {
                            return null;
                        }
                        if (arrayData == null) {
                            return null;
                        }
                        return null;//arrayDataAccessor.wrap(arrayData);
                    };

                default:
                    throw new UnsupportedOperationException("Not supported type: " + type.typeId());
            }
        }
    }
}
