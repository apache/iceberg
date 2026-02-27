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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

public class VariantRowDataWrapper implements RowData {

  private final RowType rowType;
  private RowKind rowKind;
  private Variant variantData;

  public VariantRowDataWrapper(RowType rowType) {
    this(rowType, RowKind.INSERT);
  }

  public VariantRowDataWrapper(RowType rowType, RowKind rowKind) {
    this.rowType = rowType;
    this.rowKind = rowKind;
  }

  public VariantRowDataWrapper wrap(Variant variant) {
    this.variantData = variant;
    return this;
  }

  @Override
  public int getArity() {
    return rowType.getFieldCount();
  }

  @Override
  public RowKind getRowKind() {
    return rowKind;
  }

  @Override
  public void setRowKind(RowKind rowKind) {
    this.rowKind = rowKind;
  }

  @Override
  public boolean isNullAt(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    Variant value = variantData.getField(fieldName);
    return value == null || value.isNull();
  }

  @Override
  public boolean getBoolean(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return variantData.getField(fieldName).getBoolean();
  }

  @Override
  public byte getByte(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return variantData.getField(fieldName).getByte();
  }

  @Override
  public short getShort(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return variantData.getField(fieldName).getShort();
  }

  @Override
  public int getInt(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return getIntValue(variantData.getField(fieldName));
  }

  @Override
  public long getLong(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return getLongValue(variantData.getField(fieldName));
  }

  @Override
  public float getFloat(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return variantData.getField(fieldName).getFloat();
  }

  @Override
  public double getDouble(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return getDoubleValue(variantData.getField(fieldName));
  }

  @Override
  public StringData getString(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return isNullAt(pos)
        ? null
        : StringData.fromString(variantData.getField(fieldName).getString());
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    String fieldName = getFieldNameByIndex(pos);
    return isNullAt(pos)
        ? null
        : DecimalData.fromBigDecimal(
            variantData.getField(fieldName).getDecimal(), precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    String fieldName = getFieldNameByIndex(pos);
    return isNullAt(pos) ? null : getTimestamp(variantData.getField(fieldName));
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    throw new UnsupportedOperationException("RawValue in Variant column is not supported.");
  }

  @Override
  public byte[] getBinary(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return variantData.getField(fieldName).getBytes();
  }

  @Override
  public ArrayData getArray(int pos) {
    //    ArrayType arrayType = (ArrayType) rowType.getTypeAt(pos);
    //    LogicalType elementType = arrayType.getElementType();
    //
    //    String fieldName = getFieldNameByIndex(pos);
    //    Variant arrayVariant = variantData.getField(fieldName);
    //
    //    if (arrayVariant != null) {
    //        Preconditions.checkArgument(arrayVariant.getType().equals(Variant.Type.ARRAY),
    //                "Expected Array type, but got " + arrayVariant.getType());
    //        int arraySize = arrayVariant.getArraySize();
    //        Object[] elements = new Object[arraySize];
    //
    //        for (int i=0;i< arraySize;i++) {
    //            Variant element = arrayVariant.getElement(i);
    //            elements[i] = element == null ? null : getElementValue(elementType, element);
    //        }
    //
    //        return new GenericArrayData(elements);
    //    }
    //
    //    return null;
    throw new UnsupportedOperationException("Array Type is not supported yet.");
  }

  @Override
  public MapData getMap(int pos) {
    //    MapType mapType = (MapType) rowType.getTypeAt(pos);
    //    LogicalType keyType = mapType.getKeyType();
    //    LogicalType valueType = mapType.getValueType();
    //
    //    Preconditions.checkArgument(keyType instanceof VarCharType,
    //            "Map with STRING key types are only supported in Variant to RowData conversion");
    //
    //    String mapFieldName = getFieldNameByIndex(pos);
    //    Variant mapVariant = variantData.getField(mapFieldName);
    //
    //    Map<Object, Object> mapData = Maps.newHashMap();
    //    if (mapVariant != null) {
    //        List<String> keys = mapVariant.getFieldNames();
    //        for (String key: keys) {
    //            mapData.put(StringData.fromString(key), getElementValue(valueType,
    // mapVariant.getField(key)));
    //        }
    //
    //        return new GenericMapData(mapData);
    //    }
    //
    //    return null;
    throw new UnsupportedOperationException("Map Type is not supported yet.");
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    String fieldName = getFieldNameByIndex(pos);
    VariantRowDataWrapper rowDataWrapper =
        new VariantRowDataWrapper((RowType) rowType.getTypeAt(pos));
    return rowDataWrapper.wrap(variantData.getField(fieldName));
  }

  @Override
  public Variant getVariant(int pos) {
    String fieldName = getFieldNameByIndex(pos);
    return variantData.getField(fieldName);
  }

  /* private Object getElementValue(LogicalType elementType, Variant variant) {
    LogicalTypeRoot root = elementType.getTypeRoot();

    switch (root) {
      case NULL:
        return null;
      case BOOLEAN:
        return variant.getBoolean();
      case TINYINT:
        return variant.getByte();
      case SMALLINT:
        return variant.getShort();
      case INTEGER:
        return getIntValue(variant);
      case BIGINT:
        return getLongValue(variant);
      case FLOAT:
        return variant.getFloat();
      case DOUBLE:
        return getDoubleValue(variant);
      case DECIMAL:
        DecimalType decimalType = (DecimalType) elementType;
        return DecimalData.fromBigDecimal(
            variant.getDecimal(), decimalType.getPrecision(), decimalType.getScale());
      case CHAR:
      case VARCHAR:
        return StringData.fromString(variant.getString());
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return getTimestamp(variant);
      case BINARY:
      case VARBINARY:
        return variant.getBytes();
      default:
        throw new UnsupportedOperationException(
            "Unsupported Element type in Array/Map type:" + elementType);
    }
  }*/

  private int getIntValue(Variant variant) {
    switch (variant.getType()) {
      case TINYINT:
        return variant.getByte();
      case SMALLINT:
        return variant.getShort();
      case INT:
        return variant.getInt();
      default:
        throw new UnsupportedOperationException(errMsg(variant, "int"));
    }
  }

  private long getLongValue(Variant variant) {
    switch (variant.getType()) {
      case TINYINT:
        return variant.getByte();
      case SMALLINT:
        return variant.getShort();
      case INT:
        return variant.getInt();
      case BIGINT:
        return variant.getLong();
      default:
        throw new UnsupportedOperationException(errMsg(variant, "long"));
    }
  }

  private double getDoubleValue(Variant variant) {
    switch (variant.getType()) {
      case FLOAT:
        return variant.getFloat();
      case DOUBLE:
        return variant.getDouble();
      default:
        throw new UnsupportedOperationException(errMsg(variant, "double"));
    }
  }

  private TimestampData getTimestamp(Variant variant) {
    switch (variant.getType()) {
      case TIMESTAMP:
        return TimestampData.fromLocalDateTime(variant.getDateTime());
      case TIMESTAMP_LTZ:
        return TimestampData.fromInstant(variant.getInstant());
      case BIGINT:
        long timeLong = variant.getLong();
        return TimestampData.fromEpochMillis(timeLong / 1000, (int) (timeLong % 1000) * 1000);
      default:
        throw new UnsupportedOperationException(errMsg(variant, "timestamp"));
    }
  }

  private static String errMsg(Variant variant, String type) {
    return String.format("Failed to read Variant %s as %s", variant.toJson(), type);
  }

  private String getFieldNameByIndex(int pos) {
    return rowType.getFields().get(pos).getName();
  }
}
