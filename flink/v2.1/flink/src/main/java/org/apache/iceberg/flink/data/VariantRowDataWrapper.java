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

import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.BinaryVariantAccessorUtils;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

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
    Variant value = fieldByIndex(pos);
    return value == null || value.isNull();
  }

  @Override
  public boolean getBoolean(int pos) {
    return fieldByIndex(pos).getBoolean();
  }

  @Override
  public byte getByte(int pos) {
    return fieldByIndex(pos).getByte();
  }

  @Override
  public short getShort(int pos) {
    return fieldByIndex(pos).getShort();
  }

  @Override
  public int getInt(int pos) {
    return intValue(fieldByIndex(pos));
  }

  @Override
  public long getLong(int pos) {
    return longValue(fieldByIndex(pos));
  }

  @Override
  public float getFloat(int pos) {
    return fieldByIndex(pos).getFloat();
  }

  @Override
  public double getDouble(int pos) {
    return doubleValue(fieldByIndex(pos));
  }

  @Override
  public StringData getString(int pos) {
    Variant value = fieldByIndex(pos);
    return value == null
        ? null
        : StringData.fromString(value.getString());
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    Variant value = fieldByIndex(pos);
    return value == null
        ? null
        : DecimalData.fromBigDecimal(
            value.getDecimal(), precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    Variant value = fieldByIndex(pos);
    return value == null ? null : timestampValue(value);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    throw new UnsupportedOperationException("RawValue in Variant column is not supported.");
  }

  @Override
  public byte[] getBinary(int pos) {
    return fieldByIndex(pos).getBytes();
  }

  @Override
  public ArrayData getArray(int pos) {
    ArrayType arrayType = (ArrayType) rowType.getTypeAt(pos);
    LogicalType elementType = arrayType.getElementType();
    Variant arrayVariant = fieldByIndex(pos);
    return arrayDataValue(arrayVariant, elementType);
  }

  @Override
  public MapData getMap(int pos) {
    MapType mapType = (MapType) rowType.getTypeAt(pos);
    Variant mapVariant = fieldByIndex(pos);
    return mapDataValue(mapVariant, mapType);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    VariantRowDataWrapper rowDataWrapper =
        new VariantRowDataWrapper((RowType) rowType.getTypeAt(pos));
    return rowDataWrapper.wrap(fieldByIndex(pos));
  }

  @Override
  public Variant getVariant(int pos) {
    return fieldByIndex(pos);
  }

  private Object element(Variant variant, LogicalType elementType) {
    LogicalTypeRoot root = elementType.getTypeRoot();

      return switch (root) {
          case NULL -> null;
          case BOOLEAN -> variant.getBoolean();
          case TINYINT -> variant.getByte();
          case SMALLINT -> variant.getShort();
          case INTEGER -> intValue(variant);
          case BIGINT -> longValue(variant);
          case FLOAT -> variant.getFloat();
          case DOUBLE -> doubleValue(variant);
          case DECIMAL -> decimalDataValue(variant, (DecimalType) elementType);
          case CHAR, VARCHAR -> StringData.fromString(variant.getString());
          case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> timestampValue(variant);
          case BINARY, VARBINARY -> variant.getBytes();
          case ARRAY -> arrayDataValue(variant, ((ArrayType) elementType).getElementType());
          case MAP -> mapDataValue(variant, (MapType) elementType);
          case ROW -> new VariantRowDataWrapper((RowType) elementType).wrap(variant);
          default -> throw new UnsupportedOperationException(
                  "Unsupported Element type in Array/Map type:" + elementType);
      };
  }

  private static DecimalData decimalDataValue(Variant variant, DecimalType decimalType) {
    return DecimalData.fromBigDecimal(
            variant.getDecimal(), decimalType.getPrecision(), decimalType.getScale());
  }

  private MapData mapDataValue(Variant variant, MapType mapType) {
    LogicalType keyType = mapType.getKeyType();
    LogicalType valueType = mapType.getValueType();

    Preconditions.checkArgument(
        keyType instanceof VarCharType,
        "Map with STRING key types are only supported in Variant to RowData conversion");

    Map<Object, Object> mapData = Maps.newHashMap();
    if (variant != null) {
      List<String> keys = BinaryVariantAccessorUtils.fieldNames(variant);
      for (String key : keys) {
        mapData.put(StringData.fromString(key), element(variant.getField(key), valueType));
      }

      return new GenericMapData(mapData);
    }

    return null;
  }

  private ArrayData arrayDataValue(Variant variant, LogicalType innerElementType) {
    if (variant != null) {
      Preconditions.checkArgument(
          variant.getType().equals(Variant.Type.ARRAY),
          "Expected Array type, but got " + variant.getType());
      int arraySize = BinaryVariantAccessorUtils.arraySize(variant);
      Object[] elements = new Object[arraySize];

      for (int i = 0; i < arraySize; i++) {
        Variant element = variant.getElement(i);
        elements[i] = element == null ? null : element(element, innerElementType);
      }

      return new GenericArrayData(elements);
    }

    return null;
  }

  private int intValue(Variant variant) {
      return switch (variant.getType()) {
          case TINYINT -> variant.getByte();
          case SMALLINT -> variant.getShort();
          case INT -> variant.getInt();
          default -> throw new UnsupportedOperationException(errMsg(variant, "int"));
      };
  }

  private long longValue(Variant variant) {
      return switch (variant.getType()) {
          case TINYINT -> variant.getByte();
          case SMALLINT -> variant.getShort();
          case INT -> variant.getInt();
          case BIGINT -> variant.getLong();
          default -> throw new UnsupportedOperationException(errMsg(variant, "long"));
      };
  }

  private double doubleValue(Variant variant) {
      return switch (variant.getType()) {
          case FLOAT -> variant.getFloat();
          case DOUBLE -> variant.getDouble();
          default -> throw new UnsupportedOperationException(errMsg(variant, "double"));
      };
  }

  private TimestampData timestampValue(Variant variant) {
      return switch (variant.getType()) {
          case TIMESTAMP -> TimestampData.fromLocalDateTime(variant.getDateTime());
          case TIMESTAMP_LTZ -> TimestampData.fromInstant(variant.getInstant());
          case BIGINT -> timestampDataValue(variant.getLong());
          default -> throw new UnsupportedOperationException(errMsg(variant, "timestamp"));
      };
  }

  private static TimestampData timestampDataValue(long timeLong) {
    return TimestampData.fromEpochMillis(timeLong / 1000, (int) (timeLong % 1000) * 1000);
  }

  private static String errMsg(Variant variant, String type) {
    return String.format(
        "Failed to read Variant %s of type %s as %s", variant.toJson(), variant.getType(), type);
  }

  private Variant fieldByIndex(int pos) {
    String fieldName = rowType.getFields().get(pos).getName();
    return variantData.getField(fieldName);
  }
}
