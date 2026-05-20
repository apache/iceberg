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
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.BinaryVariantAccessorUtils;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class VariantRowDataWrapper implements RowData {

  private static final int MICROSECOND_PRECISION = 6;

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
    return isNull(field(pos));
  }

  @Override
  public boolean getBoolean(int pos) {
    return field(pos).getBoolean();
  }

  @Override
  public byte getByte(int pos) {
    return field(pos).getByte();
  }

  @Override
  public short getShort(int pos) {
    return field(pos).getShort();
  }

  @Override
  public int getInt(int pos) {
    return intValue(field(pos));
  }

  @Override
  public long getLong(int pos) {
    return longValue(field(pos));
  }

  @Override
  public float getFloat(int pos) {
    return field(pos).getFloat();
  }

  @Override
  public double getDouble(int pos) {
    return doubleValue(field(pos));
  }

  @Override
  public StringData getString(int pos) {
    Variant value = field(pos);
    return isNull(value) ? null : StringData.fromString(value.getString());
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    Variant value = field(pos);
    return isNull(value) ? null : DecimalData.fromBigDecimal(value.getDecimal(), precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    Variant value = field(pos);
    return isNull(value) ? null : timestampValue(value, precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    throw new UnsupportedOperationException("RawValue in Variant column is not supported.");
  }

  @Override
  public byte[] getBinary(int pos) {
    Variant value = field(pos);
    return isNull(value) ? null : value.getBytes();
  }

  @Override
  public ArrayData getArray(int pos) {
    ArrayType arrayType = (ArrayType) rowType.getTypeAt(pos);
    LogicalType elementType = arrayType.getElementType();
    Variant arrayVariant = field(pos);
    return arrayDataValue(arrayVariant, elementType);
  }

  @Override
  public MapData getMap(int pos) {
    MapType mapType = (MapType) rowType.getTypeAt(pos);
    Variant mapVariant = field(pos);
    return mapDataValue(mapVariant, mapType);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return new VariantRowDataWrapper((RowType) rowType.getTypeAt(pos)).wrap(field(pos));
  }

  @Override
  public Variant getVariant(int pos) {
    return field(pos);
  }

  private static Object elementValue(Variant variant, LogicalType elementType) {
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
      case TIMESTAMP_WITHOUT_TIME_ZONE ->
          timestampValue(variant, ((TimestampType) elementType).getPrecision());
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          timestampValue(variant, ((LocalZonedTimestampType) elementType).getPrecision());
      case BINARY, VARBINARY -> variant.getBytes();
      case ARRAY -> arrayDataValue(variant, ((ArrayType) elementType).getElementType());
      case MAP -> mapDataValue(variant, (MapType) elementType);
      case ROW -> new VariantRowDataWrapper((RowType) elementType).wrap(variant);
      default ->
          throw new UnsupportedOperationException(
              String.format("Unsupported element type for Array or Map: %s", elementType));
    };
  }

  private static MapData mapDataValue(Variant variant, MapType mapType) {
    if (variant == null) {
      return null;
    }

    LogicalType keyType = mapType.getKeyType();
    LogicalType valueType = mapType.getValueType();

    Preconditions.checkArgument(
        keyType instanceof VarCharType,
        "Only Map with STRING key type is supported in Variant to RowData conversion");

    Map<Object, Object> mapData = Maps.newHashMap();
    List<String> keys = BinaryVariantAccessorUtils.fieldNames(variant);
    for (String key : keys) {
      mapData.put(StringData.fromString(key), elementValue(variant.getField(key), valueType));
    }

    return new GenericMapData(mapData);
  }

  private static ArrayData arrayDataValue(Variant variant, LogicalType innerElementType) {
    if (variant == null) {
      return null;
    }

    int arraySize = BinaryVariantAccessorUtils.arraySize(variant);
    Object[] elements = new Object[arraySize];

    for (int i = 0; i < arraySize; i++) {
      Variant element = variant.getElement(i);
      elements[i] = element == null ? null : elementValue(element, innerElementType);
    }

    return new GenericArrayData(elements);
  }

  private static int intValue(Variant variant) {
    return switch (variant.getType()) {
      case TINYINT -> variant.getByte();
      case SMALLINT -> variant.getShort();
      case INT -> variant.getInt();
      default -> throw new UnsupportedOperationException(errMsg(variant, "int"));
    };
  }

  private static long longValue(Variant variant) {
    return switch (variant.getType()) {
      case TINYINT -> variant.getByte();
      case SMALLINT -> variant.getShort();
      case INT -> variant.getInt();
      case BIGINT -> variant.getLong();
      default -> throw new UnsupportedOperationException(errMsg(variant, "long"));
    };
  }

  private static double doubleValue(Variant variant) {
    return switch (variant.getType()) {
      case FLOAT -> variant.getFloat();
      case DOUBLE -> variant.getDouble();
      default -> throw new UnsupportedOperationException(errMsg(variant, "double"));
    };
  }

  private static DecimalData decimalDataValue(Variant variant, DecimalType decimalType) {
    return DecimalData.fromBigDecimal(
        variant.getDecimal(), decimalType.getPrecision(), decimalType.getScale());
  }

  private static TimestampData timestampValue(Variant variant, int precision) {
    return switch (variant.getType()) {
      case TIMESTAMP -> TimestampData.fromLocalDateTime(variant.getDateTime());
      case TIMESTAMP_LTZ -> TimestampData.fromInstant(variant.getInstant());
      case BIGINT ->
          precision > MICROSECOND_PRECISION
              ? nanoTimestampValue(variant.getLong())
              : microTimestampValue(variant.getLong());
      default -> throw new UnsupportedOperationException(errMsg(variant, "timestamp"));
    };
  }

  private static TimestampData microTimestampValue(long micros) {
    return TimestampData.fromEpochMillis(micros / 1000, (int) (micros % 1000) * 1000);
  }

  private static TimestampData nanoTimestampValue(long nanos) {
    return TimestampData.fromEpochMillis(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
  }

  private Variant field(int position) {
    String fieldName = rowType.getFields().get(position).getName();
    return variantData.getField(fieldName);
  }

  private static boolean isNull(Variant value) {
    return value == null || value.isNull();
  }

  private static String errMsg(Variant variant, String type) {
    return String.format(
        "Failed to read Variant %s of type %s as %s", variant.toJson(), variant.getType(), type);
  }
}
