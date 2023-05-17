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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class RowDataProjection implements RowData {
  /**
   * Creates a projecting wrapper for {@link RowData} rows.
   *
   * <p>This projection will not project the nested children types of repeated types like lists and
   * maps.
   *
   * @param schema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @return a wrapper to project rows
   */
  public static RowDataProjection create(Schema schema, Schema projectedSchema) {
    return RowDataProjection.create(
        FlinkSchemaUtil.convert(schema), schema.asStruct(), projectedSchema.asStruct());
  }

  /**
   * Creates a projecting wrapper for {@link RowData} rows.
   *
   * <p>This projection will not project the nested children types of repeated types like lists and
   * maps.
   *
   * @param rowType flink row type of rows wrapped by this projection
   * @param schema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @return a wrapper to project rows
   */
  public static RowDataProjection create(
      RowType rowType, Types.StructType schema, Types.StructType projectedSchema) {
    return new RowDataProjection(rowType, schema, projectedSchema);
  }

  private final RowData.FieldGetter[] getters;
  private RowData rowData;

  private RowDataProjection(
      RowType rowType, Types.StructType rowStruct, Types.StructType projectType) {
    Map<Integer, Integer> fieldIdToPosition = Maps.newHashMap();
    for (int i = 0; i < rowStruct.fields().size(); i++) {
      fieldIdToPosition.put(rowStruct.fields().get(i).fieldId(), i);
    }

    this.getters = new RowData.FieldGetter[projectType.fields().size()];
    for (int i = 0; i < getters.length; i++) {
      Types.NestedField projectField = projectType.fields().get(i);
      Types.NestedField rowField = rowStruct.field(projectField.fieldId());

      Preconditions.checkNotNull(
          rowField,
          "Cannot locate the project field <%s> in the iceberg struct <%s>",
          projectField,
          rowStruct);

      getters[i] =
          createFieldGetter(
              rowType, fieldIdToPosition.get(projectField.fieldId()), rowField, projectField);
    }
  }

  private static RowData.FieldGetter createFieldGetter(
      RowType rowType, int position, Types.NestedField rowField, Types.NestedField projectField) {
    Preconditions.checkArgument(
        rowField.type().typeId() == projectField.type().typeId(),
        "Different iceberg type between row field <%s> and project field <%s>",
        rowField,
        projectField);

    switch (projectField.type().typeId()) {
      case STRUCT:
        RowType nestedRowType = (RowType) rowType.getTypeAt(position);
        return row -> {
          // null nested struct value
          if (row.isNullAt(position)) {
            return null;
          }

          RowData nestedRow = row.getRow(position, nestedRowType.getFieldCount());
          return RowDataProjection.create(
                  nestedRowType, rowField.type().asStructType(), projectField.type().asStructType())
              .wrap(nestedRow);
        };

      case MAP:
        Types.MapType projectedMap = projectField.type().asMapType();
        Types.MapType originalMap = rowField.type().asMapType();

        boolean keyProjectable =
            !projectedMap.keyType().isNestedType()
                || projectedMap.keyType().equals(originalMap.keyType());
        boolean valueProjectable =
            !projectedMap.valueType().isNestedType()
                || projectedMap.valueType().equals(originalMap.valueType());
        Preconditions.checkArgument(
            keyProjectable && valueProjectable,
            "Cannot project a partial map key or value with non-primitive type. Trying to project <%s> out of <%s>",
            projectField,
            rowField);

        return RowData.createFieldGetter(rowType.getTypeAt(position), position);

      case LIST:
        Types.ListType projectedList = projectField.type().asListType();
        Types.ListType originalList = rowField.type().asListType();

        boolean elementProjectable =
            !projectedList.elementType().isNestedType()
                || projectedList.elementType().equals(originalList.elementType());
        Preconditions.checkArgument(
            elementProjectable,
            "Cannot project a partial list element with non-primitive type. Trying to project <%s> out of <%s>",
            projectField,
            rowField);

        return RowData.createFieldGetter(rowType.getTypeAt(position), position);

      default:
        return RowData.createFieldGetter(rowType.getTypeAt(position), position);
    }
  }

  public RowData wrap(RowData row) {
    // StructProjection allow wrapping null root struct object.
    // See more discussions in https://github.com/apache/iceberg/pull/7517.
    // RowDataProjection never allowed null root object to be wrapped.
    // Hence, it is fine to enforce strict Preconditions check here.
    Preconditions.checkArgument(row != null, "Invalid row data: null");
    this.rowData = row;
    return this;
  }

  private Object getValue(int pos) {
    Preconditions.checkState(rowData != null, "Row data not wrapped");
    return getters[pos].getFieldOrNull(rowData);
  }

  @Override
  public int getArity() {
    return getters.length;
  }

  @Override
  public RowKind getRowKind() {
    Preconditions.checkState(rowData != null, "Row data not wrapped");
    return rowData.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    throw new UnsupportedOperationException("Cannot set row kind in the RowDataProjection");
  }

  @Override
  public boolean isNullAt(int pos) {
    return getValue(pos) == null;
  }

  @Override
  public boolean getBoolean(int pos) {
    return (boolean) getValue(pos);
  }

  @Override
  public byte getByte(int pos) {
    return (byte) getValue(pos);
  }

  @Override
  public short getShort(int pos) {
    return (short) getValue(pos);
  }

  @Override
  public int getInt(int pos) {
    return (int) getValue(pos);
  }

  @Override
  public long getLong(int pos) {
    return (long) getValue(pos);
  }

  @Override
  public float getFloat(int pos) {
    return (float) getValue(pos);
  }

  @Override
  public double getDouble(int pos) {
    return (double) getValue(pos);
  }

  @Override
  public StringData getString(int pos) {
    return (StringData) getValue(pos);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return (DecimalData) getValue(pos);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return (TimestampData) getValue(pos);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RawValueData<T> getRawValue(int pos) {
    return (RawValueData<T>) getValue(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return (byte[]) getValue(pos);
  }

  @Override
  public ArrayData getArray(int pos) {
    return (ArrayData) getValue(pos);
  }

  @Override
  public MapData getMap(int pos) {
    return (MapData) getValue(pos);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return (RowData) getValue(pos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof RowDataProjection)) {
      return false;
    }

    RowDataProjection that = (RowDataProjection) o;
    return deepEquals(that);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(getRowKind());
    for (int pos = 0; pos < getArity(); pos++) {
      if (!isNullAt(pos)) {
        // Arrays.deepHashCode handles array object properly
        result = 31 * result + Arrays.deepHashCode(new Object[] {getValue(pos)});
      }
    }

    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getRowKind().shortString()).append("(");
    for (int pos = 0; pos < getArity(); pos++) {
      if (pos != 0) {
        sb.append(",");
      }
      // copied the behavior from Flink GenericRowData
      sb.append(StringUtils.arrayAwareToString(getValue(pos)));
    }

    sb.append(")");
    return sb.toString();
  }

  private boolean deepEquals(RowDataProjection other) {
    if (getRowKind() != other.getRowKind()) {
      return false;
    }

    if (getArity() != other.getArity()) {
      return false;
    }

    for (int pos = 0; pos < getArity(); ++pos) {
      if (isNullAt(pos) && other.isNullAt(pos)) {
        continue;
      }

      if ((isNullAt(pos) && !other.isNullAt(pos)) || (!isNullAt(pos) && other.isNullAt(pos))) {
        return false;
      }

      // Objects.deepEquals handles array object properly
      if (!Objects.deepEquals(getValue(pos), other.getValue(pos))) {
        return false;
      }
    }

    return true;
  }
}
