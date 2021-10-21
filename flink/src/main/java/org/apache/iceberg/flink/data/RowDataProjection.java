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
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class RowDataProjection implements RowData {

  private final RowData.FieldGetter[] getters;
  private RowData rowData;

  public static RowDataProjection create(Schema schema, Schema projectSchema) {
    return new RowDataProjection(FlinkSchemaUtil.convert(schema), schema.asStruct(), projectSchema.asStruct());
  }

  private RowDataProjection(RowType rowType, Types.StructType rowStruct, Types.StructType projectType) {
    this.getters = new RowData.FieldGetter[projectType.fields().size()];
    for (int i = 0; i < getters.length; i++) {
      getters[i] = createFieldGetter(rowType, rowStruct, projectType.fields().get(i));
    }
  }

  private static RowData.FieldGetter createFieldGetter(RowType rowType,
                                                       Types.StructType rowStruct,
                                                       Types.NestedField projectField) {
    for (int i = 0; i < rowStruct.fields().size(); i++) {
      Types.NestedField rowField = rowStruct.fields().get(i);
      if (rowField.fieldId() == projectField.fieldId()) {
        Preconditions.checkArgument(rowField.type().typeId() == projectField.type().typeId(),
            String.format("Different iceberg type between row field <%s> and project field <%s>",
                rowField, projectField));

        switch (projectField.type().typeId()) {
          case STRUCT:
            RowType nestedRowType = (RowType) rowType.getTypeAt(i);
            int rowPos = i;
            return row -> {
              RowData nestedRow = row.isNullAt(rowPos) ? null : row.getRow(rowPos, nestedRowType.getFieldCount());
              return new RowDataProjection(nestedRowType, rowField.type().asStructType(),
                  projectField.type().asStructType()).wrap(nestedRow);
            };

          case MAP:
            Types.MapType projectedMap = projectField.type().asMapType();
            Types.MapType originalMap = rowField.type().asMapType();

            boolean keyProjectable = !projectedMap.keyType().isNestedType() ||
                projectedMap.keyType().equals(originalMap.keyType());
            boolean valueProjectable = !projectedMap.valueType().isNestedType() ||
                projectedMap.valueType().equals(originalMap.valueType());
            Preconditions.checkArgument(keyProjectable && valueProjectable,
                "Cannot project a partial map key or value RowData. Trying to project %s out of %s",
                projectField, rowField);

            return RowData.createFieldGetter(rowType.getTypeAt(i), i);

          case LIST:
            Types.ListType projectedList = projectField.type().asListType();
            Types.ListType originalList = rowField.type().asListType();

            boolean elementProjectable = !projectedList.elementType().isNestedType() ||
                projectedList.elementType().equals(originalList.elementType());
            Preconditions.checkArgument(elementProjectable,
                "Cannot project a partial list element RowData. Trying to project %s out of %s",
                projectField, rowField);

            return RowData.createFieldGetter(rowType.getTypeAt(i), i);

          default:
            return RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }
      }
    }
    throw new IllegalArgumentException(String.format("Cannot find field %s in %s", projectField, rowStruct));
  }

  public RowData wrap(RowData row) {
    this.rowData = row;
    return this;
  }

  public Object getValue(int pos) {
    return getters[pos].getFieldOrNull(rowData);
  }

  @Override
  public int getArity() {
    return getters.length;
  }

  @Override
  public RowKind getRowKind() {
    return rowData.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    throw new UnsupportedOperationException("Cannot set row kind in the RowDataProjection");
  }

  @Override
  public boolean isNullAt(int pos) {
    return rowData == null || getValue(pos) == null;
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
}
