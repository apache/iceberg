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

package org.apache.iceberg.orc;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

class RowFilterValueReader implements OrcRowReader<Object[]> {

  private final Converter[] converters;
  private final List<TypeDescription> columns;
  private final int[] filterSchemaToReadSchemaColumnIndex;

  RowFilterValueReader(TypeDescription readSchema, TypeDescription filterSchema) {
    columns = filterSchema.getChildren();
    converters = buildConverters();
    filterSchemaToReadSchemaColumnIndex = buildFilterSchemaToReadSchemaColumnIndex(readSchema, filterSchema);
  }

  private int[] buildFilterSchemaToReadSchemaColumnIndex(TypeDescription readSchema, TypeDescription filterSchema) {
    int[] index = new int[filterSchema.getChildren().size()];
    List<String> filterFieldNames = filterSchema.getFieldNames();
    List<String> readSchemaFieldNames = readSchema.getFieldNames();
    Map<String, Integer> readSchemaFieldNameToIndex = new HashMap<>();
    for (int i = 0; i < readSchemaFieldNames.size(); i++) {
      readSchemaFieldNameToIndex.put(readSchemaFieldNames.get(i), i);
    }
    for (int i = 0; i < filterFieldNames.size(); i++) {
      index[i] = readSchemaFieldNameToIndex.get(filterFieldNames.get(i));
    }
    return index;
  }

  @Override
  public Object[] read(VectorizedRowBatch batch, int row) {
    Object[] rowFields = new Object[converters.length];
    for (int c = 0; c < converters.length; ++c) {
      rowFields[c] = converters[c].convert(batch.cols[filterSchemaToReadSchemaColumnIndex[c]], row);
    }
    return rowFields;
  }

  @Override
  public void setBatchContext(long batchOffsetInFile) {
  }

  interface Converter<T> {
    default T convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return convertNonNullValue(vector, rowIndex);
      }
    }

    T convertNonNullValue(ColumnVector vector, int row);
  }

  private Converter[] buildConverters() {
    Converter[] newConverters = new Converter[columns.size()];
    for (int c = 0; c < newConverters.length; ++c) {
      newConverters[c] = buildConverter(columns.get(c));
    }
    return newConverters;
  }

  private static Converter buildConverter(final TypeDescription schema) {
    switch (schema.getCategory()) {
      case INT:
        return new IntConverter();
      case LONG:
        String longAttributeValue = schema.getAttributeValue(ORCSchemaUtil.ICEBERG_LONG_TYPE_ATTRIBUTE);
        ORCSchemaUtil.LongType longType = longAttributeValue == null ? ORCSchemaUtil.LongType.LONG :
            ORCSchemaUtil.LongType.valueOf(longAttributeValue);
        switch (longType) {
          case LONG:
            return new LongConverter();
          default:
            throw new IllegalStateException("Unhandled Long type found in ORC type attribute: " + longType);
        }
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringConverter();
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  private static class IntConverter implements Converter<Integer> {
    @Override
    public Integer convertNonNullValue(ColumnVector vector, int row) {
      return (int) ((LongColumnVector) vector).vector[row];
    }
  }

  private static class LongConverter implements Converter<Long> {
    @Override
    public Long convertNonNullValue(ColumnVector vector, int row) {
      return ((LongColumnVector) vector).vector[row];
    }
  }

  private static class StringConverter implements Converter<String> {
    @Override
    public String convertNonNullValue(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      return new String(bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row],
          StandardCharsets.UTF_8);
    }
  }
}
