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

import org.apache.flink.types.Row;
import org.apache.iceberg.data.orc.BaseOrcWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class FlinkOrcWriter extends BaseOrcWriter<Row> {

  protected FlinkOrcWriter(TypeDescription schema) {
    super(schema);
  }

  public static BaseOrcWriter<Row> buildWriter(TypeDescription fileSchema) {
    return new FlinkOrcWriter(fileSchema);
  }

  @Override
  protected Converter<Row> createStructConverter(TypeDescription schema) {
    return new RowConverter(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Row value, VectorizedRowBatch output) {
    int row = output.size++;
    Converter[] converters = getConverters();
    for (int c = 0; c < converters.length; ++c) {
      Class clazz = converters[c].getJavaClass();
      converters[c].addValue(row, clazz.cast(value.getField(c)), output.cols[c]);
    }
  }

  private class RowConverter implements Converter<Row> {
    private final Converter[] children;

    private RowConverter(TypeDescription schema) {
      this.children = new Converter[schema.getChildren().size()];
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(schema.getChildren().get(c));
      }
    }

    @Override
    public Class<Row> getJavaClass() {
      return Row.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int rowId, Row data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        StructColumnVector cv = (StructColumnVector) output;
        for (int c = 0; c < children.length; ++c) {
          Class childCls = children[c].getJavaClass();
          children[c].addValue(rowId, childCls.cast(data.getField(c)), cv.fields[c]);
        }
      }
    }
  }
}
