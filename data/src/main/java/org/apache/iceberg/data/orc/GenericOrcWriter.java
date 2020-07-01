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

package org.apache.iceberg.data.orc;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericOrcWriter extends BaseOrcWriter<Record> {

  protected GenericOrcWriter(TypeDescription schema) {
    super(schema);
  }

  public static OrcValueWriter<Record> buildWriter(TypeDescription fileSchema) {
    return new GenericOrcWriter(fileSchema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Record value, VectorizedRowBatch output) {
    int row = output.size++;
    Converter[] converters = getConverters();
    for (int c = 0; c < converters.length; ++c) {
      converters[c].addValue(row, value.get(c, converters[c].getJavaClass()), output.cols[c]);
    }
  }

  @Override
  protected Converter<Record> createStructConverter(TypeDescription schema) {
    return new StructConverter(schema);
  }

  private class StructConverter implements Converter<Record> {
    private final Converter[] children;

    StructConverter(TypeDescription schema) {
      this.children = new Converter[schema.getChildren().size()];
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(schema.getChildren().get(c));
      }
    }

    @Override
    public Class<Record> getJavaClass() {
      return Record.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int rowId, Record data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        StructColumnVector cv = (StructColumnVector) output;
        for (int c = 0; c < children.length; ++c) {
          children[c].addValue(rowId, data.get(c, children[c].getJavaClass()), cv.fields[c]);
        }
      }
    }
  }
}
