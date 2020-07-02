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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.BaseOrcReader;
import org.apache.iceberg.orc.OrcRowReader;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class FlinkOrcReader extends BaseOrcReader<Row> {

  private FlinkOrcReader(org.apache.iceberg.Schema expectedSchema,
                         TypeDescription readOrcSchema,
                         Map<Integer, ?> idToConstant) {
    super(expectedSchema, readOrcSchema, idToConstant);
  }

  public static OrcRowReader<Row> buildReader(Schema expectedSchema, TypeDescription fileSchema) {
    return buildReader(expectedSchema, fileSchema, Collections.emptyMap());
  }

  public static OrcRowReader<Row> buildReader(Schema expectedSchema,
                                              TypeDescription fileSchema,
                                              Map<Integer, ?> idToConstant) {
    return new FlinkOrcReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  protected OrcValueReader<Row> createStructReader(List<OrcValueReader<?>> fields,
                                                   Types.StructType expected,
                                                   Map<Integer, ?> idToConstant) {
    return new RowReader(fields, expected, idToConstant);
  }

  @Override
  public Row read(VectorizedRowBatch batch, int row) {
    return (Row) getReader().read(new StructColumnVector(batch.size, batch.cols), row);
  }

  private static class RowReader extends OrcValueReaders.StructReader<Row> {
    private final Types.StructType structType;

    private RowReader(List<OrcValueReader<?>> readers, Types.StructType structType, Map<Integer, ?> idToConstant) {
      super(readers, structType, idToConstant);
      this.structType = structType;
    }

    @Override
    protected Row create() {
      return new Row(structType.fields().size());
    }

    @Override
    protected void set(Row row, int pos, Object value) {
      row.setField(pos, value);
    }
  }
}
