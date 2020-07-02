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
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.parquet.BaseParquetReaders;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class FlinkParquetReaders extends BaseParquetReaders<Row> {

  private static final FlinkParquetReaders INSTANCE = new FlinkParquetReaders();

  private FlinkParquetReaders() {
  }

  public static ParquetValueReader<Row> buildReader(Schema expectedSchema, MessageType fileSchema) {
    return INSTANCE.createReader(expectedSchema, fileSchema);
  }

  @Override
  protected ParquetValueReader<Row> createStructReader(List<Type> types,
                                                       List<ParquetValueReader<?>> fieldReaders,
                                                       Types.StructType structType) {
    return new RowReader(types, fieldReaders, structType);
  }

  private static class RowReader extends ParquetValueReaders.StructReader<Row, Row> {
    private final Types.StructType structType;

    RowReader(List<Type> types, List<ParquetValueReader<?>> readers, Types.StructType struct) {
      super(types, readers);
      this.structType = struct;
    }

    @Override
    protected Row newStructData(Row reuse) {
      if (reuse != null) {
        return reuse;
      } else {
        return new Row(structType.fields().size());
      }
    }

    @Override
    protected Object getField(Row row, int pos) {
      return row.getField(pos);
    }

    @Override
    protected Row buildStruct(Row row) {
      return row;
    }

    @Override
    protected void set(Row row, int pos, Object value) {
      row.setField(pos, value);
    }
  }
}
