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
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class FlinkParquetReaders {
  private FlinkParquetReaders() {

  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<Row> buildReader(Schema expectedSchema,
                                                    MessageType fileSchema) {
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      return (ParquetValueReader<Row>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new ReadBuilder(fileSchema, ImmutableMap.of()));
    } else {
      return (ParquetValueReader<Row>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new FallbackReadBuilder(fileSchema, ImmutableMap.of()));
    }
  }

  private static class FallbackReadBuilder extends GenericParquetReaders.FallbackReadBuilder {

    private FallbackReadBuilder(MessageType type, Map<Integer, ?> idToConstant) {
      super(type, idToConstant);
    }

    @Override
    protected ParquetValueReaders.StructReader<?, ?> createStructReader(List<Type> types,
                                                                        List<ParquetValueReader<?>> readers,
                                                                        Types.StructType struct) {
      return new RowReader(types, readers, struct);
    }
  }

  private static class ReadBuilder extends GenericParquetReaders.ReadBuilder {

    private ReadBuilder(MessageType type, Map<Integer, ?> idToConstant) {
      super(type, idToConstant);
    }

    @Override
    protected ParquetValueReaders.StructReader<Row, Row> createStructReader(List<Type> types,
                                                                            List<ParquetValueReader<?>> readers,
                                                                            Types.StructType struct) {
      return new RowReader(types, readers, struct);
    }
  }

  static class RowReader extends ParquetValueReaders.StructReader<Row, Row> {
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
