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
import org.apache.avro.Schema;
import org.apache.flink.types.Row;
import org.apache.iceberg.avro.AvroSchemaWithTypeVisitor;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

public class FlinkAvroReader extends DataReader<Row> {

  public FlinkAvroReader(org.apache.iceberg.Schema expectedSchema, Schema readSchema) {
    super(expectedSchema, readSchema, ImmutableMap.of());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected ValueReader<Row> initReader(org.apache.iceberg.Schema expectedSchema,
                                        Schema readSchema,
                                        Map<Integer, ?> idToConstant) {
    return (ValueReader<Row>) AvroSchemaWithTypeVisitor.visit(expectedSchema, readSchema,
        new ReadBuilder(idToConstant));
  }

  private static class ReadBuilder extends DataReader.ReadBuilder {

    ReadBuilder(Map<Integer, ?> idToConstant) {
      super(idToConstant);
    }

    @Override
    public ValueReader<?> record(Types.StructType struct, Schema record,
                                 List<String> names, List<ValueReader<?>> fields) {
      return new RowReader(fields, struct, getIdToConstant());
    }
  }

  private static class RowReader extends ValueReaders.StructReader<Row> {
    private final Types.StructType structType;

    private RowReader(List<ValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.structType = struct;
    }

    @Override
    protected Row reuseOrCreate(Object reuse) {
      if (reuse instanceof Row) {
        return (Row) reuse;
      } else {
        return new Row(structType.fields().size());
      }
    }

    @Override
    protected Object get(Row row, int pos) {
      return row.getField(pos);
    }

    @Override
    protected void set(Row row, int pos, Object value) {
      row.setField(pos, value);
    }
  }
}
