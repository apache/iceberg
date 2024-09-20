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
package org.apache.iceberg.data.parquet;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders.StructReader;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class GenericParquetReaders extends BaseParquetReaders<Record> {

  private static final GenericParquetReaders INSTANCE = new GenericParquetReaders();

  private GenericParquetReaders() {}

  public static ParquetValueReader<Record> buildReader(
      Schema expectedSchema, MessageType fileSchema) {
    return INSTANCE.createReader(expectedSchema, fileSchema);
  }

  public static ParquetValueReader<Record> buildReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return INSTANCE.createReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  protected ParquetValueReader<Record> createStructReader(
      List<Type> types, List<ParquetValueReader<?>> fieldReaders, StructType structType) {
    return new RecordReader(types, fieldReaders, structType);
  }

  @Override
  protected ParquetValueReader<Record> createVariantReader(
      List<ParquetValueReader<?>> fieldReaders) {
    return new VariantReader(fieldReaders);
  }

  private static class RecordReader extends StructReader<Record, Record> {
    private final GenericRecord template;

    RecordReader(List<Type> types, List<ParquetValueReader<?>> readers, StructType struct) {
      super(types, readers);
      this.template = struct != null ? GenericRecord.create(struct) : null;
    }

    @Override
    protected Record newStructData(Record reuse) {
      if (reuse != null) {
        return reuse;
      } else {
        // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since
        // NAME_MAP_CACHE access
        // is eliminated. Using copy here to gain performance.
        return template.copy();
      }
    }

    @Override
    protected Object getField(Record intermediate, int pos) {
      return intermediate.get(pos);
    }

    @Override
    protected Record buildStruct(Record struct) {
      return struct;
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }

  /**
   * Variant reader to read Value and Metadata binaries from Parquet file and convert to a record.
   * TODO: Aihua return A record to model Variant data.
   */
  public static class VariantReader extends RecordReader {
    private static final List<Type> types =
        List.of(
            new PrimitiveType(
                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "Value"),
            new PrimitiveType(
                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "Metadata"));

    VariantReader(List<ParquetValueReader<?>> readers) {
      super(types, readers, null);
    }

    @Override
    protected Record buildStruct(Record struct) {
      // struct is of Value + Metadata binaries
      return struct;
    }
  }
}
