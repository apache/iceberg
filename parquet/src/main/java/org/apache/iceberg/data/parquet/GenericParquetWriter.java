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

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.parquet.schema.MessageType;

public class GenericParquetWriter extends BaseParquetWriter<Record> {
  private static final GenericParquetWriter INSTANCE = new GenericParquetWriter();

  private GenericParquetWriter() {}

  public static ParquetValueWriter<Record> buildWriter(MessageType type) {
    return INSTANCE.createWriter(type);
  }

  public static ParquetValueWriter<Record> buildWriter(
      Schema tableSchema, MessageType messageType) {
    return INSTANCE.createWriter(tableSchema, messageType);
  }

  @Override
  protected StructWriter<Record> createStructWriter(List<ParquetValueWriter<?>> writers) {
    return new RecordWriter(writers);
  }

  private static class RecordWriter extends StructWriter<Record> {
    private RecordWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Record struct, int index) {
      return struct.get(index);
    }
  }
}
