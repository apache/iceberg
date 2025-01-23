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
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class InternalReader extends BaseParquetReaders<Record> {

  private static final InternalReader INSTANCE = new InternalReader();

  private InternalReader() {}

  public static ParquetValueReader<Record> create(Schema expectedSchema, MessageType fileSchema) {
    return INSTANCE.createReader(expectedSchema, fileSchema);
  }

  public static ParquetValueReader<Record> create(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return INSTANCE.createReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  protected ParquetValueReader<Record> createStructReader(
      List<Type> types, List<ParquetValueReader<?>> fieldReaders, StructType structType) {
    return ParquetValueReaders.recordReader(types, fieldReaders, structType);
  }

  @Override
  protected ParquetValueReader<?> fixedReader(ColumnDescriptor desc) {
    return new ParquetValueReaders.BytesReader(desc);
  }

  @Override
  protected ParquetValueReader<?> dateReader(ColumnDescriptor desc) {
    return new ParquetValueReaders.UnboxedReader<>(desc);
  }

  @Override
  protected ParquetValueReader<?> timeReader(
      ColumnDescriptor desc, LogicalTypeAnnotation.TimeUnit unit) {
    if (unit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
      return ParquetValueReaders.millisAsTimestamps(desc);
    }

    return new ParquetValueReaders.UnboxedReader<>(desc);
  }

  @Override
  protected ParquetValueReader<?> timestampReader(
      ColumnDescriptor desc, LogicalTypeAnnotation.TimeUnit unit, boolean isAdjustedToUTC) {
    if (desc.getPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
      return ParquetValueReaders.int96Timestamps(desc);
    }

    if (unit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
      return ParquetValueReaders.millisAsTimestamps(desc);
    }

    return new ParquetValueReaders.UnboxedReader<>(desc);
  }
}
