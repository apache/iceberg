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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

public class InternalReader<T extends StructLike> extends BaseParquetReaders<T> {


  private Class<? extends StructLike> rootType = Record.class;
  private Map<Integer, Class<? extends StructLike>> typesById = Map.of();

  public InternalReader() {}

  @Override
  protected ParquetValueReader<T> createStructReader(List<ParquetValueReader<?>> fieldReaders, StructType structType) {
    throw new UnsupportedOperationException(
        "createStructReader(List<ParquetValueReader<?>>, StructType) is not supported because " +
          "InternalReader needs the fieldId to determine the type of struct to return");
  }

  @SuppressWarnings("unchecked")
  public static <T extends StructLike> ParquetValueReader<T> create(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return (ParquetValueReader<T>) new InternalReader<>().createReader(expectedSchema, fileSchema, idToConstant);
  }

  @SuppressWarnings("unchecked")
  public <T extends StructLike> ParquetValueReader<T> create(Schema expectedSchema, MessageType fileSchema) {
    return (ParquetValueReader<T>) createReader(expectedSchema, fileSchema);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected ParquetValueReader<T> createStructReader(
    List<ParquetValueReader<?>> fieldReaders, StructType structType, Integer fieldId) {
    return (ParquetValueReader<T>) ParquetValueReaders.structLikeReader(fieldReaders, structType, typesById.getOrDefault(fieldId, rootType));
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
  protected ParquetValueReader<?> timeReader(ColumnDescriptor desc) {
    return ParquetValueReaders.times(desc);
  }

  @Override
  protected ParquetValueReader<?> timestampReader(ColumnDescriptor desc, boolean isAdjustedToUTC) {
    return ParquetValueReaders.timestamps(desc);
  }

  public void setCustomTypeMap(Map<Integer, Class<? extends StructLike>> typesById) {
    this.typesById = typesById;
  }
}
