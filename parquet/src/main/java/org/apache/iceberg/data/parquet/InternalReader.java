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
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

public class InternalReader<T extends StructLike> extends BaseParquetReaders<T> {

  private final Map<Integer, Class<? extends StructLike>> typesById = Maps.newHashMap();

  private static final InternalReader<?> INSTANCE = new InternalReader<>();

  private InternalReader() {}

  @SuppressWarnings("unchecked")
  public static <T extends StructLike> ParquetValueReader<T> create(
      Schema expectedSchema, MessageType fileSchema) {
    return (ParquetValueReader<T>) INSTANCE.createReader(expectedSchema, fileSchema);
  }

  @SuppressWarnings("unchecked")
  public static <T extends StructLike> ParquetValueReader<T> create(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return (ParquetValueReader<T>) INSTANCE.createReader(expectedSchema, fileSchema, idToConstant);
  }

  public static Parquet.ReadBuilder.ReaderFunction readerFunction() {
    InternalReader<?> reader = new InternalReader<>();

    return new Parquet.ReadBuilder.ReaderFunction() {
      private Schema schema;

      @Override
      public Function<MessageType, ParquetValueReader<?>> apply() {
        return messageType -> reader.createReader(schema, messageType);
      }

      @Override
      public Parquet.ReadBuilder.ReaderFunction withSchema(Schema schema) {
        this.schema = schema;
        return this;
      }

      @Override
      public Parquet.ReadBuilder.ReaderFunction withCustomTypes(
          Map<Integer, Class<? extends StructLike>> customTypes) {
        reader.typesById.putAll(customTypes);
        return this;
      }

      @Override
      public Parquet.ReadBuilder.ReaderFunction withRootType(Class<? extends StructLike> rootType) {
        if (rootType != null) {
          reader.typesById.put(ROOT_ID, rootType);
        }

        return this;
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  protected ParquetValueReader<T> createStructReader(
      List<ParquetValueReader<?>> fieldReaders, StructType structType, Integer fieldId) {
    return (ParquetValueReader<T>)
        ParquetValueReaders.structLikeReader(
            fieldReaders, structType, typesById.getOrDefault(fieldId, Record.class));
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
}
