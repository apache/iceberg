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
package org.apache.iceberg.parquet;

import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetFormatModel<D, S, F> implements FormatModel<D, S> {
  private final Class<D> type;
  private final Class<S> schemaType;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D, F> batchReaderFunction;
  private final WriterFunction<S> writerFunction;

  private ParquetFormatModel(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D, F> batchReaderFunction,
      WriterFunction<S> writerFunction) {
    this.type = type;
    this.schemaType = schemaType;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
  }

  public ParquetFormatModel(Class<D> type) {
    this(type, null, null, null);
  }

  public ParquetFormatModel(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<D> readerFunction,
      WriterFunction<S> writerFunction) {
    this(type, schemaType, readerFunction, null, writerFunction);
  }

  public ParquetFormatModel(
      Class<D> type, Class<S> schemaType, BatchReaderFunction<D, F> batchReaderFunction) {
    this(type, schemaType, null, batchReaderFunction, null);
  }

  @Override
  public FileFormat format() {
    return FileFormat.PARQUET;
  }

  @Override
  public Class<D> type() {
    return type;
  }

  @Override
  public Class<S> schemaType() {
    return schemaType;
  }

  @Override
  public WriteBuilder writeBuilder(OutputFile outputFile) {
    return Parquet.write(outputFile)
        .inputSchemaClass(schemaType)
        .writerFunction((WriterFunction<Object>) writerFunction);
  }

  @Override
  public ReadBuilder readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return Parquet.read(inputFile).createBatchedReaderFunc(impl(batchReaderFunction));
    } else {
      return Parquet.read(inputFile).createReaderFunc(impl(readerFunction));
    }
  }

  @FunctionalInterface
  public interface ReaderFunction<D> {
    ParquetValueReader<D> read(
        Schema schema, MessageType messageType, Map<Integer, ?> constantValues);
  }

  @FunctionalInterface
  public interface BatchReaderFunction<D, F> {
    VectorizedReader<D> read(
        Schema schema,
        MessageType messageType,
        Map<Integer, ?> constantValues,
        F deleteFilter,
        Map<String, String> config);
  }

  @FunctionalInterface
  public interface WriterFunction<S> {
    ParquetValueWriter<?> write(Schema icebergSchema, MessageType messageType, S engineSchema);
  }

  public interface SupportsDeleteFilter<F> {
    void deleteFilter(F deleteFilter);
  }

  public static Parquet.ReadBuilder.ReaderFunction impl(ReaderFunction<?> readerFunction) {
    return new Parquet.ReadBuilder.ReaderFunction() {
      private Schema schema;
      private Map<Integer, ?> constantValues;

      @Override
      public Function<MessageType, ParquetValueReader<?>> apply() {
        return messageType -> readerFunction.read(schema, messageType, constantValues);
      }

      @Override
      public Parquet.ReadBuilder.ReaderFunction withSchema(Schema schema) {
        this.schema = schema;
        return this;
      }

      @Override
      public Parquet.ReadBuilder.ReaderFunction withConstantValues(Map<Integer, ?> constantValues) {
        this.constantValues = constantValues;
        return this;
      }
    };
  }

  public static <A, B> Parquet.ReadBuilder.BatchReaderFunction impl(
      BatchReaderFunction<A, B> readerFunction) {
    return new Parquet.ReadBuilder.BatchReaderFunction() {
      private Schema schema;
      private Map<Integer, ?> constantValues;
      private B deleteFilter;
      private Map<String, String> config;

      @Override
      public Function<MessageType, VectorizedReader<?>> apply() {
        return messageType ->
            readerFunction.read(schema, messageType, constantValues, deleteFilter, config);
      }

      @Override
      public Parquet.ReadBuilder.BatchReaderFunction withSchema(Schema schema) {
        this.schema = schema;
        return this;
      }

      @Override
      public Parquet.ReadBuilder.BatchReaderFunction withConstantValues(
          Map<Integer, ?> constantValues) {
        this.constantValues = constantValues;
        return this;
      }

      @Override
      public Parquet.ReadBuilder.BatchReaderFunction withDeleteFilter(Object deleteFilter) {
        this.deleteFilter = (B) deleteFilter;
        return this;
      }

      @Override
      public Parquet.ReadBuilder.BatchReaderFunction withConfig(Map<String, String> config) {
        this.config = config;
        return this;
      }
    };
  }
}
