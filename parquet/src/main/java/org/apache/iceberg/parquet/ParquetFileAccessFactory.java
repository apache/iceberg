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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetFileAccessFactory<E, D, V, F>
    implements org.apache.iceberg.io.FileAccessFactory<E, D, V> {
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<V, F> batchReaderFunction;
  private final WriterFunction<D, E> writerFunction;
  private final Function<CharSequence, ?> pathTransformFunc;

  public ParquetFileAccessFactory(
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<V, F> batchReaderFunction,
      WriterFunction<D, E> writerFunction,
      Function<CharSequence, ?> pathTransformFunc) {
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
    this.pathTransformFunc = pathTransformFunc;
  }

  public ParquetFileAccessFactory(
      ReaderFunction<D> readerFunction,
      WriterFunction<D, E> writerFunction,
      Function<CharSequence, ?> pathTransformFunc) {
    this(readerFunction, null, writerFunction, pathTransformFunc);
  }

  public ParquetFileAccessFactory(BatchReaderFunction<V, F> batchReaderFunction) {
    this(null, batchReaderFunction, null, null);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, D>> B dataWriteBuilder(
      OutputFile outputFile) {
    return (B)
        new Parquet.WriteBuilderImpl<E, D>(outputFile, FileContent.DATA)
            .writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, D>> B equalityDeleteWriteBuilder(
      OutputFile outputFile) {
    return (B)
        new Parquet.WriteBuilderImpl<E, D>(outputFile, FileContent.EQUALITY_DELETES)
            .writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, PositionDelete<D>>>
      B positionDeleteWriteBuilder(OutputFile outputFile) {
    return (B)
        new Parquet.WriteBuilderImpl<E, D>(outputFile, FileContent.POSITION_DELETES)
            .writerFunction(writerFunction)
            .pathTransformFunc(pathTransformFunc);
  }

  @Override
  public <B extends org.apache.iceberg.io.ReadBuilder<B, D>> B readBuilder(InputFile inputFile) {
    return (B) new Parquet.ReadBuilderImpl<D, F>(inputFile).readerFunction(readerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.ReadBuilder<B, V>> B vectorizedReadBuilder(
      InputFile inputFile) {
    return (B)
        new Parquet.ReadBuilderImpl<V, F>(inputFile).batchReaderFunction(batchReaderFunction);
  }

  public interface ReaderFunction<D> {
    ParquetValueReader<D> read(
        Schema schema, MessageType messageType, Map<Integer, ?> constantFieldAccessors);
  }

  public interface SupportsDeleteFilter<F> {
    void deleteFilter(F deleteFilter);
  }

  public interface BatchReaderFunction<D, F> {
    VectorizedReader<D> read(
        Schema schema,
        MessageType messageType,
        Map<Integer, ?> constantFieldAccessors,
        F deleteFilter,
        Map<String, String> config);
  }

  public interface WriterFunction<D, E> {
    ParquetValueWriter<D> write(E engineSchema, Schema icebergSchema, MessageType messageType);
  }
}
