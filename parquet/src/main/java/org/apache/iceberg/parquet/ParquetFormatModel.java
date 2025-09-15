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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FormatModel;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.parquet.schema.MessageType;

public class ParquetFormatModel<D, S, F> implements FormatModel<D, S> {
  private final Class<D> type;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D, F> batchReaderFunction;
  private final WriterFunction<D, S> writerFunction;

  private ParquetFormatModel(
      Class<D> type,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D, F> batchReaderFunction,
      WriterFunction<D, S> writerFunction) {
    this.type = type;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
  }

  public ParquetFormatModel(
      Class<D> type, ReaderFunction<D> readerFunction, WriterFunction<D, S> writerFunction) {
    this(type, readerFunction, null, writerFunction);
  }

  public ParquetFormatModel(Class<D> type, BatchReaderFunction<D, F> batchReaderFunction) {
    this(type, null, batchReaderFunction, null);
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
  public org.apache.iceberg.io.WriteBuilder<D, S> writeBuilder(OutputFile outputFile) {
    return new Parquet.WriteBuilderImpl<D, S>(outputFile).writerFunction(writerFunction);
  }

  @Override
  public WriteBuilder<PositionDelete<D>, S> positionDeleteWriteBuilder(OutputFile outputFile) {
    return new Parquet.WriteBuilderImpl<PositionDelete<D>, S>(outputFile).deleteWriter();
  }

  @Override
  public org.apache.iceberg.io.ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return new Parquet.ReadBuilderImpl<D, S, F>(inputFile)
          .batchReaderFunction(batchReaderFunction);
    } else {
      return new Parquet.ReadBuilderImpl<D, S, F>(inputFile).readerFunction(readerFunction);
    }
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

  public interface WriterFunction<D, S> {
    ParquetValueWriter<D> write(Schema icebergSchema, MessageType messageType, S engineSchema);
  }
}
