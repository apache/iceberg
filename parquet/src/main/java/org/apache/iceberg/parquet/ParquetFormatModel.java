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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FormatModel;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetFormatModel<D, F> implements FormatModel<D> {
  private final String objectModelName;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D, F> batchReaderFunction;
  private final BiFunction<Schema, MessageType, ParquetValueWriter<D>> writerFunction;
  private final Supplier<Function<PositionDelete<D>, D>> positionDeleteConverter;

  private ParquetFormatModel(
      String objectModelName,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D, F> batchReaderFunction,
      BiFunction<Schema, MessageType, ParquetValueWriter<D>> writerFunction,
      Supplier<Function<PositionDelete<D>, D>> positionDeleteConverter) {
    this.objectModelName = objectModelName;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
    this.positionDeleteConverter = positionDeleteConverter;
  }

  public ParquetFormatModel(
      String objectModelName,
      ReaderFunction<D> readerFunction,
      BiFunction<Schema, MessageType, ParquetValueWriter<D>> writerFunction,
      Supplier<Function<PositionDelete<D>, D>> positionDeleteConverter) {
    this(objectModelName, readerFunction, null, writerFunction, positionDeleteConverter);
  }

  public ParquetFormatModel(String objectModelName, BatchReaderFunction<D, F> batchReaderFunction) {
    this(objectModelName, null, batchReaderFunction, null, null);
  }

  @Override
  public FileFormat format() {
    return FileFormat.PARQUET;
  }

  @Override
  public String modelName() {
    return objectModelName;
  }

  @Override
  public org.apache.iceberg.io.WriteBuilder<D> writeBuilder(OutputFile outputFile) {
    return new Parquet.WriteBuilderImpl<D>(outputFile).writerFunction(writerFunction);
  }

  @Override
  public Function<PositionDelete<D>, D> positionDeleteConverter(Schema schema) {
    return positionDeleteConverter.get();
  }

  @Override
  public org.apache.iceberg.io.ReadBuilder<D> readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return new Parquet.ReadBuilderImpl<D, F>(inputFile).batchReaderFunction(batchReaderFunction);
    } else {
      return new Parquet.ReadBuilderImpl<D, F>(inputFile).readerFunction(readerFunction);
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
}
