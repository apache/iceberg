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
package org.apache.iceberg.orc;

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FormatModel;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.orc.TypeDescription;

public class ORCFormatModel<D, S> implements FormatModel<D, S> {
  private final Class<D> type;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D> batchReaderFunction;
  private final WriterFunction<S> writerFunction;

  private ORCFormatModel(
      Class<D> type,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D> batchReaderFunction,
      WriterFunction<S> writerFunction) {
    this.type = type;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
  }

  public ORCFormatModel(
      Class<D> type, ReaderFunction<D> readerFunction, WriterFunction<S> writerFunction) {
    this(type, readerFunction, null, writerFunction);
  }

  public ORCFormatModel(Class<D> type, BatchReaderFunction<D> batchReaderFunction) {
    this(type, null, batchReaderFunction, null);
  }

  public ORCFormatModel(Class<D> type) {
    this(type, null, null, null);
  }

  @Override
  public FileFormat format() {
    return FileFormat.ORC;
  }

  @Override
  public Class<D> type() {
    return type;
  }

  @Override
  public org.apache.iceberg.io.WriteBuilder<D, S> writeBuilder(OutputFile outputFile) {
    return new ORC.WriteBuilderImpl<D, S>(outputFile).writerFunction(writerFunction);
  }

  @Override
  public org.apache.iceberg.io.ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return new ORC.ReadBuilderImpl<D, S>(inputFile).batchReaderFunction(batchReaderFunction);
    } else {
      return new ORC.ReadBuilderImpl<D, S>(inputFile).readerFunction(readerFunction);
    }
  }

  @FunctionalInterface
  public interface ReaderFunction<D> {
    OrcRowReader<D> read(
        Schema schema, TypeDescription messageType, Map<Integer, ?> constantFieldAccessors);
  }

  @FunctionalInterface
  public interface BatchReaderFunction<D> {
    OrcBatchReader<D> read(
        Schema schema, TypeDescription messageType, Map<Integer, ?> constantFieldAccessors);
  }

  @FunctionalInterface
  public interface WriterFunction<E> {
    OrcRowWriter<?> write(Schema schema, TypeDescription messageType, E nativeSchema);
  }
}
