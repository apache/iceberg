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
import java.util.function.Function;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.orc.TypeDescription;

public class ORCFileAccessFactory<E, D, V>
    implements org.apache.iceberg.io.FileAccessFactory<E, D, V> {
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<V> batchReaderFunction;
  private final WriterFunction<E> writerFunction;
  private final Function<CharSequence, ?> pathTransformFunc;

  public ORCFileAccessFactory(
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<V> batchReaderFunction,
      WriterFunction<E> writerFunction,
      Function<CharSequence, ?> pathTransformFunc) {
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
    this.pathTransformFunc = pathTransformFunc;
  }

  public ORCFileAccessFactory(
      ReaderFunction<D> readerFunction,
      WriterFunction<E> writerFunction,
      Function<CharSequence, ?> pathTransformFunc) {
    this(readerFunction, null, writerFunction, pathTransformFunc);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, D>> B dataWriteBuilder(
      OutputFile outputFile) {
    return (B)
        new ORC.WriteBuilderImpl<E, D>(outputFile, FileContent.DATA).writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, D>> B equalityDeleteWriteBuilder(
      OutputFile outputFile) {
    return (B)
        new ORC.WriteBuilderImpl<E, D>(outputFile, FileContent.EQUALITY_DELETES)
            .writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, PositionDelete<D>>>
      B positionDeleteWriteBuilder(OutputFile outputFile) {
    return (B)
        new ORC.WriteBuilderImpl<E, D>(outputFile, FileContent.POSITION_DELETES)
            .writerFunction(writerFunction)
            .pathTransformFunc(pathTransformFunc);
  }

  @Override
  public <B extends org.apache.iceberg.io.ReadBuilder<B, D>> B readBuilder(InputFile inputFile) {
    return (B) new ORC.ReadBuilderImpl<D>(inputFile).readerFunction(readerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.ReadBuilder<B, V>> B vectorizedReadBuilder(
      InputFile inputFile) {
    return (B) new ORC.ReadBuilderImpl<V>(inputFile).batchReaderFunction(batchReaderFunction);
  }

  public interface WriterFunction<E> {
    OrcRowWriter<?> write(Schema schema, TypeDescription messageType, E nativeSchema);
  }

  public interface ReaderFunction<D> {
    OrcRowReader<D> read(
        Schema schema, TypeDescription messageType, Map<Integer, ?> constantFieldAccessors);
  }

  public interface BatchReaderFunction<D> {
    OrcBatchReader<D> read(
        Schema schema, TypeDescription messageType, Map<Integer, ?> constantFieldAccessors);
  }
}
