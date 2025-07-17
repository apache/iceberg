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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FormatModel;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.orc.TypeDescription;

public class ORCFormatModel<D> implements FormatModel<D> {
  private final String objectModelName;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D> batchReaderFunction;
  private final WriterFunction writerFunction;
  private final Function<CharSequence, ?> pathTransformFunc;

  private ORCFormatModel(
      String objectModelName,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D> batchReaderFunction,
      WriterFunction writerFunction,
      Function<CharSequence, ?> pathTransformFunc) {
    this.objectModelName = objectModelName;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
    this.pathTransformFunc = pathTransformFunc;
  }

  public ORCFormatModel(
      String objectModelName,
      ReaderFunction<D> readerFunction,
      WriterFunction writerFunction,
      Function<CharSequence, ?> pathTransformFunc) {
    this(objectModelName, readerFunction, null, writerFunction, pathTransformFunc);
  }

  public ORCFormatModel(String objectModelName, BatchReaderFunction<D> batchReaderFunction) {
    this(objectModelName, null, batchReaderFunction, null, null);
  }

  @Override
  public FileFormat format() {
    return FileFormat.ORC;
  }

  @Override
  public String modelName() {
    return objectModelName;
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, D>> B dataBuilder(OutputFile outputFile) {
    return (B)
        new ORC.WriteBuilderImpl<D>(outputFile, FileContent.DATA).writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, D>> B equalityDeleteBuilder(
      OutputFile outputFile) {
    return (B)
        new ORC.WriteBuilderImpl<D>(outputFile, FileContent.EQUALITY_DELETES)
            .writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, PositionDelete<D>>>
      B positionDeleteBuilder(OutputFile outputFile) {
    return (B)
        new ORC.WriteBuilderImpl<D>(outputFile, FileContent.POSITION_DELETES)
            .writerFunction(writerFunction)
            .pathTransformFunc(pathTransformFunc);
  }

  @Override
  public <B extends org.apache.iceberg.io.ReadBuilder<B, D>> B readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return (B) new ORC.ReadBuilderImpl<D>(inputFile).batchReaderFunction(batchReaderFunction);
    } else {
      return (B) new ORC.ReadBuilderImpl<D>(inputFile).readerFunction(readerFunction);
    }
  }

  public interface WriterFunction {
    OrcRowWriter<?> write(Schema schema, TypeDescription messageType);
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
