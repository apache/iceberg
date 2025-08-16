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
import java.util.function.BiFunction;
import java.util.function.Function;
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
  private final BiFunction<Schema, TypeDescription, OrcRowWriter<D>> writerFunction;
  private final Function<PositionDelete<D>, D> positionDeleteConverter;

  private ORCFormatModel(
      String objectModelName,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D> batchReaderFunction,
      BiFunction<Schema, TypeDescription, OrcRowWriter<D>> writerFunction,
      Function<PositionDelete<D>, D> positionDeleteConverter) {
    this.objectModelName = objectModelName;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
    this.positionDeleteConverter = positionDeleteConverter;
  }

  public ORCFormatModel(
      String objectModelName,
      ReaderFunction<D> readerFunction,
      BiFunction<Schema, TypeDescription, OrcRowWriter<D>> writerFunction,
      Function<PositionDelete<D>, D> positionDeleteConverter) {
    this(objectModelName, readerFunction, null, writerFunction, positionDeleteConverter);
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
  public org.apache.iceberg.io.WriteBuilder<D> writeBuilder(OutputFile outputFile) {
    return new ORC.WriteBuilderImpl<D>(outputFile).writerFunction(writerFunction);
  }

  @Override
  public Function<PositionDelete<D>, D> positionDeleteConverter(Schema schema) {
    return positionDeleteConverter;
  }

  @Override
  public org.apache.iceberg.io.ReadBuilder<D> readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return new ORC.ReadBuilderImpl<D>(inputFile).batchReaderFunction(batchReaderFunction);
    } else {
      return new ORC.ReadBuilderImpl<D>(inputFile).readerFunction(readerFunction);
    }
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
