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
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.orc.TypeDescription;

public class ORCFormatModel<D, S> implements FormatModel<D, S> {
  private final Class<D> type;
  private final Class<S> schemaType;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D> batchReaderFunction;
  private final WriterFunction<S> writerFunction;

  private ORCFormatModel(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D> batchReaderFunction,
      WriterFunction<S> writerFunction) {
    this.type = type;
    this.schemaType = schemaType;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
  }

  public ORCFormatModel(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<D> readerFunction,
      WriterFunction<S> writerFunction) {
    this(type, schemaType, readerFunction, null, writerFunction);
  }

  public ORCFormatModel(
      Class<D> type, Class<S> schemaType, BatchReaderFunction<D> batchReaderFunction) {
    this(type, schemaType, null, batchReaderFunction, null);
  }

  public ORCFormatModel(Class<D> type) {
    this(type, null, null, null, null);
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
  public Class<S> schemaType() {
    return schemaType;
  }

  @Override
  public WriteBuilder writeBuilder(OutputFile outputFile, Schema icebergSchema, S engineSchema) {
    return ORC.write(outputFile)
        .createWriterFunc(
            (schema, typeDescription) ->
                writerFunction.write(icebergSchema, typeDescription, engineSchema));
  }

  @Override
  public ReadBuilder readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return ORC.read(inputFile).batchReaderFunction(batchReaderFunction);
    } else {
      return ORC.read(inputFile).readerFunction(readerFunction);
    }
  }

  @FunctionalInterface
  public interface ReaderFunction<D> {
    OrcRowReader<D> read(
        Schema schema, TypeDescription typeDescription, Map<Integer, ?> constantValues);
  }

  @FunctionalInterface
  public interface BatchReaderFunction<D> {
    OrcBatchReader<D> read(
        Schema schema, TypeDescription typeDescription, Map<Integer, ?> constantValues);
  }

  @FunctionalInterface
  public interface WriterFunction<E> {
    OrcRowWriter<?> write(Schema schema, TypeDescription typeDescription, E nativeSchema);
  }
}
