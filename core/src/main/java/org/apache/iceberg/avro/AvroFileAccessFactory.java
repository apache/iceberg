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
package org.apache.iceberg.avro;

import java.util.Map;
import java.util.function.BiFunction;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class AvroFileAccessFactory<E, D, V>
    implements org.apache.iceberg.io.FileAccessFactory<E, D, V> {
  private final BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction;
  private final BiFunction<org.apache.avro.Schema, E, DatumWriter<D>> writerFunction;
  private final BiFunction<org.apache.avro.Schema, E, DatumWriter<D>> deleteRowWriterFunction;

  public AvroFileAccessFactory(
      BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction,
      BiFunction<org.apache.avro.Schema, E, DatumWriter<D>> writerFunction,
      BiFunction<org.apache.avro.Schema, E, DatumWriter<D>> deleteRowWriterFunction) {
    this.readerFunction = readerFunction;
    this.writerFunction = writerFunction;
    this.deleteRowWriterFunction = deleteRowWriterFunction;
  }

  public AvroFileAccessFactory(
      BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction,
      BiFunction<org.apache.avro.Schema, E, DatumWriter<D>> writerFunction) {
    this(readerFunction, writerFunction, null);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, D>> B dataWriteBuilder(
      OutputFile outputFile) {
    return (B)
        new Avro.WriteBuilderImpl<E, D>(outputFile, FileContent.DATA)
            .writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, D>> B equalityDeleteWriteBuilder(
      OutputFile outputFile) {
    return (B)
        new Avro.WriteBuilderImpl<E, D>(outputFile, FileContent.EQUALITY_DELETES)
            .writerFunction(writerFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.WriteBuilder<B, E, PositionDelete<D>>>
      B positionDeleteWriteBuilder(OutputFile outputFile) {
    return (B)
        new Avro.WriteBuilderImpl<E, D>(outputFile, FileContent.POSITION_DELETES)
            .writerFunction(writerFunction)
            .deleteRowWriterFunction(deleteRowWriterFunction);
  }

  @Override
  public <B extends org.apache.iceberg.io.ReadBuilder<B, D>> B readBuilder(InputFile inputFile) {
    return (B) new Avro.ReadBuilderImpl(inputFile).readerFunction(readerFunction);
  }
}
