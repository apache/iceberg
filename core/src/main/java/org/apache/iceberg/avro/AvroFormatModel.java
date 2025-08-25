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
import java.util.function.Function;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FormatModel;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class AvroFormatModel<D, S> implements FormatModel<D, S> {
  private final String objectModelName;
  private final BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction;
  private final BiFunction<org.apache.avro.Schema, S, DatumWriter<D>> writerFunction;
  private final Function<Schema, Function<PositionDelete<D>, D>> positionDeleteConverter;

  public AvroFormatModel(
      String objectModelName,
      BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction,
      BiFunction<org.apache.avro.Schema, S, DatumWriter<D>> writerFunction,
      Function<Schema, Function<PositionDelete<D>, D>> positionDeleteConverter) {
    this.objectModelName = objectModelName;
    this.readerFunction = readerFunction;
    this.writerFunction = writerFunction;
    this.positionDeleteConverter = positionDeleteConverter;
  }

  @Override
  public FileFormat format() {
    return FileFormat.AVRO;
  }

  @Override
  public String modelName() {
    return objectModelName;
  }

  @Override
  public org.apache.iceberg.io.WriteBuilder<D, S> writeBuilder(OutputFile outputFile) {
    return new Avro.WriteBuilderImpl<D, S>(outputFile).writerFunction(writerFunction);
  }

  @Override
  public Function<PositionDelete<D>, D> positionDeleteConverter(Schema schema) {
    return positionDeleteConverter.apply(schema);
  }

  @Override
  public org.apache.iceberg.io.ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new Avro.ReadBuilderImpl(inputFile).readerFunction(readerFunction);
  }
}
