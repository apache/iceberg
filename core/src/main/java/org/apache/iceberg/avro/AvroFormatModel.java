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
import java.util.function.Supplier;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FormatModel;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class AvroFormatModel<D> implements FormatModel<D> {
  private final String objectModelName;
  private final BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction;
  private final BiFunction<Schema, org.apache.avro.Schema, DatumWriter<D>> writerFunction;
  private final Supplier<Function<PositionDelete<D>, D>> positionDeleteConverter;

  public AvroFormatModel(
      String objectModelName,
      BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction,
      BiFunction<Schema, org.apache.avro.Schema, DatumWriter<D>> writerFunction,
      Supplier<Function<PositionDelete<D>, D>> positionDeleteConverter) {
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
  public org.apache.iceberg.io.WriteBuilder<D> writeBuilder(OutputFile outputFile) {
    return new Avro.WriteBuilderImpl<D>(outputFile).writerFunction(writerFunction);
  }

  @Override
  public Function<PositionDelete<D>, D> positionDeleteConverter(Schema schema) {
    return positionDeleteConverter.get();
  }

  @Override
  public org.apache.iceberg.io.ReadBuilder<D> readBuilder(InputFile inputFile) {
    return new Avro.ReadBuilderImpl(inputFile).readerFunction(readerFunction);
  }
}
