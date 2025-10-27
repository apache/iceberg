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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class AvroFormatModel<D, S> implements FormatModel<D, S> {
  private final Class<D> type;
  private final Class<S> schemaType;
  private final BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction;
  private final BiFunction<org.apache.avro.Schema, S, DatumWriter<D>> writerFunction;

  public AvroFormatModel(Class<D> type) {
    this(type, null, null, null);
  }

  public AvroFormatModel(
      Class<D> type,
      Class<S> schemaType,
      BiFunction<Schema, Map<Integer, ?>, DatumReader<D>> readerFunction,
      BiFunction<org.apache.avro.Schema, S, DatumWriter<D>> writerFunction) {
    this.type = type;
    this.schemaType = schemaType;
    this.readerFunction = readerFunction;
    this.writerFunction = writerFunction;
  }

  @Override
  public FileFormat format() {
    return FileFormat.AVRO;
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
    return Avro.write(outputFile)
        .createWriterFunc(schema -> writerFunction.apply(schema, engineSchema));
  }

  @Override
  public ReadBuilder readBuilder(InputFile inputFile) {
    return Avro.read(inputFile).readerFunction(readerFunction::apply);
  }
}
