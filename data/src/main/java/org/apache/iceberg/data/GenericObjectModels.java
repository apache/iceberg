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
package org.apache.iceberg.data;

import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

public class GenericObjectModels {
  public static final String GENERIC_OBJECT_MODEL = "generic";

  public static void register() {
    ObjectModelRegistry.registerReader(
        FileFormat.PARQUET,
        GENERIC_OBJECT_MODEL,
        inputFile -> Parquet.read(inputFile).readerFunction(GenericParquetReaders::buildReader));

    ObjectModelRegistry.registerAppender(
        FileFormat.PARQUET,
        GENERIC_OBJECT_MODEL,
        outputFile ->
            Parquet.appender(outputFile)
                .writerFunction(
                    (nativeSchema, icebergSchema, messageType) ->
                        GenericParquetWriter.create(icebergSchema, messageType))
                .pathTransformFunc(Function.identity()));

    ObjectModelRegistry.registerReader(
        FileFormat.AVRO,
        GENERIC_OBJECT_MODEL,
        inputFile -> Avro.read(inputFile).readerFunction(PlannedDataReader::create));

    ObjectModelRegistry.registerAppender(
        FileFormat.AVRO,
        GENERIC_OBJECT_MODEL,
        outputFile ->
            Avro.appender(outputFile)
                .writerFunction((avroSchema, unused) -> DataWriter.create(avroSchema)));

    ObjectModelRegistry.registerReader(
        FileFormat.ORC,
        GENERIC_OBJECT_MODEL,
        inputFile -> ORC.read(inputFile).readerFunction(GenericOrcReader::buildReader));

    ObjectModelRegistry.registerAppender(
        FileFormat.ORC,
        GENERIC_OBJECT_MODEL,
        outputFile ->
            ORC.appender(outputFile)
                .writerFunction(
                    (schema, messageType, nativeSchema) ->
                        GenericOrcWriter.buildWriter(schema, messageType))
                .pathTransformFunc(Function.identity()));
  }

  private GenericObjectModels() {}
}
