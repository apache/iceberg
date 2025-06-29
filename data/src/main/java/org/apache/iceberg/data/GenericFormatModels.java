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
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericFormatModels {
  private static final Logger LOG = LoggerFactory.getLogger(GenericFormatModels.class);

  public static final String MODEL_NAME = "generic";

  public static void register() {
    // ORC, Parquet are optional dependencies. If they are not present, we should just log and
    // ignore NoClassDefFoundErrors
    registerAvro();
    registerParquet();
    registerOrc();
  }

  private static void registerParquet() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.registerFormatModel(
                new ParquetFormatModel<>(
                    MODEL_NAME,
                    GenericParquetReaders::buildReader,
                    (nativeSchema, icebergSchema, messageType) ->
                        GenericParquetWriter.create(icebergSchema, messageType),
                    Function.identity())));
  }

  private static void registerAvro() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.registerFormatModel(
                new AvroFormatModel<>(
                    MODEL_NAME,
                    PlannedDataReader::create,
                    (avroSchema, unused) -> DataWriter.create(avroSchema))));
  }

  private static void registerOrc() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.registerFormatModel(
                new ORCFormatModel<>(
                    MODEL_NAME,
                    GenericOrcReader::buildReader,
                    (schema, messageType, nativeSchema) ->
                        GenericOrcWriter.buildWriter(schema, messageType),
                    Function.identity())));
  }

  private GenericFormatModels() {}

  @SuppressWarnings("CatchBlockLogException")
  private static void logAngIgnoreNoClassDefFoundError(Runnable runnable) {
    try {
      runnable.run();
    } catch (NoClassDefFoundError e) {
      // Log the exception and ignore it
      LOG.info("Exception occurred when trying to register object models: {}", e.getMessage());
    }
  }
}
