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
import org.apache.iceberg.avro.AvroObjectModelFactory;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.orc.ORCObjectModelFactory;
import org.apache.iceberg.parquet.ParquetObjectModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericObjectModels {
  private static final Logger LOG = LoggerFactory.getLogger(GenericObjectModels.class);

  public static final String GENERIC_OBJECT_MODEL = "generic";

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
            ObjectModelRegistry.registerObjectModelFactory(
                new ParquetObjectModelFactory<>(
                    GENERIC_OBJECT_MODEL,
                    GenericParquetReaders::buildReader,
                    (nativeSchema, icebergSchema, messageType) ->
                        GenericParquetWriter.create(icebergSchema, messageType),
                    Function.identity())));
  }

  private static void registerAvro() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            ObjectModelRegistry.registerObjectModelFactory(
                new AvroObjectModelFactory<>(
                    GENERIC_OBJECT_MODEL,
                    PlannedDataReader::create,
                    (avroSchema, unused) -> DataWriter.create(avroSchema))));
  }

  private static void registerOrc() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            ObjectModelRegistry.registerObjectModelFactory(
                new ORCObjectModelFactory<>(
                    GENERIC_OBJECT_MODEL,
                    GenericOrcReader::buildReader,
                    (schema, messageType, nativeSchema) ->
                        GenericOrcWriter.buildWriter(schema, messageType),
                    Function.identity())));
  }

  private GenericObjectModels() {}

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
