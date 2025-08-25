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
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericFormatModels {
  private static final Logger LOG = LoggerFactory.getLogger(GenericFormatModels.class);
  private static final DeleteTransformer DELETE_TRANSFORMER = new DeleteTransformer();

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
            FormatModelRegistry.register(
                new ParquetFormatModel<>(
                    MODEL_NAME,
                    GenericParquetReaders::buildReader,
                    (schema, messageType, inputType) ->
                        GenericParquetWriter.create(schema, messageType),
                    DELETE_TRANSFORMER)));
  }

  private static void registerAvro() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.register(
                new AvroFormatModel<>(
                    MODEL_NAME,
                    PlannedDataReader::create,
                    (schema, inputSchema) -> DataWriter.create(schema),
                    DELETE_TRANSFORMER)));
  }

  private static void registerOrc() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.register(
                new ORCFormatModel<>(
                    MODEL_NAME,
                    GenericOrcReader::buildReader,
                    (schema, typeDescription, unused) ->
                        GenericOrcWriter.buildWriter(schema, typeDescription),
                    DELETE_TRANSFORMER)));
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

  private static class DeleteTransformer
      implements Function<Schema, Function<PositionDelete<Record>, Record>> {
    @Override
    public Function<PositionDelete<Record>, Record> apply(Schema schema) {
      GenericRecord deleteRecord = GenericRecord.create(DeleteSchemaUtil.posDeleteSchema(schema));
      if (schema == null) {
        return delete -> {
          deleteRecord.set(0, delete.path());
          deleteRecord.set(1, delete.pos());
          return deleteRecord;
        };
      } else {
        return delete -> {
          deleteRecord.set(0, delete.path());
          deleteRecord.set(1, delete.pos());
          deleteRecord.set(2, delete.row());
          return deleteRecord;
        };
      }
    }
  }
}
