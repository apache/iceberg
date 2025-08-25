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

import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.avro.AvroFileAccessFactory;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAccessFactory;
import org.apache.iceberg.io.FileAccessFactory.Combiner;
import org.apache.iceberg.orc.ORCFileAccessFactory;
import org.apache.iceberg.parquet.ParquetFileAccessFactory;
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
            FileAccessFactoryRegistry.registerFileAccessFactory(
                new ParquetFileAccessFactory<>(
                    GENERIC_OBJECT_MODEL,
                    GenericParquetReaders::buildReader,
                    (nativeSchema, icebergSchema, messageType) ->
                        GenericParquetWriter.create(icebergSchema, messageType),
                    Function.identity(),
                    (icebergSchema, families) -> {
                      CombinedRecord record = CombinedRecord.create(icebergSchema, families);
                      return new MyCombiner(record);
                    },
                    (icebergSchema, family) -> {
                      NarrowedRecord record = NarrowedRecord.create(icebergSchema, family);
                      return new MyNarrower(record);
                    })));
  }

  private static class MyNarrower implements FileAccessFactory.Narrower<Record> {
    private final NarrowedRecord record;

    MyNarrower(NarrowedRecord record) {
      this.record = record;
    }

    @Override
    public Record narrow(Record wrappedRecord) {
      record.set(wrappedRecord);

      return record;
    }
  }

  private static class MyCombiner implements Combiner<Record> {
    private final CombinedRecord template;

    MyCombiner(CombinedRecord template) {
      this.template = template;
    }

    @Override
    public Record combine(List<Record> records) {
      CombinedRecord clonedRecord = CombinedRecord.clone(template);
      for (int i = 0; i < records.size(); i++) {
        clonedRecord.setFamily(i, records.get(i));
      }

      return clonedRecord;
    }
  }

  private static void registerAvro() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FileAccessFactoryRegistry.registerFileAccessFactory(
                new AvroFileAccessFactory<>(
                    GENERIC_OBJECT_MODEL,
                    PlannedDataReader::create,
                    (avroSchema, unused) -> DataWriter.create(avroSchema))));
  }

  private static void registerOrc() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FileAccessFactoryRegistry.registerFileAccessFactory(
                new ORCFileAccessFactory<>(
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
