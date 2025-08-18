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

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;
import org.apache.iceberg.types.Types;
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
            FormatModelRegistry.register(
                new ParquetFormatModel<>(
                    MODEL_NAME,
                    GenericParquetReaders::buildReader,
                    GenericParquetWriter::create,
                    deleteTransformer())));
  }

  private static void registerAvro() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.register(
                new AvroFormatModel<>(
                    MODEL_NAME,
                    PlannedDataReader::create,
                    (schema, avroSchema) -> DataWriter.create(avroSchema),
                    deleteTransformer())));
  }

  private static void registerOrc() {
    logAngIgnoreNoClassDefFoundError(
        () ->
            FormatModelRegistry.register(
                new ORCFormatModel<>(
                    MODEL_NAME,
                    GenericOrcReader::buildReader,
                    GenericOrcWriter::buildWriter,
                    deleteTransformer())));
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

  private static Supplier<Function<PositionDelete<Record>, Record>> deleteTransformer() {
    DeleteRecord deleteRecord = new DeleteRecord();
    return () -> deleteRecord::transform;
  }

  private static class DeleteRecord implements Record {
    private PositionDelete<Record> delete;

    private DeleteRecord transform(PositionDelete<Record> newDelete) {
      this.delete = newDelete;
      return this;
    }

    @Override
    public Types.StructType struct() {
      throw new UnsupportedOperationException("Not supported for DeleteRecord");
    }

    @Override
    public Object getField(String name) {
      throw new UnsupportedOperationException("Not supported for DeleteRecord");
    }

    @Override
    public void setField(String name, Object value) {
      throw new UnsupportedOperationException("DeleteRecord is immutable");
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return delete.path();
        case 1:
          return delete.pos();
        case 2:
          return delete.row();
      }

      throw new IllegalArgumentException("Cannot get value for invalid index: " + pos);
    }

    @Override
    public Record copy() {
      throw new UnsupportedOperationException("Not supported for DeleteRecord");
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
      throw new UnsupportedOperationException("Not supported for DeleteRecord");
    }

    @Override
    public int size() {
      return 3;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("DeleteRecord is immutable");
    }
  }
}
