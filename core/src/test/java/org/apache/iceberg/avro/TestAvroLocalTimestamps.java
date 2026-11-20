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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.iceberg.Files;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestAvroLocalTimestamps {
  private static final long NANOS = 1_697_380_245_123_456_789L;
  private static final long MICROS = 1_697_380_245_123_456L;
  private static final long MILLIS = 1_697_380_245_123L;

  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "timestamp_nanos", Types.TimestampNanoType.withoutZone()),
          Types.NestedField.required(2, "timestamp_micros", Types.TimestampType.withoutZone()),
          Types.NestedField.required(3, "timestamp_millis", Types.TimestampType.withoutZone()));

  @TempDir private Path temp;

  @Test
  void genericAvroReader() throws IOException {
    File file = writeLocalTimestamps(temp);
    GenericData.Record row = readLocalTimestamps(file, GenericAvroReader::create);

    assertThat(row.get("timestamp_nanos")).isEqualTo(NANOS);
    assertThat(row.get("timestamp_micros")).isEqualTo(MICROS);
    assertThat(row.get("timestamp_millis")).isEqualTo(MILLIS * 1000);
  }

  @Test
  void internalReader() throws IOException {
    File file = writeLocalTimestamps(temp);
    StructLike row = readLocalTimestamps(file, InternalReader::create);

    assertThat(row.get(0, Long.class)).isEqualTo(NANOS);
    assertThat(row.get(1, Long.class)).isEqualTo(MICROS);
    assertThat(row.get(2, Long.class)).isEqualTo(MILLIS * 1000);
  }

  private static File writeLocalTimestamps(Path dir) throws IOException {
    Schema avroSchema =
        SchemaBuilder.record("local_timestamps")
            .fields()
            .name("timestamp_nanos")
            .type(LogicalTypes.localTimestampNanos().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("timestamp_micros")
            .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("timestamp_millis")
            .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .endRecord();

    avroSchema.getField("timestamp_nanos").addProp("field-id", 1);
    avroSchema.getField("timestamp_micros").addProp("field-id", 2);
    avroSchema.getField("timestamp_millis").addProp("field-id", 3);

    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("timestamp_nanos", NANOS);
    record.put("timestamp_micros", MICROS);
    record.put("timestamp_millis", MILLIS);

    File file = dir.resolve("local_timestamps.avro").toFile();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, file);
      writer.append(record);
    }

    return file;
  }

  private static <T> T readLocalTimestamps(
      File file, Function<org.apache.iceberg.Schema, DatumReader<?>> readerFunction)
      throws IOException {
    try (AvroIterable<T> reader =
        Avro.read(Files.localInput(file))
            .project(SCHEMA)
            .createResolvingReader(readerFunction)
            .build()) {
      return Iterables.getOnlyElement(reader);
    }
  }
}
