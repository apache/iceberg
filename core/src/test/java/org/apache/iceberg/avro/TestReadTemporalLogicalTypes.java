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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

/**
 * Direct-decode tests for the "raw" Avro readers ({@link GenericAvroReader} and {@link
 * InternalReader}), which keep temporal values as raw long counts (micros/nanos) rather than
 * java.time objects. These exercise the {@code time-millis} and {@code local-timestamp-*} logical
 * types, which Iceberg's own write path never emits but which external producers (e.g. Kafka/Avro
 * Java services) commonly do.
 */
public class TestReadTemporalLogicalTypes {

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "time_millis", Types.TimeType.get()),
          Types.NestedField.required(2, "time_micros", Types.TimeType.get()),
          Types.NestedField.required(3, "local_ts_millis", Types.TimestampType.withoutZone()),
          Types.NestedField.required(4, "local_ts_micros", Types.TimestampType.withoutZone()),
          Types.NestedField.required(5, "local_ts_nanos", Types.TimestampNanoType.withoutZone()));

  private static final LocalTime TIME_MILLIS = LocalTime.of(14, 30, 45, 123_000_000);
  private static final LocalTime TIME_MICROS = LocalTime.of(14, 30, 45, 123_456_000);
  private static final LocalDateTime TS_MILLIS =
      LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123_000_000);
  private static final LocalDateTime TS_MICROS =
      LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123_456_000);
  private static final LocalDateTime TS_NANOS =
      LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123_456_789);

  private static Schema avroSchema() {
    Schema schema =
        SchemaBuilder.record("test_temporal")
            .fields()
            .name("time_millis")
            .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)))
            .noDefault()
            .name("time_micros")
            .type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("local_ts_millis")
            .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("local_ts_micros")
            .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("local_ts_nanos")
            .type(LogicalTypes.localTimestampNanos().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .endRecord();

    schema.getField("time_millis").addProp("field-id", 1);
    schema.getField("time_micros").addProp("field-id", 2);
    schema.getField("local_ts_millis").addProp("field-id", 3);
    schema.getField("local_ts_micros").addProp("field-id", 4);
    schema.getField("local_ts_nanos").addProp("field-id", 5);
    return schema;
  }

  private static GenericRecord avroRecord(Schema avroSchema) {
    GenericRecord record = new GenericData.Record(avroSchema);
    // time-millis is encoded on the wire as an int number of milliseconds from midnight
    record.put("time_millis", (int) (DateTimeUtil.microsFromTime(TIME_MILLIS) / 1000));
    record.put("time_micros", DateTimeUtil.microsFromTime(TIME_MICROS));
    record.put("local_ts_millis", DateTimeUtil.millisFromTimestamp(TS_MILLIS));
    record.put("local_ts_micros", DateTimeUtil.microsFromTimestamp(TS_MICROS));
    record.put("local_ts_nanos", DateTimeUtil.nanosFromTimestamp(TS_NANOS));
    return record;
  }

  @Test
  public void genericAvroReaderKeepsRawValues() throws IOException {
    Schema avroSchema = avroSchema();
    GenericAvroReader<GenericRecord> reader = GenericAvroReader.create(ICEBERG_SCHEMA);
    reader.setSchema(avroSchema);

    GenericRecord result = readRecord(reader, avroSchema, avroRecord(avroSchema));

    // millis-based types are normalized to micros; micros/nanos pass through unchanged
    assertThat(result.get("time_millis")).isEqualTo(DateTimeUtil.microsFromTime(TIME_MILLIS));
    assertThat(result.get("time_micros")).isEqualTo(DateTimeUtil.microsFromTime(TIME_MICROS));
    assertThat(result.get("local_ts_millis"))
        .isEqualTo(DateTimeUtil.microsFromTimestamp(TS_MILLIS));
    assertThat(result.get("local_ts_micros"))
        .isEqualTo(DateTimeUtil.microsFromTimestamp(TS_MICROS));
    assertThat(result.get("local_ts_nanos")).isEqualTo(DateTimeUtil.nanosFromTimestamp(TS_NANOS));
  }

  @Test
  public void internalReaderKeepsRawValues() throws IOException {
    Schema avroSchema = avroSchema();
    InternalReader<Record> reader = InternalReader.create(ICEBERG_SCHEMA);
    reader.setSchema(avroSchema);

    Record result = readRecord(reader, avroSchema, avroRecord(avroSchema));

    assertThat(result.getField("time_millis")).isEqualTo(DateTimeUtil.microsFromTime(TIME_MILLIS));
    assertThat(result.getField("time_micros")).isEqualTo(DateTimeUtil.microsFromTime(TIME_MICROS));
    assertThat(result.getField("local_ts_millis"))
        .isEqualTo(DateTimeUtil.microsFromTimestamp(TS_MILLIS));
    assertThat(result.getField("local_ts_micros"))
        .isEqualTo(DateTimeUtil.microsFromTimestamp(TS_MICROS));
    assertThat(result.getField("local_ts_nanos"))
        .isEqualTo(DateTimeUtil.nanosFromTimestamp(TS_NANOS));
  }

  private <T> T readRecord(DatumReader<T> reader, Schema avroSchema, GenericRecord avroRecord)
      throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
      writer.write(avroRecord, encoder);
      encoder.flush();

      try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
        return reader.read(null, DecoderFactory.get().binaryDecoder(in, null));
      }
    }
  }
}
