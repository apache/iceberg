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
package org.apache.iceberg.data.avro;

import static org.apache.iceberg.avro.AvroSchemaUtil.ADJUST_TO_UTC_PROP;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

class TestDataReader {

  @Test
  public void testTimestampDataReader() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "timestamp_nanos", Types.TimestampType.withoutZone()),
            Types.NestedField.required(2, "timestamp_micros", Types.TimestampType.withoutZone()),
            Types.NestedField.required(3, "timestamp_millis", Types.TimestampType.withoutZone()));

    Schema avroSchema =
        SchemaBuilder.record("test_programmatic")
            .fields()
            .name("timestamp_nanos")
            .type(LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("timestamp_micros")
            .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name("timestamp_millis")
            .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .endRecord();

    avroSchema.getField("timestamp_nanos").addProp("field-id", 1);
    avroSchema.getField("timestamp_micros").addProp("field-id", 2);
    avroSchema.getField("timestamp_millis").addProp("field-id", 3);

    DataReader<Record> reader = DataReader.create(icebergSchema, avroSchema);
    reader.setSchema(avroSchema);

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    LocalDateTime timestampNanos = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123456789);
    LocalDateTime timestampMicros = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123456000);
    LocalDateTime timestampMillis = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123000000);

    avroRecord.put("timestamp_nanos", DateTimeUtil.nanosFromTimestamp(timestampNanos));
    avroRecord.put("timestamp_micros", DateTimeUtil.microsFromTimestamp(timestampMicros));
    avroRecord.put("timestamp_millis", DateTimeUtil.millisFromTimestamp(timestampMillis));

    Record result = readRecord(reader, avroSchema, avroRecord);

    assertThat(result.getField("timestamp_nanos")).isEqualTo(timestampNanos);
    assertThat(result.getField("timestamp_micros")).isEqualTo(timestampMicros);
    assertThat(result.getField("timestamp_millis")).isEqualTo(timestampMillis);
  }

  @Test
  public void testTimestampTzDataReader() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "timestamp_nanos_tz", Types.TimestampType.withZone()),
            Types.NestedField.required(2, "timestamp_micros_tz", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "timestamp_millis_tz", Types.TimestampType.withZone()));

    Schema avroSchema =
        SchemaBuilder.record("test_tz")
            .fields()
            .name("timestamp_nanos_tz")
            .type(LogicalTypes.timestampNanos().addToSchema(utcAdjustedLongSchema()))
            .noDefault()
            .name("timestamp_micros_tz")
            .type(LogicalTypes.timestampMicros().addToSchema(utcAdjustedLongSchema()))
            .noDefault()
            .name("timestamp_millis_tz")
            .type(LogicalTypes.timestampMillis().addToSchema(utcAdjustedLongSchema()))
            .noDefault()
            .endRecord();

    avroSchema.getField("timestamp_nanos_tz").addProp("field-id", 1);
    avroSchema.getField("timestamp_micros_tz").addProp("field-id", 2);
    avroSchema.getField("timestamp_millis_tz").addProp("field-id", 3);

    DataReader<Record> reader = DataReader.create(icebergSchema, avroSchema);
    reader.setSchema(avroSchema);

    GenericRecord avroRecord = new GenericData.Record(avroSchema);

    OffsetDateTime offsetTimestampNanos =
        OffsetDateTime.of(2023, 10, 15, 14, 30, 45, 123456789, ZoneOffset.ofHours(-8));
    OffsetDateTime offsetTimestampMicros =
        OffsetDateTime.of(2023, 10, 15, 14, 30, 45, 123456000, ZoneOffset.ofHours(5));
    OffsetDateTime offsetTimestampMillis =
        OffsetDateTime.of(2023, 10, 15, 14, 30, 45, 123000000, ZoneOffset.ofHours(-3));

    avroRecord.put("timestamp_nanos_tz", DateTimeUtil.nanosFromTimestamptz(offsetTimestampNanos));
    avroRecord.put(
        "timestamp_micros_tz", DateTimeUtil.microsFromTimestamptz(offsetTimestampMicros));
    avroRecord.put(
        "timestamp_millis_tz", DateTimeUtil.millisFromTimestamptz(offsetTimestampMillis));

    Record result = readRecord(reader, avroSchema, avroRecord);

    assertThat(result.getField("timestamp_nanos_tz"))
        .isEqualTo(offsetTimestampNanos.withOffsetSameInstant(ZoneOffset.UTC));
    assertThat(result.getField("timestamp_micros_tz"))
        .isEqualTo(offsetTimestampMicros.withOffsetSameInstant(ZoneOffset.UTC));
    assertThat(result.getField("timestamp_millis_tz"))
        .isEqualTo(offsetTimestampMillis.withOffsetSameInstant(ZoneOffset.UTC));
  }

  private Record readRecord(DataReader<Record> reader, Schema avroSchema, GenericRecord avroRecord)
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

  private Schema utcAdjustedLongSchema() {
    Schema schema = Schema.create(Schema.Type.LONG);
    schema.addProp(ADJUST_TO_UTC_PROP, "true");
    return schema;
  }
}
