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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

class TestDataWriter {

  @Test
  public void timestampDataWriterWithLegacyMappingDisabled() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts_tz", Types.TimestampType.withZone()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.required(3, "ts_tz_ns", Types.TimestampNanoType.withZone()),
            Types.NestedField.required(4, "ts_ns", Types.TimestampNanoType.withoutZone()));

    Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, "test", false);

    Record record = GenericRecord.create(icebergSchema);
    OffsetDateTime tsTz = OffsetDateTime.of(2023, 10, 15, 14, 30, 45, 123456000, ZoneOffset.UTC);
    LocalDateTime ts = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123456000);
    OffsetDateTime tsTzNs = OffsetDateTime.of(2023, 10, 15, 14, 30, 45, 123456789, ZoneOffset.UTC);
    LocalDateTime tsNs = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123456789);

    record.setField("ts_tz", tsTz);
    record.setField("ts", ts);
    record.setField("ts_tz_ns", tsTzNs);
    record.setField("ts_ns", tsNs);

    GenericData.Record avroRecord = writeAndReadWithAvroReader(avroSchema, record, false);

    assertThat(avroRecord.get("ts_tz")).isEqualTo(DateTimeUtil.microsFromTimestamptz(tsTz));
    assertThat(avroRecord.get("ts")).isEqualTo(DateTimeUtil.microsFromTimestamp(ts));
    assertThat(avroRecord.get("ts_tz_ns")).isEqualTo(DateTimeUtil.nanosFromTimestamptz(tsTzNs));
    assertThat(avroRecord.get("ts_ns")).isEqualTo(DateTimeUtil.nanosFromTimestamp(tsNs));
  }

  private GenericData.Record writeAndReadWithAvroReader(
      Schema avroSchema, Record record, boolean legacyTimestampMapping) throws IOException {
    DataWriter<Record> writer = DataWriter.create(avroSchema, legacyTimestampMapping);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(record, EncoderFactory.get().directBinaryEncoder(out, null));

    GenericDatumReader<GenericData.Record> avroReader = new GenericDatumReader<>(avroSchema);
    return avroReader.read(
        null,
        DecoderFactory.get()
            .directBinaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));
  }
}
