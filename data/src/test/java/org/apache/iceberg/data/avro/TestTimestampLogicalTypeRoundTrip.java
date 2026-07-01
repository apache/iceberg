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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestTimestampLogicalTypeRoundTrip {

  @TempDir private Path temp;

  private static final Schema TS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(2, "tstz", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "ts_ns", Types.TimestampNanoType.withoutZone()),
          Types.NestedField.required(4, "tstz_ns", Types.TimestampNanoType.withZone()));

  @Test
  void writerEmitsLocalTimestampForNonZoneTypes() {
    org.apache.avro.Schema avro = AvroSchemaUtil.convert(TS_SCHEMA, "test");
    assertThat(logicalTypeOf(avro, "ts")).isInstanceOf(LogicalTypes.LocalTimestampMicros.class);
    assertThat(logicalTypeOf(avro, "tstz")).isInstanceOf(LogicalTypes.TimestampMicros.class);
    assertThat(logicalTypeOf(avro, "ts_ns")).isInstanceOf(LogicalTypes.LocalTimestampNanos.class);
    assertThat(logicalTypeOf(avro, "tstz_ns")).isInstanceOf(LogicalTypes.TimestampNanos.class);
  }

  @Test
  void roundTripsAllFourTimestampTypes() throws IOException {
    LocalDateTime localTs = LocalDateTime.of(2026, 5, 27, 12, 34, 56, 123_456_000);
    OffsetDateTime utcTs = OffsetDateTime.of(localTs, ZoneOffset.UTC);
    LocalDateTime localTsNanos = LocalDateTime.of(2026, 5, 27, 12, 34, 56, 123_456_789);
    OffsetDateTime utcTsNanos = OffsetDateTime.of(localTsNanos, ZoneOffset.UTC);

    Record record = GenericRecord.create(TS_SCHEMA);
    record.setField("ts", localTs);
    record.setField("tstz", utcTs);
    record.setField("ts_ns", localTsNanos);
    record.setField("tstz_ns", utcTsNanos);

    List<Record> written = ImmutableList.of(record);
    File file = temp.resolve("timestamps-" + System.nanoTime() + ".avro").toFile();

    try (org.apache.iceberg.io.FileAppender<Record> writer =
        Avro.write(Files.localOutput(file))
            .schema(TS_SCHEMA)
            .createWriterFunc(DataWriter::create)
            .named("test")
            .build()) {
      writer.add(record);
    }

    List<Record> read;
    try (AvroIterable<Record> reader =
        Avro.read(Files.localInput(file))
            .project(TS_SCHEMA)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      read = Lists.newArrayList(reader);
    }

    assertThat(read).hasSize(1);
    Record actual = read.get(0);
    assertThat(actual.getField("ts")).isEqualTo(localTs);
    assertThat(actual.getField("tstz")).isEqualTo(utcTs);
    assertThat(actual.getField("ts_ns")).isEqualTo(localTsNanos);
    assertThat(actual.getField("tstz_ns")).isEqualTo(utcTsNanos);
    assertThat(written.get(0)).isEqualTo(actual);
  }

  @Test
  void readsLegacyAdjustToUtcFalseEncoding() {
    assertThat(AvroSchemaUtil.convert(legacyStamped(LogicalTypes.timestampMicros(), false)))
        .isEqualTo(Types.TimestampType.withoutZone());
    assertThat(AvroSchemaUtil.convert(legacyStamped(LogicalTypes.timestampNanos(), false)))
        .isEqualTo(Types.TimestampNanoType.withoutZone());
    assertThat(AvroSchemaUtil.convert(legacyStamped(LogicalTypes.timestampMicros(), true)))
        .isEqualTo(Types.TimestampType.withZone());
    assertThat(AvroSchemaUtil.convert(legacyStamped(LogicalTypes.timestampNanos(), true)))
        .isEqualTo(Types.TimestampNanoType.withZone());
  }

  private static LogicalType logicalTypeOf(org.apache.avro.Schema record, String fieldName) {
    org.apache.avro.Schema field = record.getField(fieldName).schema();
    if (field.getType() == org.apache.avro.Schema.Type.UNION) {
      for (org.apache.avro.Schema branch : field.getTypes()) {
        if (branch.getType() != org.apache.avro.Schema.Type.NULL) {
          return branch.getLogicalType();
        }
      }
    }
    return field.getLogicalType();
  }

  private static org.apache.avro.Schema legacyStamped(
      LogicalType logicalType, boolean adjustToUtc) {
    org.apache.avro.Schema stamped =
        logicalType.addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
    stamped.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, adjustToUtc);
    return stamped;
  }
}
