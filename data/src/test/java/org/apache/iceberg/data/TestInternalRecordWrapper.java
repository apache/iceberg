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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.jupiter.api.Test;

public class TestInternalRecordWrapper {

  @Test
  public void testDateConversion() {
    Schema schema = new Schema(required(1, "date_field", Types.DateType.get()));

    LocalDate testDate = LocalDate.of(2025, 10, 1);
    Record record = GenericRecord.create(schema);
    record.set(0, testDate);

    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    StructLike wrappedRecord = wrapper.wrap(record);

    Integer convertedValue = wrappedRecord.get(0, Integer.class);
    Integer expectedValue = DateTimeUtil.daysFromDate(testDate);

    assertThat(convertedValue).isEqualTo(expectedValue);
  }

  @Test
  public void testTimeConversion() {
    Schema schema = new Schema(required(1, "time_field", Types.TimeType.get()));

    LocalTime testTime = LocalTime.of(14, 30, 45, 123456000);
    Record record = GenericRecord.create(schema);
    record.set(0, testTime);

    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    StructLike wrappedRecord = wrapper.wrap(record);

    Long convertedValue = wrappedRecord.get(0, Long.class);
    Long expectedValue = DateTimeUtil.microsFromTime(testTime);

    assertThat(convertedValue).isEqualTo(expectedValue);
  }

  @Test
  public void testTimestampWithZoneConversion() {
    Schema schema = new Schema(required(1, "timestamp_tz_field", Types.TimestampType.withZone()));

    OffsetDateTime testTimestamp =
        OffsetDateTime.of(2025, 10, 1, 14, 30, 45, 123456000, ZoneOffset.UTC);
    Record record = GenericRecord.create(schema);
    record.set(0, testTimestamp);

    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    StructLike wrappedRecord = wrapper.wrap(record);

    Long convertedValue = wrappedRecord.get(0, Long.class);
    Long expectedValue = DateTimeUtil.microsFromTimestamptz(testTimestamp);

    assertThat(convertedValue).isEqualTo(expectedValue);
  }

  @Test
  public void testTimestampWithoutZoneConversion() {
    Schema schema = new Schema(required(1, "timestamp_field", Types.TimestampType.withoutZone()));

    LocalDateTime testTimestamp = LocalDateTime.of(2025, 10, 1, 14, 30, 45, 123456000);
    Record record = GenericRecord.create(schema);
    record.set(0, testTimestamp);

    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    StructLike wrappedRecord = wrapper.wrap(record);

    Long convertedValue = wrappedRecord.get(0, Long.class);
    Long expectedValue = DateTimeUtil.microsFromTimestamp(testTimestamp);

    assertThat(convertedValue).isEqualTo(expectedValue);
  }

  @Test
  public void testFixedConversion() {
    Schema schema = new Schema(required(1, "fixed_field", Types.FixedType.ofLength(16)));

    byte[] testBytes = "test fixed bytes".getBytes();
    Record record = GenericRecord.create(schema);
    record.set(0, testBytes);

    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    StructLike wrappedRecord = wrapper.wrap(record);

    ByteBuffer convertedValue = wrappedRecord.get(0, ByteBuffer.class);
    ByteBuffer expectedValue = ByteBuffer.wrap(testBytes);

    assertThat(convertedValue).isEqualTo(expectedValue);
  }

  @Test
  public void testUuidConversion() {
    Schema schema = new Schema(required(1, "uuid_field", Types.UUIDType.get()));

    byte[] uuidBytes = new byte[16];
    Random random = new Random();
    random.nextBytes(uuidBytes);
    Record record = GenericRecord.create(schema);
    record.set(0, uuidBytes);

    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    StructLike wrappedRecord = wrapper.wrap(record);

    UUID convertedValue = wrappedRecord.get(0, UUID.class);
    UUID expectedValue = UUIDUtil.convert(uuidBytes);

    assertThat(convertedValue).isEqualTo(expectedValue);
  }
}
