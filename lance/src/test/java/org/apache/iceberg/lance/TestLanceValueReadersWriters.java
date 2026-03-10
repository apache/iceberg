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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link LanceValueWriters} and {@link LanceValueReaders}. */
public class TestLanceValueReadersWriters {

  @Test
  public void testBooleanRoundTrip() {
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.booleans().write(true, 0, buffer);
    assertThat(buffer.get(0)).isEqualTo(true);

    Boolean result = LanceValueReaders.booleans().read(buffer.get(0));
    assertThat(result).isTrue();
  }

  @Test
  public void testIntegerRoundTrip() {
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.ints().write(42, 0, buffer);
    assertThat(buffer.get(0)).isEqualTo(42);

    Integer result = LanceValueReaders.ints().read(buffer.get(0));
    assertThat(result).isEqualTo(42);
  }

  @Test
  public void testLongRoundTrip() {
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.longs().write(Long.MAX_VALUE, 0, buffer);

    Long result = LanceValueReaders.longs().read(buffer.get(0));
    assertThat(result).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testFloatRoundTrip() {
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.floats().write(3.14f, 0, buffer);

    Float result = LanceValueReaders.floats().read(buffer.get(0));
    assertThat(result).isEqualTo(3.14f);
  }

  @Test
  public void testDoubleRoundTrip() {
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.doubles().write(2.718281828, 0, buffer);

    Double result = LanceValueReaders.doubles().read(buffer.get(0));
    assertThat(result).isEqualTo(2.718281828);
  }

  @Test
  public void testStringRoundTrip() {
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.strings().write("hello world", 0, buffer);

    String result = LanceValueReaders.strings().read(buffer.get(0));
    assertThat(result).isEqualTo("hello world");
  }

  @Test
  public void testBinaryRoundTrip() {
    byte[] original = {0x01, 0x02, 0x03, 0x04};
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.byteBuffers().write(ByteBuffer.wrap(original), 0, buffer);

    ByteBuffer result = LanceValueReaders.byteBuffers().read(buffer.get(0));
    byte[] resultBytes = new byte[result.remaining()];
    result.get(resultBytes);
    assertThat(resultBytes).isEqualTo(original);
  }

  @Test
  public void testUuidRoundTrip() {
    UUID original = UUID.randomUUID();
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.uuids().write(original, 0, buffer);

    UUID result = LanceValueReaders.uuids().read(buffer.get(0));
    assertThat(result).isEqualTo(original);
  }

  @Test
  public void testDateRoundTrip() {
    LocalDate original = LocalDate.of(2025, 3, 10);
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.dates().write(original, 0, buffer);

    // Writer converts to epoch days (int)
    LocalDate result = LanceValueReaders.dates().read(buffer.get(0));
    assertThat(result).isEqualTo(original);
  }

  @Test
  public void testTimeRoundTrip() {
    LocalTime original = LocalTime.of(14, 30, 45);
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.times().write(original, 0, buffer);

    LocalTime result = LanceValueReaders.times().read(buffer.get(0));
    assertThat(result).isEqualTo(original);
  }

  @Test
  public void testTimestampRoundTrip() {
    LocalDateTime original = LocalDateTime.of(2025, 3, 10, 14, 30, 45);
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.timestamps().write(original, 0, buffer);

    LocalDateTime result = LanceValueReaders.timestamps().read(buffer.get(0));
    assertThat(result).isEqualTo(original);
  }

  @Test
  public void testTimestampTzRoundTrip() {
    OffsetDateTime original = OffsetDateTime.of(2025, 3, 10, 14, 30, 45, 0, ZoneOffset.UTC);
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.timestampTzs().write(original, 0, buffer);

    OffsetDateTime result = LanceValueReaders.timestampTzs().read(buffer.get(0));
    assertThat(result).isEqualTo(original);
  }

  @Test
  public void testDecimalRoundTrip() {
    BigDecimal original = new BigDecimal("12345.678900");
    List<Object> buffer = new ArrayList<>();
    LanceValueWriters.decimals().write(original, 0, buffer);

    BigDecimal result = LanceValueReaders.decimals().read(buffer.get(0));
    assertThat(result).isEqualByComparingTo(original);
  }

  @Test
  public void testForTypeBoolean() {
    LanceValueWriters.ValueWriter<Object> writer =
        LanceValueWriters.forType(Types.BooleanType.get());
    List<Object> buffer = new ArrayList<>();
    writer.write(false, 0, buffer);
    assertThat(buffer.get(0)).isEqualTo(false);

    LanceValueReaders.ValueReader<Object> reader =
        LanceValueReaders.forType(Types.BooleanType.get());
    assertThat(reader.read(buffer.get(0))).isEqualTo(false);
  }

  @Test
  public void testForTypeString() {
    LanceValueWriters.ValueWriter<Object> writer =
        LanceValueWriters.forType(Types.StringType.get());
    List<Object> buffer = new ArrayList<>();
    writer.write("test", 0, buffer);

    LanceValueReaders.ValueReader<Object> reader =
        LanceValueReaders.forType(Types.StringType.get());
    assertThat(reader.read(buffer.get(0))).isEqualTo("test");
  }

  @Test
  public void testForTypeTimestampWithZone() {
    LanceValueWriters.ValueWriter<Object> writer =
        LanceValueWriters.forType(Types.TimestampType.withZone());
    List<Object> buffer = new ArrayList<>();
    OffsetDateTime value = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    writer.write(value, 0, buffer);

    LanceValueReaders.ValueReader<Object> reader =
        LanceValueReaders.forType(Types.TimestampType.withZone());
    assertThat(reader.read(buffer.get(0))).isEqualTo(value);
  }

  @Test
  public void testForTypeTimestampWithoutZone() {
    LanceValueWriters.ValueWriter<Object> writer =
        LanceValueWriters.forType(Types.TimestampType.withoutZone());
    List<Object> buffer = new ArrayList<>();
    LocalDateTime value = LocalDateTime.of(2025, 6, 15, 12, 0, 0);
    writer.write(value, 0, buffer);

    LanceValueReaders.ValueReader<Object> reader =
        LanceValueReaders.forType(Types.TimestampType.withoutZone());
    assertThat(reader.read(buffer.get(0))).isEqualTo(value);
  }
}
