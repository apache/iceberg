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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

public class TestGenericDataUtil {

  @Test
  public void testDateConversion() {
    int days = DateTimeUtil.daysFromDate(LocalDate.of(2025, 1, 15));

    Object result = GenericDataUtil.internalToGeneric(Types.DateType.get(), days);

    assertThat(result).isInstanceOf(LocalDate.class).isEqualTo(LocalDate.of(2025, 1, 15));
  }

  @Test
  public void testTimeConversion() {
    long micros = DateTimeUtil.microsFromTime(LocalTime.of(11, 25, 20, 111_456_000));

    Object result = GenericDataUtil.internalToGeneric(Types.TimeType.get(), micros);

    assertThat(result)
        .isInstanceOf(LocalTime.class)
        .isEqualTo(LocalTime.of(11, 25, 20, 111_456_000));
  }

  @Test
  public void testTimestampConversion() {
    long micros = DateTimeUtil.isoTimestampToMicros("2025-01-15T11:25:20.111456");

    Object withoutZone =
        GenericDataUtil.internalToGeneric(Types.TimestampType.withoutZone(), micros);

    Object withZone = GenericDataUtil.internalToGeneric(Types.TimestampType.withZone(), micros);

    assertThat(withoutZone)
        .as("TIMESTAMP without zone should materialize as LocalDateTime")
        .isInstanceOf(LocalDateTime.class)
        .isEqualTo(LocalDateTime.of(2025, 1, 15, 11, 25, 20, 111_456_000));

    assertThat(withZone)
        .as("TIMESTAMP with zone should materialize as OffsetDateTime")
        .isInstanceOf(OffsetDateTime.class)
        .isEqualTo(OffsetDateTime.parse("2025-01-15T11:25:20.111456Z"));
  }

  @Test
  public void testTimestampNanoConversion() {
    long nanos = DateTimeUtil.isoTimestampToNanos("2025-01-15T11:25:20.111456789");

    Object withoutZone =
        GenericDataUtil.internalToGeneric(Types.TimestampNanoType.withoutZone(), nanos);

    Object withZone = GenericDataUtil.internalToGeneric(Types.TimestampNanoType.withZone(), nanos);

    assertThat(withoutZone)
        .as("TIMESTAMP_NANO without zone should materialize as LocalDateTime")
        .isInstanceOf(LocalDateTime.class)
        .isEqualTo(LocalDateTime.of(2025, 1, 15, 11, 25, 20, 111_456_789));

    assertThat(withZone)
        .as("TIMESTAMP_NANO with zone should materialize as OffsetDateTime")
        .isInstanceOf(OffsetDateTime.class)
        .isEqualTo(OffsetDateTime.parse("2025-01-15T11:25:20.111456789Z"));
  }

  @Test
  public void testFixedConversion() {
    byte[] bytes = new byte[] {1, 2, 3, 4};
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    Object result = GenericDataUtil.internalToGeneric(Types.FixedType.ofLength(4), buffer);

    assertThat(result).isInstanceOf(byte[].class).isEqualTo(bytes);
  }

  @Test
  public void testNullValueConversion() {
    Object result = GenericDataUtil.internalToGeneric(Types.IntegerType.get(), null);
    assertThat(result).isNull();
  }
}
