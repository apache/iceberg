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
package org.apache.iceberg.expressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.format.DateTimeParseException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

public class TestTimestampLiteralConversions {
  @Test
  public void testTimestampToTimestampNanoConversion() {
    Literal<Long> timestamp =
        Literal.of("2017-11-16T14:31:08.000000001").to(Types.TimestampType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(1510842668000000L);

    Literal<Long> timestampNano = timestamp.to(Types.TimestampNanoType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(1510842668000000000L);

    timestamp = Literal.of("1970-01-01T00:00:00.000000001").to(Types.TimestampType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(0L);

    timestampNano = timestamp.to(Types.TimestampNanoType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(0L);

    timestamp = Literal.of("1969-12-31T23:59:59.999999999").to(Types.TimestampType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(0L);

    timestampNano = timestamp.to(Types.TimestampNanoType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(0L);

    timestamp = Literal.of("1969-12-31T23:59:59.999999000").to(Types.TimestampType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(-1L);

    timestampNano = timestamp.to(Types.TimestampNanoType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(-1000L);
  }

  @Test
  public void testTimestampToDateConversion() {
    Literal<Long> ts =
        Literal.of("2017-11-16T14:31:08.000001").to(Types.TimestampType.withoutZone());
    int dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(DateTimeUtil.isoDateToDays("2017-11-16"));

    ts = Literal.of("1970-01-01T00:00:00.000001").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(0);

    ts = Literal.of("1969-12-31T23:59:59.999999").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(-1);

    ts = Literal.of("2017-11-16T14:31:08.000000001").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(DateTimeUtil.isoDateToDays("2017-11-16"));

    ts = Literal.of("1970-01-01T00:00:00.000000001").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(0).isEqualTo(DateTimeUtil.isoDateToDays("1970-01-01"));

    ts = Literal.of("1969-12-31T23:59:59.999999999").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(0).isEqualTo(DateTimeUtil.isoDateToDays("1970-01-01"));

    ts = Literal.of("1969-12-31T23:59:59.999999000").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(-1).isEqualTo(DateTimeUtil.isoDateToDays("1969-12-31"));
  }

  @Test
  public void testTimestampMicrosToDateConversion() {
    Literal<Long> ts =
        Literal.of("2017-11-16T14:31:08.000000001").to(Types.TimestampType.withoutZone());
    int dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(DateTimeUtil.isoDateToDays("2017-11-16"));

    ts = Literal.of("1970-01-01T00:00:00.000000001").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(0);

    ts = Literal.of("1969-12-31T23:59:59.999999999").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(0);

    ts = Literal.of("1969-12-31T23:59:59.999999000").to(Types.TimestampType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(-1);
  }

  @Test
  public void testTimestampNanoToTimestampConversion() {
    Literal<Long> timestamp =
        Literal.of("2017-11-16T14:31:08.000000001").to(Types.TimestampNanoType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(1510842668000000001L);

    Literal<Long> timestampNano = timestamp.to(Types.TimestampType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(1510842668000000L);

    timestamp =
        Literal.of("1970-01-01T00:00:00.000000001").to(Types.TimestampNanoType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(1L);

    timestampNano = timestamp.to(Types.TimestampType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(0L);

    timestamp =
        Literal.of("1969-12-31T23:59:59.999999999").to(Types.TimestampNanoType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(-1L);

    timestampNano = timestamp.to(Types.TimestampType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(-1L);

    timestamp =
        Literal.of("1969-12-31T23:59:59.999999000").to(Types.TimestampNanoType.withoutZone());
    assertThat(timestamp.value()).isEqualTo(-1000L);

    timestampNano = timestamp.to(Types.TimestampType.withoutZone());
    assertThat(timestampNano.value()).isEqualTo(-1L);
  }

  @Test
  public void testTimestampNanosToDateConversion() {
    Literal<Long> ts =
        Literal.of("2017-11-16T14:31:08.000000001").to(Types.TimestampNanoType.withoutZone());
    int dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(DateTimeUtil.isoDateToDays("2017-11-16"));

    ts = Literal.of("1970-01-01T00:00:00.000000001").to(Types.TimestampNanoType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(0);

    ts = Literal.of("1969-12-31T23:59:59.999999999").to(Types.TimestampNanoType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(-1);

    ts = Literal.of("1969-12-31T23:59:59.999999000").to(Types.TimestampNanoType.withoutZone());
    dateOrdinal = (Integer) ts.to(Types.DateType.get()).value();
    assertThat(dateOrdinal).isEqualTo(-1);
  }

  @Test
  public void testTimestampNanosWithZoneConversion() {
    Literal<CharSequence> isoTimestampNanosWithZoneOffset =
        Literal.of("2017-11-16T14:31:08.000000001+00:00");

    assertThatThrownBy(
        () -> isoTimestampNanosWithZoneOffset.to(Types.TimestampType.withoutZone()))
        .as("Should not convert timestamp with offset to a timestamp without zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThatThrownBy(
            () -> isoTimestampNanosWithZoneOffset.to(Types.TimestampNanoType.withoutZone()))
        .as("Should not convert timestamp with offset to a timestamp without zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThat(isoTimestampNanosWithZoneOffset.to(Types.TimestampType.withZone()).value())
        .isEqualTo(1510842668000000L);

    assertThat(isoTimestampNanosWithZoneOffset.to(Types.TimestampNanoType.withZone()).value())
        .isEqualTo(1510842668000000001L);
  }


  @Test
  public void testTimestampMicrosWithZoneConversion() {
    Literal<CharSequence> isoTimestampMicrosWithZoneOffset =
        Literal.of("2017-11-16T14:31:08.000001+00:00");

    assertThatThrownBy(
        () -> isoTimestampMicrosWithZoneOffset.to(Types.TimestampType.withoutZone()))
        .as("Should not convert timestamp with offset to a timestamp without zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThatThrownBy(
        () -> isoTimestampMicrosWithZoneOffset.to(Types.TimestampNanoType.withoutZone()))
        .as("Should not convert timestamp with offset to a timestamp without zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThat(isoTimestampMicrosWithZoneOffset.to(Types.TimestampType.withZone()).value())
        .isEqualTo(1510842668000001L);

    assertThat(isoTimestampMicrosWithZoneOffset.to(Types.TimestampNanoType.withZone()).value())
        .isEqualTo(1510842668000001000L);
  }

  @Test
  public void testTimestampNanosWithoutZoneConversion() {
    Literal<CharSequence> isoTimestampNanosWithoutZoneOffset =
        Literal.of("2017-11-16T14:31:08.000000001");

    assertThatThrownBy(
        () -> isoTimestampNanosWithoutZoneOffset.to(Types.TimestampType.withZone()))
        .as("Should not convert timestamp without offset to a timestamp with zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThatThrownBy(
        () -> isoTimestampNanosWithoutZoneOffset.to(Types.TimestampNanoType.withZone()))
        .as("Should not convert timestamp without offset to a timestamp with zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThat(isoTimestampNanosWithoutZoneOffset.to(Types.TimestampType.withoutZone()).value())
        .isEqualTo(1510842668000000L);

    assertThat(isoTimestampNanosWithoutZoneOffset.to(Types.TimestampNanoType.withoutZone()).value())
        .isEqualTo(1510842668000000001L);
  }

  @Test
  public void testTimestampMicrosWithoutZoneConversion() {
    Literal<CharSequence> isoTimestampMicrosWithoutZoneOffset =
        Literal.of("2017-11-16T14:31:08.000001");

    assertThatThrownBy(
        () -> isoTimestampMicrosWithoutZoneOffset.to(Types.TimestampType.withZone()))
        .as("Should not convert timestamp without offset to a timestamp with zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThatThrownBy(
        () -> isoTimestampMicrosWithoutZoneOffset.to(Types.TimestampNanoType.withZone()))
        .as("Should not convert timestamp without offset to a timestamp with zone")
        .isInstanceOf(DateTimeParseException.class);

    assertThat(isoTimestampMicrosWithoutZoneOffset.to(Types.TimestampType.withoutZone()).value())
        .isEqualTo(1510842668000001L);

    assertThat(isoTimestampMicrosWithoutZoneOffset.to(Types.TimestampNanoType.withoutZone()).value())
        .isEqualTo(1510842668000001000L);
  }
}
