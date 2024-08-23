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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

public class TestDateTimeUtil {

  @Test
  public void formatTimestampMillis() {
    String timestamp = "1970-01-01T00:00:00.001+00:00";
    assertThat(DateTimeUtil.formatTimestampMillis(1L)).isEqualTo(timestamp);
    assertThat(ZonedDateTime.parse(timestamp).toInstant().toEpochMilli()).isEqualTo(1L);

    timestamp = "1970-01-01T00:16:40+00:00";
    assertThat(DateTimeUtil.formatTimestampMillis(1000000L)).isEqualTo(timestamp);
    assertThat(ZonedDateTime.parse(timestamp).toInstant().toEpochMilli()).isEqualTo(1000000L);
  }

  @Test
  public void nanosToMicros() {
    assertThat(DateTimeUtil.nanosToMicros(1510871468000001001L)).isEqualTo(1510871468000001L);
    assertThat(DateTimeUtil.nanosToMicros(-1510871468000001001L)).isEqualTo(-1510871468000002L);
  }

  @Test
  public void microsToNanos() {
    assertThat(DateTimeUtil.microsToNanos(1510871468000001L)).isEqualTo(1510871468000001000L);
    assertThat(DateTimeUtil.microsToNanos(-1510871468000001L)).isEqualTo(-1510871468000001000L);
  }

  @Test
  public void isoTimestampToNanos() {
    assertThat(DateTimeUtil.isoTimestampToNanos("2017-11-16T22:31:08.000001001"))
        .isEqualTo(1510871468000001001L);
  }

  @Test
  public void isoTimestamptzToNanos() {
    assertThat(DateTimeUtil.isoTimestamptzToNanos("2017-11-16T14:31:08.000001001-08:00"))
        .isEqualTo(1510871468000001001L);
  }

  @Test
  public void convertNanos() {
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.ERAS)).isEqualTo(0);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.MILLENNIA)).isEqualTo(0);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.CENTURIES)).isEqualTo(0);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.DECADES)).isEqualTo(4);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.YEARS)).isEqualTo(47);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.MONTHS)).isEqualTo(574);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.WEEKS)).isEqualTo(2498);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.DAYS)).isEqualTo(17486);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.HALF_DAYS))
        .isEqualTo(34973);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.HOURS)).isEqualTo(419686);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.MINUTES))
        .isEqualTo(25181191);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.SECONDS))
        .isEqualTo(1510871468);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.MILLIS))
        .isEqualTo(1510871468000L);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.MICROS))
        .isEqualTo(1510871468000001L);
    assertThat(DateTimeUtil.convertNanos(1510871468000001001L, ChronoUnit.NANOS))
        .isEqualTo(1510871468000001001L);
  }

  @Test
  public void convertNanosNegative() {
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.ERAS)).isEqualTo(-1);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.MILLENNIA))
        .isEqualTo(-1);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.CENTURIES))
        .isEqualTo(-1);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.DECADES)).isEqualTo(-5);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.YEARS)).isEqualTo(-48);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.MONTHS)).isEqualTo(-575);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.WEEKS)).isEqualTo(-2499);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.DAYS)).isEqualTo(-17487);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.HALF_DAYS))
        .isEqualTo(-34974);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.HOURS))
        .isEqualTo(-419687);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.MINUTES))
        .isEqualTo(-25181192);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.SECONDS))
        .isEqualTo(-1510871469);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.MILLIS))
        .isEqualTo(-1510871468001L);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.MICROS))
        .isEqualTo(-1510871468000002L);
    assertThat(DateTimeUtil.convertNanos(-1510871468000001001L, ChronoUnit.NANOS))
        .isEqualTo(-1510871468000001001L);
  }
}
