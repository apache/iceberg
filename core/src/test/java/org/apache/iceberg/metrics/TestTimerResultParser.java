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
package org.apache.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestTimerResultParser {

  @Test
  public void nullTimer() {
    assertThatThrownBy(() -> TimerResultParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timer from null object");

    assertThatThrownBy(() -> TimerResultParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid timer: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> TimerResultParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: count");

    assertThatThrownBy(() -> TimerResultParser.fromJson("{\"count\":44}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: time-unit");

    assertThatThrownBy(() -> TimerResultParser.fromJson("{\"count\":44,\"time-unit\":\"hours\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: total-duration");
  }

  @Test
  public void extraFields() {
    assertThat(
            TimerResultParser.fromJson(
                "{\"count\":44,\"time-unit\":\"hours\",\"total-duration\":24,\"extra\": \"value\"}"))
        .isEqualTo(TimerResult.of(TimeUnit.HOURS, Duration.ofHours(24), 44));
  }

  @Test
  public void unsupportedDuration() {
    assertThatThrownBy(
            () ->
                TimerResultParser.fromJson(
                    "{\"count\":44,\"time-unit\":\"hours\",\"total-duration\":\"xx\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: total-duration: \"xx\"");
  }

  @Test
  public void unsupportedTimeUnit() {
    assertThatThrownBy(
            () ->
                TimerResultParser.fromJson(
                    "{\"count\":44,\"time-unit\":\"unknown\",\"total-duration\":24}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid time unit: unknown");
  }

  @Test
  public void invalidCount() {
    assertThatThrownBy(
            () ->
                TimerResultParser.fromJson(
                    "{\"count\":\"illegal\",\"time-unit\":\"hours\",\"total-duration\":24}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: count: \"illegal\"");
  }

  @Test
  public void roundTripSerde() {
    TimerResult timer = TimerResult.of(TimeUnit.HOURS, Duration.ofHours(23), 44);

    String json = TimerResultParser.toJson(timer);
    assertThat(TimerResultParser.fromJson(json)).isEqualTo(timer);
    assertThat(json).isEqualTo("{\"count\":44,\"time-unit\":\"hours\",\"total-duration\":23}");
  }

  @Test
  public void toDuration() {
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.NANOSECONDS))
        .isEqualTo(Duration.ofNanos(5L));
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.MICROSECONDS))
        .isEqualTo(Duration.of(5L, ChronoUnit.MICROS));
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.MILLISECONDS))
        .isEqualTo(Duration.ofMillis(5L));
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.SECONDS))
        .isEqualTo(Duration.ofSeconds(5L));
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.MINUTES))
        .isEqualTo(Duration.ofMinutes(5L));
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.HOURS)).isEqualTo(Duration.ofHours(5L));
    assertThat(TimerResultParser.toDuration(5L, TimeUnit.DAYS)).isEqualTo(Duration.ofDays(5L));
  }

  @Test
  public void fromDuration() {
    assertThat(TimerResultParser.fromDuration(Duration.ofNanos(5L), TimeUnit.NANOSECONDS))
        .isEqualTo(5L);
    assertThat(
            TimerResultParser.fromDuration(
                Duration.of(5L, ChronoUnit.MICROS), TimeUnit.MICROSECONDS))
        .isEqualTo(5L);
    assertThat(TimerResultParser.fromDuration(Duration.ofMillis(5L), TimeUnit.MILLISECONDS))
        .isEqualTo(5L);
    assertThat(TimerResultParser.fromDuration(Duration.ofSeconds(5L), TimeUnit.SECONDS))
        .isEqualTo(5L);
    assertThat(TimerResultParser.fromDuration(Duration.ofMinutes(5L), TimeUnit.MINUTES))
        .isEqualTo(5L);
    assertThat(TimerResultParser.fromDuration(Duration.ofHours(5L), TimeUnit.HOURS)).isEqualTo(5L);
    assertThat(TimerResultParser.fromDuration(Duration.ofDays(5L), TimeUnit.DAYS)).isEqualTo(5L);
  }
}
