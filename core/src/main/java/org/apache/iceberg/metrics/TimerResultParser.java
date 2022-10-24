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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class TimerResultParser {
  private static final String MISSING_FIELD_ERROR_MSG =
      "Cannot parse timer from '%s': Missing field '%s'";

  private static final String TIME_UNIT = "time-unit";
  private static final String COUNT = "count";
  private static final String TOTAL_DURATION = "total-duration";

  private TimerResultParser() {}

  static String toJson(TimerResult timer) {
    return toJson(timer, false);
  }

  static String toJson(TimerResult timer, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(timer, gen), pretty);
  }

  static void toJson(TimerResult timer, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != timer, "Invalid timer: null");

    gen.writeStartObject();
    gen.writeNumberField(COUNT, timer.count());
    gen.writeStringField(TIME_UNIT, timer.timeUnit().name().toLowerCase(Locale.ENGLISH));
    gen.writeNumberField(TOTAL_DURATION, fromDuration(timer.totalDuration(), timer.timeUnit()));
    gen.writeEndObject();
  }

  static TimerResult fromJson(String json) {
    return JsonUtil.parse(json, TimerResultParser::fromJson);
  }

  static TimerResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse timer from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse timer from non-object: %s", json);

    long count = JsonUtil.getLong(COUNT, json);
    TimeUnit unit = toTimeUnit(JsonUtil.getString(TIME_UNIT, json));
    long duration = JsonUtil.getLong(TOTAL_DURATION, json);
    return TimerResult.of(unit, toDuration(duration, unit), count);
  }

  /**
   * This is mainly used from {@link ScanMetricsResultParser} where the timer name is already part
   * of the parent {@link JsonNode}, so we omit checking and reading the timer name here.
   *
   * @param timerName The timer name
   * @param json The {@link JsonNode} containing all other timer information
   * @return A {@link TimerResult} instance
   */
  static TimerResult fromJson(String timerName, JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse timer from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse timer from non-object: %s", json);

    if (!json.has(timerName)) {
      return null;
    }

    JsonNode timer = json.get(timerName);
    Preconditions.checkArgument(timer.has(COUNT), MISSING_FIELD_ERROR_MSG, timerName, COUNT);
    Preconditions.checkArgument(
        timer.has(TIME_UNIT), MISSING_FIELD_ERROR_MSG, timerName, TIME_UNIT);
    Preconditions.checkArgument(
        timer.has(TOTAL_DURATION), MISSING_FIELD_ERROR_MSG, timerName, TOTAL_DURATION);

    long count = JsonUtil.getLong(COUNT, timer);
    TimeUnit unit = toTimeUnit(JsonUtil.getString(TIME_UNIT, timer));
    long duration = JsonUtil.getLong(TOTAL_DURATION, timer);
    return TimerResult.of(unit, toDuration(duration, unit), count);
  }

  @VisibleForTesting
  static long fromDuration(Duration duration, TimeUnit unit) {
    return unit.convert(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  @VisibleForTesting
  static Duration toDuration(long val, TimeUnit unit) {
    return Duration.of(val, toChronoUnit(unit));
  }

  private static TimeUnit toTimeUnit(String timeUnit) {
    try {
      return TimeUnit.valueOf(timeUnit.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid time unit: %s", timeUnit), e);
    }
  }

  private static ChronoUnit toChronoUnit(TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return ChronoUnit.NANOS;
      case MICROSECONDS:
        return ChronoUnit.MICROS;
      case MILLISECONDS:
        return ChronoUnit.MILLIS;
      case SECONDS:
        return ChronoUnit.SECONDS;
      case MINUTES:
        return ChronoUnit.MINUTES;
      case HOURS:
        return ChronoUnit.HOURS;
      case DAYS:
        return ChronoUnit.DAYS;
      default:
        throw new IllegalArgumentException("Cannot determine chrono unit from time unit: " + unit);
    }
  }
}
