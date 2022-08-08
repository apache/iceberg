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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.ScanReport.TimerResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class TimerResultParser {
  private static final String MISSING_FIELD_ERROR_MSG =
      "Cannot parse timer from '%s': Missing field '%s'";

  private static final String NAME = "name";
  private static final String TIME_UNIT = "time-unit";
  private static final String COUNT = "count";
  private static final String TOTAL_DURATION = "total-duration";

  private TimerResultParser() {}

  static String toJson(TimerResult timerResult) {
    return toJson(timerResult, false);
  }

  static String toJson(TimerResult timerResult, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(timerResult, gen), pretty);
  }

  static void toJson(TimerResult timer, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != timer, "Invalid timer: null");

    gen.writeStartObject();
    gen.writeStringField(NAME, timer.name());
    gen.writeNumberField(COUNT, timer.count());
    gen.writeStringField(TIME_UNIT, timer.timeUnit().name());
    gen.writeNumberField(TOTAL_DURATION, fromDuration(timer.totalDuration(), timer.timeUnit()));
    gen.writeEndObject();
  }

  static TimerResult fromJson(String json) {
    return JsonUtil.parse(json, TimerResultParser::fromJson);
  }

  static TimerResult fromJson(String property, JsonNode json) {
    return fromJson(get(property, json));
  }

  static TimerResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse timer from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse timer from non-object: %s", json);

    String name = JsonUtil.getString(NAME, json);
    long count = JsonUtil.getLong(COUNT, json);
    TimeUnit unit = TimeUnit.valueOf(JsonUtil.getString(TIME_UNIT, json));
    long duration = JsonUtil.getLong(TOTAL_DURATION, json);
    return new TimerResult(name, unit, toDuration(duration, unit), count);
  }

  private static JsonNode get(String property, JsonNode node) {
    Preconditions.checkArgument(
        node.has(property), "Cannot parse timer from missing object '%s'", property);

    JsonNode timer = node.get(property);
    Preconditions.checkArgument(timer.has(NAME), MISSING_FIELD_ERROR_MSG, property, NAME);
    Preconditions.checkArgument(timer.has(COUNT), MISSING_FIELD_ERROR_MSG, property, COUNT);
    Preconditions.checkArgument(timer.has(TIME_UNIT), MISSING_FIELD_ERROR_MSG, property, TIME_UNIT);
    Preconditions.checkArgument(
        timer.has(TOTAL_DURATION), MISSING_FIELD_ERROR_MSG, property, TOTAL_DURATION);
    return timer;
  }

  @VisibleForTesting
  static long fromDuration(Duration duration, TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return duration.toNanos();
      case MICROSECONDS:
        return duration.toNanos() * 1000L;
      case MILLISECONDS:
        return duration.toMillis();
      case SECONDS:
        return duration.toMillis() * 1000L;
      case MINUTES:
        return duration.toMinutes();
      case HOURS:
        return duration.toHours();
      case DAYS:
        return duration.toDays();
      default:
        throw new IllegalArgumentException("Cannot determine value from time unit: " + unit);
    }
  }

  @VisibleForTesting
  static Duration toDuration(long val, TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return Duration.ofNanos(val);
      case MICROSECONDS:
        return Duration.ofNanos(val / 1000L);
      case MILLISECONDS:
        return Duration.ofMillis(val);
      case SECONDS:
        return Duration.ofMillis(val / 1000L);
      case MINUTES:
        return Duration.ofMinutes(val);
      case HOURS:
        return Duration.ofHours(val);
      case DAYS:
        return Duration.ofDays(val);
      default:
        throw new IllegalArgumentException("Cannot determine value from time unit: " + unit);
    }
  }
}
