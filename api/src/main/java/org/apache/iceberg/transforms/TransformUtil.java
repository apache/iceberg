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
package org.apache.iceberg.transforms;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import org.apache.iceberg.util.DateTimeUtil;

class TransformUtil {

  private TransformUtil() {}

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final int EPOCH_YEAR = EPOCH.getYear();

  static String humanYear(int yearOrdinal) {
    return String.format("%04d", EPOCH_YEAR + yearOrdinal);
  }

  static String humanMonth(int monthOrdinal) {
    return String.format(
        "%04d-%02d",
        EPOCH_YEAR + Math.floorDiv(monthOrdinal, 12), 1 + Math.floorMod(monthOrdinal, 12));
  }

  static String humanDay(int dayOrdinal) {
    OffsetDateTime day = EPOCH.plusDays(dayOrdinal);
    return String.format(
        "%04d-%02d-%02d", day.getYear(), day.getMonth().getValue(), day.getDayOfMonth());
  }

  static String humanTime(Long microsFromMidnight) {
    return LocalTime.ofNanoOfDay(microsFromMidnight * 1000).toString();
  }

  static String humanTimestampWithZone(Long timestampMicros) {
    return DateTimeUtil.microsToIsoTimestamptz(timestampMicros);
  }

  static String humanTimestampWithoutZone(Long timestampMicros) {
    return DateTimeUtil.microsToIsoTimestamp(timestampMicros);
  }

  static String humanTimestampNanoWithZone(Long timestampNanos) {
    return DateTimeUtil.nanosToIsoTimestamptz(timestampNanos);
  }

  static String humanTimestampNanoWithoutZone(Long timestampNanos) {
    return DateTimeUtil.nanosToIsoTimestamp(timestampNanos);
  }

  static String humanHour(int hourOrdinal) {
    OffsetDateTime time = EPOCH.plusHours(hourOrdinal);
    return String.format(
        "%04d-%02d-%02d-%02d",
        time.getYear(), time.getMonth().getValue(), time.getDayOfMonth(), time.getHour());
  }

  static String base64encode(ByteBuffer buffer) {
    // use direct encoding because all of the encoded bytes are in ASCII
    return StandardCharsets.ISO_8859_1.decode(Base64.getEncoder().encode(buffer)).toString();
  }

  static boolean satisfiesOrderOf(ChronoUnit leftGranularity, ChronoUnit rightGranularity) {
    // test the granularity, in hours. hour(ts) => 1 hour, day(ts) => 24 hours, and hour satisfies
    // the order of day
    return leftGranularity.getDuration().toHours() <= rightGranularity.getDuration().toHours();
  }
}
