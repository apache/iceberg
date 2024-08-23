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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public class DateTimeUtil {
  private DateTimeUtil() {}

  public static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  public static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();
  public static final long MICROS_PER_MILLIS = 1000L;
  public static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long NANOS_PER_MICRO = 1_000L;

  private static final DateTimeFormatter FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .appendOffset("+HH:MM:ss", "+00:00")
          .toFormatter();

  public static LocalDate dateFromDays(int daysFromEpoch) {
    return ChronoUnit.DAYS.addTo(EPOCH_DAY, daysFromEpoch);
  }

  public static int daysFromDate(LocalDate date) {
    return (int) ChronoUnit.DAYS.between(EPOCH_DAY, date);
  }

  public static int daysFromInstant(Instant instant) {
    return (int) ChronoUnit.DAYS.between(EPOCH, instant.atOffset(ZoneOffset.UTC));
  }

  public static LocalTime timeFromMicros(long microFromMidnight) {
    return LocalTime.ofNanoOfDay(microFromMidnight * 1000);
  }

  public static long microsFromTime(LocalTime time) {
    return time.toNanoOfDay() / 1000;
  }

  public static LocalDateTime timestampFromMicros(long microsFromEpoch) {
    return ChronoUnit.MICROS.addTo(EPOCH, microsFromEpoch).toLocalDateTime();
  }

  public static LocalDateTime timestampFromNanos(long nanosFromEpoch) {
    return ChronoUnit.NANOS.addTo(EPOCH, nanosFromEpoch).toLocalDateTime();
  }

  public static long microsFromInstant(Instant instant) {
    return ChronoUnit.MICROS.between(EPOCH, instant.atOffset(ZoneOffset.UTC));
  }

  public static long microsFromTimestamp(LocalDateTime dateTime) {
    return ChronoUnit.MICROS.between(EPOCH, dateTime.atOffset(ZoneOffset.UTC));
  }

  public static long nanosFromTimestamp(LocalDateTime dateTime) {
    return ChronoUnit.NANOS.between(EPOCH, dateTime.atOffset(ZoneOffset.UTC));
  }

  public static long microsToMillis(long micros) {
    // When the timestamp is negative, i.e before 1970, we need to adjust the milliseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    return Math.floorDiv(micros, MICROS_PER_MILLIS);
  }

  public static long nanosToMicros(long nanos) {
    return Math.floorDiv(nanos, NANOS_PER_MICRO);
  }

  public static long microsToNanos(long micros) {
    return Math.multiplyExact(micros, NANOS_PER_MICRO);
  }

  public static OffsetDateTime timestamptzFromMicros(long microsFromEpoch) {
    return ChronoUnit.MICROS.addTo(EPOCH, microsFromEpoch);
  }

  public static long microsFromTimestamptz(OffsetDateTime dateTime) {
    return ChronoUnit.MICROS.between(EPOCH, dateTime);
  }

  public static long nanosFromTimestamptz(OffsetDateTime dateTime) {
    return ChronoUnit.NANOS.between(EPOCH, dateTime);
  }

  public static String formatTimestampMillis(long millis) {
    return Instant.ofEpochMilli(millis).toString().replace("Z", "+00:00");
  }

  public static String daysToIsoDate(int days) {
    return dateFromDays(days).format(DateTimeFormatter.ISO_LOCAL_DATE);
  }

  public static String microsToIsoTime(long micros) {
    return timeFromMicros(micros).format(DateTimeFormatter.ISO_LOCAL_TIME);
  }

  public static String microsToIsoTimestamptz(long micros) {
    LocalDateTime localDateTime = timestampFromMicros(micros);
    return localDateTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
  }

  public static String nanosToIsoTimestamptz(long nanos) {
    LocalDateTime localDateTime = timestampFromNanos(nanos);
    return localDateTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
  }

  public static String microsToIsoTimestamp(long micros) {
    LocalDateTime localDateTime = timestampFromMicros(micros);
    return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }

  public static String nanosToIsoTimestamp(long nanos) {
    LocalDateTime localDateTime = timestampFromNanos(nanos);
    return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }

  public static int isoDateToDays(String dateString) {
    return daysFromDate(LocalDate.parse(dateString, DateTimeFormatter.ISO_LOCAL_DATE));
  }

  public static long isoTimeToMicros(String timeString) {
    return microsFromTime(LocalTime.parse(timeString, DateTimeFormatter.ISO_LOCAL_TIME));
  }

  public static long isoTimestamptzToMicros(String timestampString) {
    return microsFromTimestamptz(
        OffsetDateTime.parse(timestampString, DateTimeFormatter.ISO_DATE_TIME));
  }

  public static long isoTimestamptzToNanos(CharSequence timestampString) {
    return nanosFromTimestamptz(
        OffsetDateTime.parse(timestampString, DateTimeFormatter.ISO_DATE_TIME));
  }

  public static boolean isUTCTimestamptz(String timestampString) {
    OffsetDateTime offsetDateTime =
        OffsetDateTime.parse(timestampString, DateTimeFormatter.ISO_DATE_TIME);
    return offsetDateTime.getOffset().equals(ZoneOffset.UTC);
  }

  public static long isoTimestampToMicros(String timestampString) {
    return microsFromTimestamp(
        LocalDateTime.parse(timestampString, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

  public static long isoTimestampToNanos(CharSequence timestampString) {
    return nanosFromTimestamp(
        LocalDateTime.parse(timestampString, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

  public static int daysToYears(int days) {
    return convertDays(days, ChronoUnit.YEARS);
  }

  public static int daysToMonths(int days) {
    return convertDays(days, ChronoUnit.MONTHS);
  }

  private static int convertDays(int days, ChronoUnit granularity) {
    if (days >= 0) {
      LocalDate date = EPOCH_DAY.plusDays(days);
      return (int) granularity.between(EPOCH_DAY, date);
    } else {
      // add 1 day to the value to account for the case where there is exactly 1 unit between the
      // date and epoch because the result will always be decremented.
      LocalDate date = EPOCH_DAY.plusDays(days + 1);
      return (int) granularity.between(EPOCH_DAY, date) - 1;
    }
  }

  public static int microsToYears(long micros) {
    return convertMicros(micros, ChronoUnit.YEARS);
  }

  public static int microsToMonths(long micros) {
    return convertMicros(micros, ChronoUnit.MONTHS);
  }

  public static int microsToDays(long micros) {
    return convertMicros(micros, ChronoUnit.DAYS);
  }

  public static int microsToHours(long micros) {
    return convertMicros(micros, ChronoUnit.HOURS);
  }

  private static int convertMicros(long micros, ChronoUnit granularity) {
    if (micros >= 0) {
      long epochSecond = Math.floorDiv(micros, MICROS_PER_SECOND);
      long nanoAdjustment = Math.floorMod(micros, MICROS_PER_SECOND) * 1000;
      return (int) granularity.between(EPOCH, toOffsetDateTime(epochSecond, nanoAdjustment));
    } else {
      // add 1 micro to the value to account for the case where there is exactly 1 unit between
      // the timestamp and epoch because the result will always be decremented.
      long epochSecond = Math.floorDiv(micros, MICROS_PER_SECOND);
      long nanoAdjustment = Math.floorMod(micros + 1, MICROS_PER_SECOND) * 1000;
      return (int) granularity.between(EPOCH, toOffsetDateTime(epochSecond, nanoAdjustment)) - 1;
    }
  }

  public static int nanosToYears(long nanos) {
    return Math.toIntExact(convertNanos(nanos, ChronoUnit.YEARS));
  }

  public static int nanosToMonths(long nanos) {
    return Math.toIntExact(convertNanos(nanos, ChronoUnit.MONTHS));
  }

  public static int nanosToDays(long nanos) {
    return Math.toIntExact(convertNanos(nanos, ChronoUnit.DAYS));
  }

  public static int nanosToHours(long nanos) {
    return Math.toIntExact(convertNanos(nanos, ChronoUnit.HOURS));
  }

  private static long convertNanos(long nanos, ChronoUnit granularity) {
    if (nanos >= 0) {
      long epochSecond = Math.floorDiv(nanos, NANOS_PER_SECOND);
      long nanoAdjustment = Math.floorMod(nanos, NANOS_PER_SECOND);
      return granularity.between(EPOCH, toOffsetDateTime(epochSecond, nanoAdjustment));
    } else {
      // add 1 nano to the value to account for the case where there is exactly 1 unit between
      // the timestamp and epoch because the result will always be decremented.
      long epochSecond = Math.floorDiv(nanos, NANOS_PER_SECOND);
      long nanoAdjustment = Math.floorMod(nanos + 1, NANOS_PER_SECOND);
      return granularity.between(EPOCH, toOffsetDateTime(epochSecond, nanoAdjustment)) - 1;
    }
  }

  private static OffsetDateTime toOffsetDateTime(long epochSecond, long nanoAdjustment) {
    return Instant.ofEpochSecond(epochSecond, nanoAdjustment).atOffset(ZoneOffset.UTC);
  }
}
