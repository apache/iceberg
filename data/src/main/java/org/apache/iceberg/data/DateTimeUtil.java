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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class DateTimeUtil {
  private DateTimeUtil() {
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static LocalDate dateFromDays(int daysFromEpoch) {
    return ChronoUnit.DAYS.addTo(EPOCH_DAY, daysFromEpoch);
  }

  public static int daysFromDate(LocalDate date) {
    return (int) ChronoUnit.DAYS.between(EPOCH_DAY, date);
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

  public static long microsFromTimestamp(LocalDateTime dateTime) {
    return ChronoUnit.MICROS.between(EPOCH, dateTime.atOffset(ZoneOffset.UTC));
  }

  public static OffsetDateTime timestamptzFromMicros(long microsFromEpoch) {
    return ChronoUnit.MICROS.addTo(EPOCH, microsFromEpoch);
  }

  public static long microsFromTimestamptz(OffsetDateTime dateTime) {
    return ChronoUnit.MICROS.between(EPOCH, dateTime);
  }
}
