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
package org.apache.iceberg.functions;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * Collapsed truncate function for the temporal masks.
 *
 * <p>Six instances cover the cross product of {YEAR, MONTH} x {DATE days, TIMESTAMP micros,
 * TIMESTAMP_NANO nanos}. Kept package-private so it can change without affecting the public Action
 * surface.
 */
final class TruncateTemporal extends Actions.NullSafeFunction<Object, Object> {

  enum Unit {
    YEAR,
    MONTH
  }

  private enum Storage {
    DATE_DAYS,
    TIMESTAMP_MICROS,
    TIMESTAMP_NANOS
  }

  static final TruncateTemporal DATE_YEAR = new TruncateTemporal(Unit.YEAR, Storage.DATE_DAYS);
  static final TruncateTemporal DATE_MONTH = new TruncateTemporal(Unit.MONTH, Storage.DATE_DAYS);
  static final TruncateTemporal TIMESTAMP_YEAR =
      new TruncateTemporal(Unit.YEAR, Storage.TIMESTAMP_MICROS);
  static final TruncateTemporal TIMESTAMP_MONTH =
      new TruncateTemporal(Unit.MONTH, Storage.TIMESTAMP_MICROS);
  static final TruncateTemporal TIMESTAMP_NANO_YEAR =
      new TruncateTemporal(Unit.YEAR, Storage.TIMESTAMP_NANOS);
  static final TruncateTemporal TIMESTAMP_NANO_MONTH =
      new TruncateTemporal(Unit.MONTH, Storage.TIMESTAMP_NANOS);

  static TruncateTemporal forType(Unit unit, Type type) {
    switch (type.typeId()) {
      case DATE:
        return unit == Unit.YEAR ? DATE_YEAR : DATE_MONTH;
      case TIMESTAMP:
        return unit == Unit.YEAR ? TIMESTAMP_YEAR : TIMESTAMP_MONTH;
      case TIMESTAMP_NANO:
        return unit == Unit.YEAR ? TIMESTAMP_NANO_YEAR : TIMESTAMP_NANO_MONTH;
      default:
        throw new IllegalArgumentException("Unsupported type for truncate: " + type);
    }
  }

  private final Unit unit;
  private final Storage storage;

  private TruncateTemporal(Unit unit, Storage storage) {
    this.unit = unit;
    this.storage = storage;
  }

  @Override
  protected Object applyNonNull(Object value) {
    switch (storage) {
      case DATE_DAYS:
        LocalDate date = DateTimeUtil.dateFromDays((Integer) value);
        return DateTimeUtil.daysFromDate(truncateDate(date));
      case TIMESTAMP_MICROS:
        LocalDateTime tsMicros = DateTimeUtil.timestampFromMicros((Long) value);
        return DateTimeUtil.microsFromTimestamp(truncateTimestamp(tsMicros));
      case TIMESTAMP_NANOS:
        LocalDateTime tsNanos = DateTimeUtil.timestampFromNanos((Long) value);
        return DateTimeUtil.nanosFromTimestamp(truncateTimestamp(tsNanos));
      default:
        throw new IllegalStateException("unreachable: " + storage);
    }
  }

  private LocalDate truncateDate(LocalDate date) {
    return unit == Unit.YEAR ? date.withMonth(1).withDayOfMonth(1) : date.withDayOfMonth(1);
  }

  private LocalDateTime truncateTimestamp(LocalDateTime ts) {
    LocalDateTime truncated =
        unit == Unit.YEAR ? ts.withMonth(1).withDayOfMonth(1) : ts.withDayOfMonth(1);
    return truncated.with(LocalTime.MIDNIGHT);
  }
}
