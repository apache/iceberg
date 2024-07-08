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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.temporal.ChronoUnit;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTimestamps {
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedTimestampTransform() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194").to(type);
    Literal<Long> pts = Literal.of("1970-01-01T00:00:01.000001").to(type);
    Literal<Long> nts = Literal.of("1969-12-31T23:59:58.999999").to(type);

    Transform<Long, Integer> years = Transforms.year(type);
    assertThat((int) years.apply(ts.value())).as("Should produce 2017 - 1970 = 47").isEqualTo(47);
    assertThat((int) years.apply(pts.value())).as("Should produce 1970 - 1970 = 0").isZero();
    assertThat((int) years.apply(nts.value())).as("Should produce 1969 - 1970 = -1").isEqualTo(-1);

    Transform<Long, Integer> months = Transforms.month(type);
    assertThat((int) months.apply(ts.value()))
        .as("Should produce 47 * 12 + 11 = 575")
        .isEqualTo(575);
    assertThat((int) months.apply(pts.value())).as("Should produce 0 * 12 + 0 = 0").isZero();
    assertThat((int) months.apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> days = Transforms.day(type);
    assertThat((int) days.apply(ts.value())).as("Should produce 17501").isEqualTo(17501);
    assertThat((int) days.apply(pts.value())).as("Should produce 0 * 365 + 0 = 0").isZero();
    assertThat((int) days.apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> hours = Transforms.hour(type);
    assertThat((int) hours.apply(ts.value()))
        .as("Should produce 17501 * 24 + 10")
        .isEqualTo(420034);
    assertThat((int) hours.apply(pts.value())).as("Should produce 0 * 24 + 0 = 0").isZero();
    assertThat((int) hours.apply(nts.value())).isEqualTo(-1);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedTimestampNanoTransform() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194789").to(type);
    Literal<Long> pts = Literal.of("1970-01-01T00:00:01.000000001").to(type);
    Literal<Long> nts = Literal.of("1969-12-31T23:59:58.999999999").to(type);

    Transform<Long, Integer> years = Transforms.year(type);
    assertThat((int) years.apply(ts.value())).as("Should produce 2017 - 1970 = 47").isEqualTo(47);
    assertThat((int) years.apply(pts.value())).as("Should produce 1970 - 1970 = 0").isZero();
    assertThat((int) years.apply(nts.value())).as("Should produce 1969 - 1970 = -1").isEqualTo(-1);

    Transform<Long, Integer> months = Transforms.month(type);
    assertThat((int) months.apply(ts.value()))
        .as("Should produce 47 * 12 + 11 = 575")
        .isEqualTo(575);
    assertThat((int) months.apply(pts.value())).as("Should produce 0 * 12 + 0 = 0").isZero();
    assertThat((int) months.apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> days = Transforms.day(type);
    assertThat((int) days.apply(ts.value())).as("Should produce 17501").isEqualTo(17501);
    assertThat((int) days.apply(pts.value())).as("Should produce 0 * 365 + 0 = 0").isZero();
    assertThat((int) days.apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> hours = Transforms.hour(type);
    assertThat((int) hours.apply(ts.value()))
        .as("Should produce 17501 * 24 + 10")
        .isEqualTo(420034);
    assertThat((int) hours.apply(pts.value())).as("Should produce 0 * 24 + 0 = 0").isZero();
    assertThat((int) hours.apply(nts.value())).isEqualTo(-1);
  }

  @Test
  public void testTimestampTransform() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194").to(type);
    Literal<Long> pts = Literal.of("1970-01-01T00:00:01.000001").to(type);
    Literal<Long> nts = Literal.of("1969-12-31T23:59:58.999999").to(type);

    Transform<Long, Integer> years = Transforms.year();
    assertThat((int) years.bind(type).apply(ts.value()))
        .as("Should produce 2017 - 1970 = 47")
        .isEqualTo(47);
    assertThat((int) years.bind(type).apply(pts.value()))
        .as("Should produce 1970 - 1970 = 0")
        .isZero();
    assertThat((int) years.bind(type).apply(nts.value()))
        .as("Should produce 1969 - 1970 = -1")
        .isEqualTo(-1);

    Transform<Long, Integer> months = Transforms.month();
    assertThat((int) months.bind(type).apply(ts.value()))
        .as("Should produce 47 * 12 + 11 = 575")
        .isEqualTo(575);
    assertThat((int) months.bind(type).apply(pts.value()))
        .as("Should produce 0 * 12 + 0 = 0")
        .isZero();
    assertThat((int) months.bind(type).apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> days = Transforms.day();
    assertThat((int) days.bind(type).apply(ts.value())).as("Should produce 17501").isEqualTo(17501);
    assertThat((int) days.bind(type).apply(pts.value()))
        .as("Should produce 0 * 365 + 0 = 0")
        .isZero();
    assertThat((int) days.bind(type).apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> hours = Transforms.hour();
    assertThat((int) hours.bind(type).apply(ts.value()))
        .as("Should produce 17501 * 24 + 10")
        .isEqualTo(420034);
    assertThat((int) hours.bind(type).apply(pts.value()))
        .as("Should produce 0 * 24 + 0 = 0")
        .isZero();
    assertThat((int) hours.bind(type).apply(nts.value())).isEqualTo(-1);
  }

  @Test
  public void testTimestampNanoTransform() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194789").to(type);
    Literal<Long> pts = Literal.of("1970-01-01T00:00:01.000000001").to(type);
    Literal<Long> nts = Literal.of("1969-12-31T23:59:58.999999999").to(type);

    Transform<Long, Integer> years = Transforms.year();
    assertThat((int) years.bind(type).apply(ts.value()))
        .as("Should produce 2017 - 1970 = 47")
        .isEqualTo(47);
    assertThat((int) years.bind(type).apply(pts.value()))
        .as("Should produce 1970 - 1970 = 0")
        .isZero();
    assertThat((int) years.bind(type).apply(nts.value()))
        .as("Should produce 1969 - 1970 = -1")
        .isEqualTo(-1);

    Transform<Long, Integer> months = Transforms.month();
    assertThat((int) months.bind(type).apply(ts.value()))
        .as("Should produce 47 * 12 + 11 = 575")
        .isEqualTo(575);
    assertThat((int) months.bind(type).apply(pts.value()))
        .as("Should produce 0 * 12 + 0 = 0")
        .isZero();
    assertThat((int) months.bind(type).apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> days = Transforms.day();
    assertThat((int) days.bind(type).apply(ts.value())).as("Should produce 17501").isEqualTo(17501);
    assertThat((int) days.bind(type).apply(pts.value()))
        .as("Should produce 0 * 365 + 0 = 0")
        .isZero();
    assertThat((int) days.bind(type).apply(nts.value())).isEqualTo(-1);

    Transform<Long, Integer> hours = Transforms.hour();
    assertThat((int) hours.bind(type).apply(ts.value()))
        .as("Should produce 17501 * 24 + 10")
        .isEqualTo(420034);
    assertThat((int) hours.bind(type).apply(pts.value()))
        .as("Should produce 0 * 24 + 0 = 0")
        .isZero();
    assertThat((int) hours.bind(type).apply(nts.value())).isEqualTo(-1);
  }

  @Test
  public void testTimestampWithoutZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("2017");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("2017-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("2017-12-01");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("2017-12-01-10");
  }

  @Test
  public void testTimestampNanoWithoutZoneToHumanString() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194789").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("2017");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("2017-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("2017-12-01");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("2017-12-01-10");
  }

  @Test
  public void testNegativeTimestampWithoutZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-30T10:12:55.038194").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-30");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("1969-12-30-10");
  }

  @Test
  public void testNegativeTimestampNanoWithoutZoneToHumanString() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-30T10:12:55.038194789").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-30");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("1969-12-30-10");
  }

  @Test
  public void testNegativeTimestampWithoutZoneToHumanStringLowerBound() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-30T00:00:00.000000").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-30");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("1969-12-30-00");
  }

  @Test
  public void testNegativeTimestampNanoWithoutZoneToHumanStringLowerBound() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-30T00:00:00.000000000").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-30");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("1969-12-30-00");
  }

  @Test
  public void testNegativeTimestampWithoutZoneToHumanStringUpperBound() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-31T23:59:59.999999").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-31");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("1969-12-31-23");
  }

  @Test
  public void testNegativeTimestampNanoWithoutZoneToHumanStringUpperBound() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-31T23:59:59.999999999").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-31");

    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("1969-12-31-23");
  }

  @Test
  public void testTimestampWithZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194-08:00").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("2017");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("2017-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("2017-12-01");

    // the hour is 18 because the value is always UTC
    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("2017-12-01-18");
  }

  @Test
  public void testTimestampNanoWithZoneToHumanString() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194789-08:00").to(type);

    Transform<Long, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("2017");

    Transform<Long, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("2017-12");

    Transform<Long, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("2017-12-01");

    // the hour is 18 because the value is always UTC
    Transform<Long, Integer> hour = Transforms.hour();
    assertThat(hour.toHumanString(type, hour.bind(type).apply(date.value())))
        .isEqualTo("2017-12-01-18");
  }

  @Test
  public void testTimestampNullHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    assertThat(Transforms.year().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.month().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.day().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.hour().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
  }

  @Test
  public void testTimestampNanoNullHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    assertThat(Transforms.year().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.month().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.day().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.hour().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
  }

  @Test
  public void testTimestampsReturnType() {
    Types.TimestampType type = Types.TimestampType.withZone();

    Transform<Integer, Integer> year = Transforms.year();
    Type yearResultType = year.getResultType(type);
    assertThat(yearResultType).isEqualTo(Types.IntegerType.get());

    Transform<Integer, Integer> month = Transforms.month();
    Type monthResultType = month.getResultType(type);
    assertThat(monthResultType).isEqualTo(Types.IntegerType.get());

    Transform<Integer, Integer> day = Transforms.day();
    Type dayResultType = day.getResultType(type);
    assertThat(dayResultType).isEqualTo(Types.DateType.get());

    Transform<Integer, Integer> hour = Transforms.hour();
    Type hourResultType = hour.getResultType(type);
    assertThat(hourResultType).isEqualTo(Types.IntegerType.get());
  }

  @Test
  public void testTimestampNanosReturnType() {
    Types.TimestampNanoType type = Types.TimestampNanoType.withZone();

    Transform<Integer, Integer> year = Transforms.year();
    Type yearResultType = year.getResultType(type);
    assertThat(yearResultType).isEqualTo(Types.IntegerType.get());

    Transform<Integer, Integer> month = Transforms.month();
    Type monthResultType = month.getResultType(type);
    assertThat(monthResultType).isEqualTo(Types.IntegerType.get());

    Transform<Integer, Integer> day = Transforms.day();
    Type dayResultType = day.getResultType(type);
    assertThat(dayResultType).isEqualTo(Types.DateType.get());

    Transform<Integer, Integer> hour = Transforms.hour();
    Type hourResultType = hour.getResultType(type);
    assertThat(hourResultType).isEqualTo(Types.IntegerType.get());
  }

  @Test
  public void testApplyRejectsBadSourceType() {
    Timestamps badSourceType =
        new Timestamps(ChronoUnit.CENTURIES, Timestamps.ResultTypeUnit.YEARS, "year");
    assertThatThrownBy(() -> badSourceType.apply(11L))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageMatching("Unsupported source type unit: Centuries");
  }

  @Test
  public void testApplyRejectsBadResultType() {
    Timestamps badResultType =
        new Timestamps(ChronoUnit.MICROS, Timestamps.ResultTypeUnit.NANOS, "nano");
    assertThatThrownBy(() -> badResultType.apply(11L))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageMatching("Unsupported result type unit: NANOS");
  }

  @Test
  public void testGetOfTimestampTypeRejectsBadChronoUnit() {
    Types.TimestampType timestampType = Types.TimestampType.withZone();
    assertThatThrownBy(() -> Timestamps.get(timestampType, ChronoUnit.CENTURIES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Unsupported source/result type units: timestamptz -> Centuries");
  }

  @Test
  public void testGetOfTimestampTypeRejectsBadString() {
    Types.TimestampType timestampType = Types.TimestampType.withZone();
    assertThatThrownBy(() -> Timestamps.get(timestampType, "trash"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Unsupported source/result type units: timestamptz -> trash");
  }

  @Test
  public void testGetOfTimestampNanoTypeRejectsBadChronoUnit() {
    Types.TimestampNanoType timestampNanoType = Types.TimestampNanoType.withZone();
    assertThatThrownBy(() -> Timestamps.get(timestampNanoType, ChronoUnit.CENTURIES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Unsupported source/result type units: timestamptz_ns -> Centuries");
  }

  @Test
  public void testGetOfTimestampNanoTypeRejectsBadString() {
    Types.TimestampNanoType timestampNanoType = Types.TimestampNanoType.withZone();
    assertThatThrownBy(() -> Timestamps.get(timestampNanoType, "trash"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Unsupported source/result type units: timestamptz_ns -> trash");
  }
}
