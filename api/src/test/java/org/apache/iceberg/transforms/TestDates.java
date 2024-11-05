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

import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestDates {
  @Test
  public void testSatisfiesOrderOfDates() {
    assertThat(Dates.DAY.satisfiesOrderOf(Dates.DAY)).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Dates.MONTH)).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Dates.YEAR)).isTrue();

    assertThat(Dates.MONTH.satisfiesOrderOf(Dates.DAY)).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Dates.MONTH)).isTrue();
    assertThat(Dates.MONTH.satisfiesOrderOf(Dates.YEAR)).isTrue();

    assertThat(Dates.YEAR.satisfiesOrderOf(Dates.DAY)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Dates.MONTH)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Dates.YEAR)).isTrue();
  }

  @Test
  public void testSatisfiesOrderOfTimestamps() {
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isFalse();
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();

    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isTrue();
    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();

    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();
  }

  @Test
  public void testSatisfiesOrderOfTimestampNanos() {
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isFalse();
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();

    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isTrue();
    assertThat(Dates.MONTH.satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();

    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();
  }

  @Test
  public void testSatisfiesOrderOfTimeTransforms() {
    assertThat(Dates.DAY.satisfiesOrderOf(Hours.get())).isFalse();
    assertThat(Dates.DAY.satisfiesOrderOf(Days.get())).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Months.get())).isTrue();
    assertThat(Dates.DAY.satisfiesOrderOf(Years.get())).isTrue();

    assertThat(Dates.MONTH.satisfiesOrderOf(Hours.get())).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Days.get())).isFalse();
    assertThat(Dates.MONTH.satisfiesOrderOf(Months.get())).isTrue();
    assertThat(Dates.MONTH.satisfiesOrderOf(Years.get())).isTrue();

    assertThat(Dates.YEAR.satisfiesOrderOf(Hours.get())).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Days.get())).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Months.get())).isFalse();
    assertThat(Dates.YEAR.satisfiesOrderOf(Years.get())).isTrue();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedDateTransform() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);
    Literal<Integer> pd = Literal.of("1970-01-01").to(type);
    Literal<Integer> nd = Literal.of("1969-12-31").to(type);

    Transform<Integer, Integer> years = Transforms.year(type);
    assertThat((int) years.apply(date.value())).as("Should produce 2017 - 1970 = 47").isEqualTo(47);
    assertThat((int) years.apply(pd.value())).as("Should produce 1970 - 1970 = 0").isZero();
    assertThat((int) years.apply(nd.value())).as("Should produce 1969 - 1970 = -1").isEqualTo(-1);

    Transform<Integer, Integer> months = Transforms.month(type);
    assertThat((int) months.apply(date.value()))
        .as("Should produce 47 * 12 + 11 = 575")
        .isEqualTo(575);
    assertThat((int) months.apply(pd.value())).as("Should produce 0 * 12 + 0 = 0").isZero();
    assertThat((int) months.apply(nd.value())).isEqualTo(-1);

    Transform<Integer, Integer> days = Transforms.day(type);
    assertThat((int) days.apply(date.value())).isEqualTo(17501);
    assertThat((int) days.apply(pd.value())).as("Should produce 0 * 365 + 0 = 0").isZero();
    assertThat((int) days.apply(nd.value())).isEqualTo(-1);
  }

  @Test
  public void testDateTransform() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);
    Literal<Integer> pd = Literal.of("1970-01-01").to(type);
    Literal<Integer> nd = Literal.of("1969-12-31").to(type);

    Transform<Integer, Integer> years = Transforms.year();
    assertThat((int) years.bind(type).apply(date.value()))
        .as("Should produce 2017 - 1970 = 47")
        .isEqualTo(47);
    assertThat((int) years.bind(type).apply(pd.value()))
        .as("Should produce 1970 - 1970 = 0")
        .isZero();
    assertThat((int) years.bind(type).apply(nd.value()))
        .as("Should produce 1969 - 1970 = -1")
        .isEqualTo(-1);

    Transform<Integer, Integer> months = Transforms.month();
    assertThat((int) months.bind(type).apply(date.value()))
        .as("Should produce 47 * 12 + 11 = 575")
        .isEqualTo(575);
    assertThat((int) months.bind(type).apply(pd.value()))
        .as("Should produce 0 * 12 + 0 = 0")
        .isZero();
    assertThat((int) months.bind(type).apply(nd.value())).isEqualTo(-1);

    Transform<Integer, Integer> days = Transforms.day();
    assertThat((int) days.bind(type).apply(date.value())).isEqualTo(17501);

    assertThat((int) days.bind(type).apply(pd.value()))
        .as("Should produce 0 * 365 + 0 = 0")
        .isZero();
    assertThat((int) days.bind(type).apply(nd.value())).isEqualTo(-1);
  }

  @Test
  public void testDateToHumanString() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("2017");

    Transform<Integer, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("2017-12");

    Transform<Integer, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("2017-12-01");
  }

  @Test
  public void testNegativeDateToHumanString() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1969-12-30").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Integer, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Integer, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-30");
  }

  @Test
  public void testDateToHumanStringLowerBound() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1970-01-01").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1970");

    Transform<Integer, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1970-01");

    Transform<Integer, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1970-01-01");
  }

  @Test
  public void testNegativeDateToHumanStringLowerBound() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1969-01-01").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Integer, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-01");

    Transform<Integer, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-01-01");
  }

  @Test
  public void testNegativeDateToHumanStringUpperBound() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1969-12-31").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    assertThat(year.toHumanString(type, year.bind(type).apply(date.value()))).isEqualTo("1969");

    Transform<Integer, Integer> month = Transforms.month();
    assertThat(month.toHumanString(type, month.bind(type).apply(date.value())))
        .isEqualTo("1969-12");

    Transform<Integer, Integer> day = Transforms.day();
    assertThat(day.toHumanString(type, day.bind(type).apply(date.value()))).isEqualTo("1969-12-31");
  }

  @Test
  public void testNullHumanString() {
    Types.DateType type = Types.DateType.get();
    assertThat(Transforms.year().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.month().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
    assertThat(Transforms.day().toHumanString(type, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
  }

  @Test
  public void testDatesReturnType() {
    Types.DateType type = Types.DateType.get();

    Transform<Integer, Integer> year = Transforms.year();
    Type yearResultType = year.getResultType(type);
    assertThat(yearResultType).isEqualTo(Types.IntegerType.get());

    Transform<Integer, Integer> month = Transforms.month();
    Type monthResultType = month.getResultType(type);
    assertThat(monthResultType).isEqualTo(Types.IntegerType.get());

    Transform<Integer, Integer> day = Transforms.day();
    Type dayResultType = day.getResultType(type);
    assertThat(dayResultType).isEqualTo(Types.DateType.get());
  }
}
