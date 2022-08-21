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

import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestTimestamps {
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedTimestampTransform() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194").to(type);
    Literal<Long> pts = Literal.of("1970-01-01T00:00:01.000001").to(type);
    Literal<Long> nts = Literal.of("1969-12-31T23:59:58.999999").to(type);

    Transform<Long, Integer> years = Transforms.year(type);
    Assert.assertEquals("Should produce 2017 - 1970 = 47", 47, (int) years.apply(ts.value()));
    Assert.assertEquals("Should produce 1970 - 1970 = 0", 0, (int) years.apply(pts.value()));
    Assert.assertEquals("Should produce 1969 - 1970 = -1", -1, (int) years.apply(nts.value()));

    Transform<Long, Integer> months = Transforms.month(type);
    Assert.assertEquals("Should produce 47 * 12 + 11 = 575", 575, (int) months.apply(ts.value()));
    Assert.assertEquals("Should produce 0 * 12 + 0 = 0", 0, (int) months.apply(pts.value()));
    Assert.assertEquals("Should produce -1", -1, (int) months.apply(nts.value()));

    Transform<Long, Integer> days = Transforms.day(type);
    Assert.assertEquals("Should produce 17501", 17501, (int) days.apply(ts.value()));
    Assert.assertEquals("Should produce 0 * 365 + 0 = 0", 0, (int) days.apply(pts.value()));
    Assert.assertEquals("Should produce -1", -1, (int) days.apply(nts.value()));

    Transform<Long, Integer> hours = Transforms.hour(type);
    Assert.assertEquals("Should produce 17501 * 24 + 10", 420034, (int) hours.apply(ts.value()));
    Assert.assertEquals("Should produce 0 * 24 + 0 = 0", 0, (int) hours.apply(pts.value()));
    Assert.assertEquals("Should produce -1", -1, (int) hours.apply(nts.value()));
  }

  @Test
  public void testTimestampTransform() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194").to(type);
    Literal<Long> pts = Literal.of("1970-01-01T00:00:01.000001").to(type);
    Literal<Long> nts = Literal.of("1969-12-31T23:59:58.999999").to(type);

    Transform<Long, Integer> years = Transforms.year();
    Assert.assertEquals(
        "Should produce 2017 - 1970 = 47", 47, (int) years.bind(type).apply(ts.value()));
    Assert.assertEquals(
        "Should produce 1970 - 1970 = 0", 0, (int) years.bind(type).apply(pts.value()));
    Assert.assertEquals(
        "Should produce 1969 - 1970 = -1", -1, (int) years.bind(type).apply(nts.value()));

    Transform<Long, Integer> months = Transforms.month();
    Assert.assertEquals(
        "Should produce 47 * 12 + 11 = 575", 575, (int) months.bind(type).apply(ts.value()));
    Assert.assertEquals(
        "Should produce 0 * 12 + 0 = 0", 0, (int) months.bind(type).apply(pts.value()));
    Assert.assertEquals("Should produce -1", -1, (int) months.bind(type).apply(nts.value()));

    Transform<Long, Integer> days = Transforms.day();
    Assert.assertEquals("Should produce 17501", 17501, (int) days.bind(type).apply(ts.value()));
    Assert.assertEquals(
        "Should produce 0 * 365 + 0 = 0", 0, (int) days.bind(type).apply(pts.value()));
    Assert.assertEquals("Should produce -1", -1, (int) days.bind(type).apply(nts.value()));

    Transform<Long, Integer> hours = Transforms.hour();
    Assert.assertEquals(
        "Should produce 17501 * 24 + 10", 420034, (int) hours.bind(type).apply(ts.value()));
    Assert.assertEquals(
        "Should produce 0 * 24 + 0 = 0", 0, (int) hours.bind(type).apply(pts.value()));
    Assert.assertEquals("Should produce -1", -1, (int) hours.bind(type).apply(nts.value()));
  }

  @Test
  public void testTimestampWithoutZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194").to(type);

    Transform<Long, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Long, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Long, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12-01",
        day.toHumanString(type, day.bind(type).apply(date.value())));

    Transform<Long, Integer> hour = Transforms.hour();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12-01-10",
        hour.toHumanString(type, hour.bind(type).apply(date.value())));
  }

  @Test
  public void testNegativeTimestampWithoutZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-30T10:12:55.038194").to(type);

    Transform<Long, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Long, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Long, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-30",
        day.toHumanString(type, day.bind(type).apply(date.value())));

    Transform<Long, Integer> hour = Transforms.hour();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-30-10",
        hour.toHumanString(type, hour.bind(type).apply(date.value())));
  }

  @Test
  public void testNegativeTimestampWithoutZoneToHumanStringLowerBound() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-30T00:00:00.000000").to(type);

    Transform<Long, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Long, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Long, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-30",
        day.toHumanString(type, day.bind(type).apply(date.value())));

    Transform<Long, Integer> hour = Transforms.hour();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-30-00",
        hour.toHumanString(type, hour.bind(type).apply(date.value())));
  }

  @Test
  public void testNegativeTimestampWithoutZoneToHumanStringUpperBound() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("1969-12-31T23:59:59.999999").to(type);

    Transform<Long, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Long, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Long, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-31",
        day.toHumanString(type, day.bind(type).apply(date.value())));

    Transform<Long, Integer> hour = Transforms.hour();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-31-23",
        hour.toHumanString(type, hour.bind(type).apply(date.value())));
  }

  @Test
  public void testTimestampWithZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194-08:00").to(type);

    Transform<Long, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Long, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Long, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12-01",
        day.toHumanString(type, day.bind(type).apply(date.value())));

    // the hour is 18 because the value is always UTC
    Transform<Long, Integer> hour = Transforms.hour();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12-01-18",
        hour.toHumanString(type, hour.bind(type).apply(date.value())));
  }

  @Test
  public void testNullHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.year().toHumanString(type, null));
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.month().toHumanString(type, null));
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.day().toHumanString(type, null));
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.hour().toHumanString(type, null));
  }

  @Test
  public void testTimestampsReturnType() {
    Types.TimestampType type = Types.TimestampType.withZone();

    Transform<Integer, Integer> year = Transforms.year();
    Type yearResultType = year.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), yearResultType);

    Transform<Integer, Integer> month = Transforms.month();
    Type monthResultType = month.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), monthResultType);

    Transform<Integer, Integer> day = Transforms.day();
    Type dayResultType = day.getResultType(type);
    Assert.assertEquals(Types.DateType.get(), dayResultType);

    Transform<Integer, Integer> hour = Transforms.hour();
    Type hourResultType = hour.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), hourResultType);
  }
}
