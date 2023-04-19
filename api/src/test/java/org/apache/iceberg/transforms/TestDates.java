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

public class TestDates {
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedDateTransform() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);
    Literal<Integer> pd = Literal.of("1970-01-01").to(type);
    Literal<Integer> nd = Literal.of("1969-12-31").to(type);

    Transform<Integer, Integer> years = Transforms.year(type);
    Assert.assertEquals("Should produce 2017 - 1970 = 47", 47, (int) years.apply(date.value()));
    Assert.assertEquals("Should produce 1970 - 1970 = 0", 0, (int) years.apply(pd.value()));
    Assert.assertEquals("Should produce 1969 - 1970 = -1", -1, (int) years.apply(nd.value()));

    Transform<Integer, Integer> months = Transforms.month(type);
    Assert.assertEquals("Should produce 47 * 12 + 11 = 575", 575, (int) months.apply(date.value()));
    Assert.assertEquals("Should produce 0 * 12 + 0 = 0", 0, (int) months.apply(pd.value()));
    Assert.assertEquals("Should produce -1", -1, (int) months.apply(nd.value()));

    Transform<Integer, Integer> days = Transforms.day(type);
    Assert.assertEquals("Should produce 17501", 17501, (int) days.apply(date.value()));
    Assert.assertEquals("Should produce 0 * 365 + 0 = 0", 0, (int) days.apply(pd.value()));
    Assert.assertEquals("Should produce -1", -1, (int) days.apply(nd.value()));
  }

  @Test
  public void testDateTransform() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);
    Literal<Integer> pd = Literal.of("1970-01-01").to(type);
    Literal<Integer> nd = Literal.of("1969-12-31").to(type);

    Transform<Integer, Integer> years = Transforms.year();
    Assert.assertEquals(
        "Should produce 2017 - 1970 = 47", 47, (int) years.bind(type).apply(date.value()));
    Assert.assertEquals(
        "Should produce 1970 - 1970 = 0", 0, (int) years.bind(type).apply(pd.value()));
    Assert.assertEquals(
        "Should produce 1969 - 1970 = -1", -1, (int) years.bind(type).apply(nd.value()));

    Transform<Integer, Integer> months = Transforms.month();
    Assert.assertEquals(
        "Should produce 47 * 12 + 11 = 575", 575, (int) months.bind(type).apply(date.value()));
    Assert.assertEquals(
        "Should produce 0 * 12 + 0 = 0", 0, (int) months.bind(type).apply(pd.value()));
    Assert.assertEquals("Should produce -1", -1, (int) months.bind(type).apply(nd.value()));

    Transform<Integer, Integer> days = Transforms.day();
    Assert.assertEquals("Should produce 17501", 17501, (int) days.bind(type).apply(date.value()));
    Assert.assertEquals(
        "Should produce 0 * 365 + 0 = 0", 0, (int) days.bind(type).apply(pd.value()));
    Assert.assertEquals("Should produce -1", -1, (int) days.bind(type).apply(nd.value()));
  }

  @Test
  public void testDateToHumanString() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Integer, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Integer, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "2017-12-01",
        day.toHumanString(type, day.bind(type).apply(date.value())));
  }

  @Test
  public void testNegativeDateToHumanString() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1969-12-30").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Integer, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Integer, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-30",
        day.toHumanString(type, day.bind(type).apply(date.value())));
  }

  @Test
  public void testDateToHumanStringLowerBound() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1970-01-01").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1970",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Integer, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1970-01",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Integer, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1970-01-01",
        day.toHumanString(type, day.bind(type).apply(date.value())));
  }

  @Test
  public void testNegativeDateToHumanStringLowerBound() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1969-01-01").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Integer, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-01",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Integer, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-01-01",
        day.toHumanString(type, day.bind(type).apply(date.value())));
  }

  @Test
  public void testNegativeDateToHumanStringUpperBound() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("1969-12-31").to(type);

    Transform<Integer, Integer> year = Transforms.year();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969",
        year.toHumanString(type, year.bind(type).apply(date.value())));

    Transform<Integer, Integer> month = Transforms.month();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12",
        month.toHumanString(type, month.bind(type).apply(date.value())));

    Transform<Integer, Integer> day = Transforms.day();
    Assert.assertEquals(
        "Should produce the correct Human string",
        "1969-12-31",
        day.toHumanString(type, day.bind(type).apply(date.value())));
  }

  @Test
  public void testNullHumanString() {
    Types.DateType type = Types.DateType.get();
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.year().toHumanString(type, null));
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.month().toHumanString(type, null));
    Assert.assertEquals(
        "Should produce \"null\" for null", "null", Transforms.day().toHumanString(type, null));
  }

  @Test
  public void testDatesReturnType() {
    Types.DateType type = Types.DateType.get();

    Transform<Integer, Integer> year = Transforms.year();
    Type yearResultType = year.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), yearResultType);

    Transform<Integer, Integer> month = Transforms.month();
    Type monthResultType = month.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), monthResultType);

    Transform<Integer, Integer> day = Transforms.day();
    Type dayResultType = day.getResultType(type);
    Assert.assertEquals(Types.DateType.get(), dayResultType);
  }
}
