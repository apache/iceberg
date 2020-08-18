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

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.TimeZone;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTimestamps {
  private static final TimeZone DEFAUL_TIMEZONE = TimeZone.getDefault();

  @Before
  public void initTimeZone() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @After
  public void resetTimeZone() {
    TimeZone.setDefault(DEFAUL_TIMEZONE);
  }

  @Test
  public void testTimestampWithoutZoneToHumanString() {
    Types.TimestampType type = Types.TimestampType.withoutZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194").to(type);

    Transform<Long, Integer> year = Transforms.year(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017", year.toHumanString(year.apply(date.value())));

    Transform<Long, Integer> month = Transforms.month(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12", month.toHumanString(month.apply(date.value())));

    Transform<Long, Integer> day = Transforms.day(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12-01", day.toHumanString(day.apply(date.value())));

    Transform<Long, Integer> hour = Transforms.hour(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12-01-10", hour.toHumanString(hour.apply(date.value())));
  }

  @Test
  public void testTimestampWithZoneToHumanString() {
    String[] timeZoneArray = {
        "-01:00", "-02:00", "-03:00", "-04:00", "-05:00", "-06:00", "-07:00",
        "-08:00", "-09:00", "-10:00", "-11:00", "+00:00", "+01:00", "+02:00",
        "+03:00", "+04:00", "+05:00", "+06:00", "+07:00", "+08:00", "+09:00",
        "+10:00", "+11:00"};

    Arrays.stream(timeZoneArray).forEach(zoneId -> {
        // Modify timeZone
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(zoneId)));

        Timestamp date = Timestamp.valueOf("2017-12-01 10:12:55.038");
        Types.TimestampType type = Types.TimestampType.withZone();

        String message = "Should produce the correct Human string[" + zoneId + "]";
        Transform<Long, Integer> year = Transforms.year(type);
        Assert.assertEquals(message, "2017", year.toHumanString(year.apply(date.getTime() * 1000)));

        Transform<Long, Integer> month = Transforms.month(type);
        Assert.assertEquals(message, "2017-12", month.toHumanString(month.apply(date.getTime() * 1000)));

        Transform<Long, Integer> day = Transforms.day(type);
        Assert.assertEquals(message, "2017-12-01", day.toHumanString(day.apply(date.getTime() * 1000)));

        Transform<Long, Integer> hour = Transforms.hour(type);
        Assert.assertEquals(message, "2017-12-01-10", hour.toHumanString(hour.apply(date.getTime() * 1000)));
    });
  }

  @Test
  public void testNullHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.year(type).toHumanString(null));
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.month(type).toHumanString(null));
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.day(type).toHumanString(null));
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.hour(type).toHumanString(null));
  }

  @Test
  public void testTimestampsReturnType() {
    Types.TimestampType type = Types.TimestampType.withZone();

    Transform<Integer, Integer> year = Transforms.year(type);
    Type yearResultType = year.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), yearResultType);

    Transform<Integer, Integer> month = Transforms.month(type);
    Type monthResultType = month.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), monthResultType);

    Transform<Integer, Integer> day = Transforms.day(type);
    Type dayResultType = day.getResultType(type);
    Assert.assertEquals(Types.DateType.get(), dayResultType);

    Transform<Integer, Integer> hour = Transforms.hour(type);
    Type hourResultType = hour.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), hourResultType);
  }
}
