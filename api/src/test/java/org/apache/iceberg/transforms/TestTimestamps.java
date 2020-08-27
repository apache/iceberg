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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestTimestamps {
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
    Types.TimestampType type = Types.TimestampType.withZone();
    Literal<Long> date = Literal.of("2017-12-01T10:12:55.038194-08:00").to(type);

    Transform<Long, Integer> year = Transforms.year(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017", year.toHumanString(year.apply(date.value())));

    Transform<Long, Integer> month = Transforms.month(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12", month.toHumanString(month.apply(date.value())));

    Transform<Long, Integer> day = Transforms.day(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12-01", day.toHumanString(day.apply(date.value())));

    // the hour is 18 because the value is always UTC
    Transform<Long, Integer> hour = Transforms.hour(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12-01-18", hour.toHumanString(hour.apply(date.value())));
  }

  @Test
  public void testTimestampWithZoneAndZoneOffsetToHumanString() {
    Types.TimestampType type = Types.TimestampType.withZone();
    Literal<Long> date = Literal.of("2017-01-01T00:12:55.038194+00:00").to(type);

    String[] timeZoneArray = {
        "-01:00", "-02:00", "-03:00", "-04:00", "-05:00", "-06:00", "-07:00",
        "-08:00", "-09:00", "-10:00", "-11:00", "+00:00", "+01:00", "+02:00",
        "+03:00", "+04:00", "+05:00", "+06:00", "+07:00", "+08:00", "+09:00",
        "+10:00", "+11:00"};

    Arrays.stream(timeZoneArray).forEach(offsetId -> {
      Transform<Long, Integer> year = Transforms.year(type, offsetId);
      Assert.assertEquals("Should produce the correct Human string",
          getExpectedStringWithOffsetId(date.value(), offsetId, "yyyy"),
          year.toHumanString(year.apply(date.value())));

      Transform<Long, Integer> month = Transforms.month(type, offsetId);
      Assert.assertEquals("Should produce the correct Human string",
          getExpectedStringWithOffsetId(date.value(), offsetId, "yyyy-MM"),
          month.toHumanString(month.apply(date.value())));

      Transform<Long, Integer> day = Transforms.day(type, offsetId);
      Assert.assertEquals("Should produce the correct Human string",
          getExpectedStringWithOffsetId(date.value(), offsetId, "yyyy-MM-dd"),
          day.toHumanString(day.apply(date.value())));

      Transform<Long, Integer> hour = Transforms.hour(type, offsetId);
      Assert.assertEquals("Should produce the correct Human string",
          getExpectedStringWithOffsetId(date.value(), offsetId, "yyyy-MM-dd-HH"),
          hour.toHumanString(hour.apply(date.value())));
    });
  }

  private String getExpectedStringWithOffsetId(long millis, String offsetId, String pattern) {
    SimpleDateFormat utcFormater = new SimpleDateFormat(pattern);
    utcFormater.setTimeZone(TimeZone.getTimeZone("UTC"));
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.setTimeInMillis(millis / 1000);
    calendar.add(Calendar.HOUR_OF_DAY, Integer.valueOf(offsetId.split("\\:")[0]));
    return utcFormater.format(calendar.getTime());
  }

  @Test
  public void testIllegalOffsetId() {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "ts", Types.TimestampType.withZone())
    );

    try {
      PartitionSpec.builderFor(schema).hour("ts", "hour", "Z").build();
      PartitionSpec.builderFor(schema).day("ts", "day", "Z").build();
      PartitionSpec.builderFor(schema).month("ts", "month", "Z").build();
      PartitionSpec.builderFor(schema).year("ts", "year", "Z").build();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Expect offsetId is null or +HH:mm or -HH:mm, but is: Z",
          e.getMessage());
    }
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
