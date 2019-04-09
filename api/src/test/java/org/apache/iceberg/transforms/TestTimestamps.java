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
}
