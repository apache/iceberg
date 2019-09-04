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
  public void testDateToHumanString() {
    Types.DateType type = Types.DateType.get();
    Literal<Integer> date = Literal.of("2017-12-01").to(type);

    Transform<Integer, Integer> year = Transforms.year(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017", year.toHumanString(year.apply(date.value())));

    Transform<Integer, Integer> month = Transforms.month(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12", month.toHumanString(month.apply(date.value())));

    Transform<Integer, Integer> day = Transforms.day(type);
    Assert.assertEquals("Should produce the correct Human string",
        "2017-12-01", day.toHumanString(day.apply(date.value())));
  }

  @Test
  public void testNullHumanString() {
    Types.DateType type = Types.DateType.get();
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.year(type).toHumanString(null));
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.month(type).toHumanString(null));
    Assert.assertEquals("Should produce \"null\" for null",
        "null", Transforms.day(type).toHumanString(null));
  }

  @Test
  public void testDatesReturnType() {
    Types.DateType type = Types.DateType.get();

    Transform<Integer, Integer> year = Transforms.year(type);
    Type yearResultType = year.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), yearResultType);

    Transform<Integer, Integer> month = Transforms.month(type);
    Type monthResultType = month.getResultType(type);
    Assert.assertEquals(Types.IntegerType.get(), monthResultType);

    Transform<Integer, Integer> day = Transforms.day(type);
    Type dayResultType = day.getResultType(type);
    Assert.assertEquals(Types.DateType.get(), dayResultType);
  }
}
