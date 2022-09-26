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
package org.apache.iceberg.spark;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.spark.sql.sources.GreaterThan;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkFilters {

  @Test
  public void testTimestampFilterConversion() {
    Instant instant = Instant.parse("2018-10-18T00:00:57.907Z");
    Timestamp timestamp = Timestamp.from(instant);
    long epochMicros = ChronoUnit.MICROS.between(Instant.EPOCH, instant);

    Expression instantExpression = SparkFilters.convert(GreaterThan.apply("x", instant));
    Expression timestampExpression = SparkFilters.convert(GreaterThan.apply("x", timestamp));
    Expression rawExpression = Expressions.greaterThan("x", epochMicros);

    Assert.assertEquals(
        "Generated Timestamp expression should be correct",
        rawExpression.toString(),
        timestampExpression.toString());
    Assert.assertEquals(
        "Generated Instant expression should be correct",
        rawExpression.toString(),
        instantExpression.toString());
  }

  @Test
  public void testDateFilterConversion() {
    LocalDate localDate = LocalDate.parse("2018-10-18");
    Date date = Date.valueOf(localDate);
    long epochDay = localDate.toEpochDay();

    Expression localDateExpression = SparkFilters.convert(GreaterThan.apply("x", localDate));
    Expression dateExpression = SparkFilters.convert(GreaterThan.apply("x", date));
    Expression rawExpression = Expressions.greaterThan("x", epochDay);

    Assert.assertEquals(
        "Generated localdate expression should be correct",
        rawExpression.toString(),
        localDateExpression.toString());

    Assert.assertEquals(
        "Generated date expression should be correct",
        rawExpression.toString(),
        dateExpression.toString());
  }
}
