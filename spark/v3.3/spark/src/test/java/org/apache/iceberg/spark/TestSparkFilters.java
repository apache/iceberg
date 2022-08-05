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
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkFilters {

  @Test
  public void testQuotedAttributes() {
    Map<String, String> attrMap = Maps.newHashMap();
    attrMap.put("id", "id");
    attrMap.put("`i.d`", "i.d");
    attrMap.put("`i``d`", "i`d");
    attrMap.put("`d`.b.`dd```", "d.b.dd`");
    attrMap.put("a.`aa```.c", "a.aa`.c");

    attrMap.forEach(
        (quoted, unquoted) -> {
          IsNull isNull = IsNull.apply(quoted);
          Expression expectedIsNull = Expressions.isNull(unquoted);
          Expression actualIsNull = SparkFilters.convert(isNull);
          Assert.assertEquals(
              "IsNull must match", expectedIsNull.toString(), actualIsNull.toString());

          IsNotNull isNotNull = IsNotNull.apply(quoted);
          Expression expectedIsNotNull = Expressions.notNull(unquoted);
          Expression actualIsNotNull = SparkFilters.convert(isNotNull);
          Assert.assertEquals(
              "IsNotNull must match", expectedIsNotNull.toString(), actualIsNotNull.toString());

          LessThan lt = LessThan.apply(quoted, 1);
          Expression expectedLt = Expressions.lessThan(unquoted, 1);
          Expression actualLt = SparkFilters.convert(lt);
          Assert.assertEquals("LessThan must match", expectedLt.toString(), actualLt.toString());

          LessThanOrEqual ltEq = LessThanOrEqual.apply(quoted, 1);
          Expression expectedLtEq = Expressions.lessThanOrEqual(unquoted, 1);
          Expression actualLtEq = SparkFilters.convert(ltEq);
          Assert.assertEquals(
              "LessThanOrEqual must match", expectedLtEq.toString(), actualLtEq.toString());

          GreaterThan gt = GreaterThan.apply(quoted, 1);
          Expression expectedGt = Expressions.greaterThan(unquoted, 1);
          Expression actualGt = SparkFilters.convert(gt);
          Assert.assertEquals("GreaterThan must match", expectedGt.toString(), actualGt.toString());

          GreaterThanOrEqual gtEq = GreaterThanOrEqual.apply(quoted, 1);
          Expression expectedGtEq = Expressions.greaterThanOrEqual(unquoted, 1);
          Expression actualGtEq = SparkFilters.convert(gtEq);
          Assert.assertEquals(
              "GreaterThanOrEqual must match", expectedGtEq.toString(), actualGtEq.toString());

          EqualTo eq = EqualTo.apply(quoted, 1);
          Expression expectedEq = Expressions.equal(unquoted, 1);
          Expression actualEq = SparkFilters.convert(eq);
          Assert.assertEquals("EqualTo must match", expectedEq.toString(), actualEq.toString());

          EqualNullSafe eqNullSafe = EqualNullSafe.apply(quoted, 1);
          Expression expectedEqNullSafe = Expressions.equal(unquoted, 1);
          Expression actualEqNullSafe = SparkFilters.convert(eqNullSafe);
          Assert.assertEquals(
              "EqualNullSafe must match",
              expectedEqNullSafe.toString(),
              actualEqNullSafe.toString());

          In in = In.apply(quoted, new Integer[] {1});
          Expression expectedIn = Expressions.in(unquoted, 1);
          Expression actualIn = SparkFilters.convert(in);
          Assert.assertEquals("In must match", expectedIn.toString(), actualIn.toString());
        });
  }

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

  @Test
  public void testNestedInInsideNot() {
    Not filter =
        Not.apply(And.apply(EqualTo.apply("col1", 1), In.apply("col2", new Integer[] {1, 2})));
    Expression converted = SparkFilters.convert(filter);
    Assert.assertNull("Expression should not be converted", converted);
  }

  @Test
  public void testNotIn() {
    Not filter = Not.apply(In.apply("col", new Integer[] {1, 2}));
    Expression actual = SparkFilters.convert(filter);
    Expression expected =
        Expressions.and(Expressions.notNull("col"), Expressions.notIn("col", 1, 2));
    Assert.assertEquals("Expressions should match", expected.toString(), actual.toString());
  }
}
