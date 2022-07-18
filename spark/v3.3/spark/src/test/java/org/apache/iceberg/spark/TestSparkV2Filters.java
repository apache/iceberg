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

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.TimestampType$;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConverters;

public class TestSparkV2Filters {

  @Test
  public void testQuotedAttributes() {
    Map<String, String> attrMap = Maps.newHashMap();
    attrMap.put("id", "id");
    attrMap.put("`i.d`", "i.d");
    attrMap.put("`i``d`", "i`d");
    attrMap.put("`d`.b.`dd```", "d.b.dd`");
    attrMap.put("a.`aa```.c", "a.aa`.c");

    attrMap.forEach((quoted, unquoted) -> {
      List<String> ref = new ArrayList();
      ref.add(quoted);
      FieldReference quotedAttr = new FieldReference(JavaConverters.asScalaBuffer(ref).toSeq());

      org.apache.spark.sql.connector.expressions.Expression[] attr =
          new org.apache.spark.sql.connector.expressions.Expression[]{quotedAttr};

      LiteralValue value = new LiteralValue(1, IntegerType$.MODULE$);
      org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
          new org.apache.spark.sql.connector.expressions.Expression[]{quotedAttr, value};

      Predicate isNull = new Predicate("IS_NULL", attr);
      Expression expectedIsNull = Expressions.isNull(unquoted);
      Expression actualIsNull = SparkV2Filters.convert(isNull);
      Assert.assertEquals("IsNull must match", expectedIsNull.toString(), actualIsNull.toString());

      Predicate isNotNull = new Predicate("IS_NOT_NULL", attr);
      Expression expectedIsNotNull = Expressions.notNull(unquoted);
      Expression actualIsNotNull = SparkV2Filters.convert(isNotNull);
      Assert.assertEquals("IsNotNull must match", expectedIsNotNull.toString(), actualIsNotNull.toString());

      Predicate lt = new Predicate("<", attrAndValue);
      Expression expectedLt = Expressions.lessThan(unquoted, 1);
      Expression actualLt = SparkV2Filters.convert(lt);
      Assert.assertEquals("LessThan must match", expectedLt.toString(), actualLt.toString());

      Predicate ltEq = new Predicate("<=", attrAndValue);
      Expression expectedLtEq = Expressions.lessThanOrEqual(unquoted, 1);
      Expression actualLtEq = SparkV2Filters.convert(ltEq);
      Assert.assertEquals("LessThanOrEqual must match", expectedLtEq.toString(), actualLtEq.toString());

      Predicate gt = new Predicate(">", attrAndValue);
      Expression expectedGt = Expressions.greaterThan(unquoted, 1);
      Expression actualGt = SparkV2Filters.convert(gt);
      Assert.assertEquals("GreaterThan must match", expectedGt.toString(), actualGt.toString());

      Predicate gtEq = new Predicate(">=", attrAndValue);
      Expression expectedGtEq = Expressions.greaterThanOrEqual(unquoted, 1);
      Expression actualGtEq = SparkV2Filters.convert(gtEq);
      Assert.assertEquals("GreaterThanOrEqual must match", expectedGtEq.toString(), actualGtEq.toString());

      Predicate eq = new Predicate("=", attrAndValue);
      Expression expectedEq = Expressions.equal(unquoted, 1);
      Expression actualEq = SparkV2Filters.convert(eq);
      Assert.assertEquals("EqualTo must match", expectedEq.toString(), actualEq.toString());

      Predicate eqNullSafe = new Predicate("<=>", attrAndValue);
      Expression expectedEqNullSafe = Expressions.equal(unquoted, 1);
      Expression actualEqNullSafe = SparkV2Filters.convert(eqNullSafe);
      Assert.assertEquals("EqualNullSafe must match", expectedEqNullSafe.toString(), actualEqNullSafe.toString());

      Predicate in = new Predicate("IN", attrAndValue);
      Expression expectedIn = Expressions.in(unquoted, 1);
      Expression actualIn = SparkV2Filters.convert(in);
      Assert.assertEquals("In must match", expectedIn.toString(), actualIn.toString());
    });
  }

  @Test
  public void testTimestampFilterConversion() {
    Instant instant = Instant.parse("2018-10-18T00:00:57.907Z");
    long epochMicros = ChronoUnit.MICROS.between(Instant.EPOCH, instant);

    List<String> ref = new ArrayList();
    ref.add("x");
    FieldReference attr = new FieldReference(JavaConverters.asScalaBuffer(ref).toSeq());
    LiteralValue ts = new LiteralValue(epochMicros, TimestampType$.MODULE$);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[]{attr, ts};

    Predicate predicate = new Predicate(">", attrAndValue);
    Expression tsExpression = SparkV2Filters.convert(predicate);
    Expression rawExpression = Expressions.greaterThan("x", epochMicros);

    Assert.assertEquals("Generated Timestamp expression should be correct",
        rawExpression.toString(), tsExpression.toString());
  }

  @Test
  public void testDateFilterConversion() {
    LocalDate localDate = LocalDate.parse("2018-10-18");
    long epochDay = localDate.toEpochDay();

    List<String> ref = new ArrayList();
    ref.add("x");
    FieldReference attr = new FieldReference(JavaConverters.asScalaBuffer(ref).toSeq());
    LiteralValue ts = new LiteralValue(epochDay, DateType$.MODULE$);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[]{attr, ts};

    Predicate predicate = new Predicate(">", attrAndValue);
    Expression dateExpression = SparkV2Filters.convert(predicate);
    Expression rawExpression = Expressions.greaterThan("x", epochDay);

    Assert.assertEquals("Generated date expression should be correct",
        rawExpression.toString(), dateExpression.toString());
  }

  @Test
  public void testNestedInInsideNot() {
    List<String> ref1 = new ArrayList();
    ref1.add("col1");
    FieldReference attr1 = new FieldReference(JavaConverters.asScalaBuffer(ref1).toSeq());
    LiteralValue v1 = new LiteralValue(1, IntegerType$.MODULE$);
    LiteralValue v2 = new LiteralValue(2, IntegerType$.MODULE$);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue1 =
        new org.apache.spark.sql.connector.expressions.Expression[]{attr1, v1};
    Predicate equal = new Predicate("=", attrAndValue1);

    List<String> ref2 = new ArrayList();
    ref2.add("col2");
    FieldReference attr2 = new FieldReference(JavaConverters.asScalaBuffer(ref2).toSeq());
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue2 =
        new org.apache.spark.sql.connector.expressions.Expression[]{attr2, v1, v2};
    Predicate in = new Predicate("IN", attrAndValue2);

    Not filter = new Not(new And(equal, in));
    Expression converted = SparkV2Filters.convert(filter);
    Assert.assertNull("Expression should not be converted", converted);
  }

  @Test
  public void testNotIn() {
    List<String> ref = new ArrayList();
    ref.add("col");
    FieldReference attr = new FieldReference(JavaConverters.asScalaBuffer(ref).toSeq());
    LiteralValue v1 = new LiteralValue(1, IntegerType$.MODULE$);
    LiteralValue v2 = new LiteralValue(2, IntegerType$.MODULE$);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[]{attr, v1, v2};

    Predicate in = new Predicate("IN", attrAndValue);
    Not not = new Not(in);

    Expression actual = SparkV2Filters.convert(not);
    Expression expected = Expressions.and(Expressions.notNull("col"), Expressions.notIn("col", 1, 2));
    Assert.assertEquals("Expressions should match", expected.toString(), actual.toString());
  }
}
