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
package org.apache.iceberg.expressions;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionUtil {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "val", Types.IntegerType.get()),
          Types.NestedField.required(3, "val2", Types.IntegerType.get()),
          Types.NestedField.required(4, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(5, "date", Types.DateType.get()),
          Types.NestedField.required(6, "time", Types.DateType.get()),
          Types.NestedField.optional(7, "data", Types.StringType.get()),
          Types.NestedField.optional(8, "measurement", Types.DoubleType.get()));

  private static final Types.StructType STRUCT = SCHEMA.asStruct();

  @Test
  public void testUnchangedUnaryPredicates() {
    for (Expression unary :
        Lists.newArrayList(
            Expressions.isNull("test"),
            Expressions.notNull("test"),
            Expressions.isNaN("test"),
            Expressions.notNaN("test"))) {
      assertEquals(unary, ExpressionUtil.sanitize(unary));
    }
  }

  @Test
  public void testSanitizeIn() {
    assertEquals(
        Expressions.in("test", "(2-digit-int)", "(3-digit-int)"),
        ExpressionUtil.sanitize(Expressions.in("test", 34, 345)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test IN ((2-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.in("test", 34, 345)));
  }

  @Test
  public void testSanitizeLongIn() {
    Object[] tooLongRange =
        IntStream.range(95, 95 + ExpressionUtil.LONG_IN_PREDICATE_ABBREVIATION_THRESHOLD)
            .boxed()
            .toArray();
    Object[] almostTooLongRange = Arrays.copyOf(tooLongRange, tooLongRange.length - 1);

    Assert.assertEquals(
        "Sanitized string should be abbreviated",
        "test IN ((2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (3-digit-int), (3-digit-int), (3-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.in("test", almostTooLongRange)));

    Assert.assertEquals(
        "Sanitized string should be abbreviated",
        "test IN ((2-digit-int), (3-digit-int), ... (8 values hidden, 10 in total))",
        ExpressionUtil.toSanitizedString(Expressions.in("test", tooLongRange)));

    // The sanitization resulting in an expression tree does not abbreviate
    List<String> expectedValues = Lists.newArrayList();
    expectedValues.addAll(Collections.nCopies(5, "(2-digit-int)"));
    expectedValues.addAll(Collections.nCopies(5, "(3-digit-int)"));
    assertEquals(
        Expressions.in("test", expectedValues),
        ExpressionUtil.sanitize(Expressions.in("test", tooLongRange)));
  }

  @Test
  public void zeroAndNegativeNumberHandling() {
    Assertions.assertThat(
            ExpressionUtil.toSanitizedString(
                Expressions.in(
                    "test",
                    0,
                    -1,
                    -100,
                    Integer.MIN_VALUE,
                    Integer.MAX_VALUE,
                    -1234567891234.4d,
                    Float.MAX_VALUE,
                    Double.MAX_VALUE)))
        .isEqualTo(
            "test IN ((1-digit-int), (1-digit-int), (3-digit-int), (10-digit-int), (10-digit-int), (13-digit-float), (39-digit-float), (309-digit-float))");
  }

  @Test
  public void testSanitizeNotIn() {
    assertEquals(
        Expressions.notIn("test", "(2-digit-int)", "(3-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notIn("test", 34, 345)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test NOT IN ((2-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.notIn("test", 34, 345)));
  }

  @Test
  public void testSanitizeLongNotIn() {
    Object[] tooLongRange =
        IntStream.range(95, 95 + ExpressionUtil.LONG_IN_PREDICATE_ABBREVIATION_THRESHOLD)
            .boxed()
            .toArray();
    Object[] almostTooLongRange = Arrays.copyOf(tooLongRange, tooLongRange.length - 1);

    Assert.assertEquals(
        "Sanitized string should be abbreviated",
        "test NOT IN ((2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (3-digit-int), (3-digit-int), (3-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.notIn("test", almostTooLongRange)));

    Assert.assertEquals(
        "Sanitized string should be abbreviated",
        "test NOT IN ((2-digit-int), (3-digit-int), ... (8 values hidden, 10 in total))",
        ExpressionUtil.toSanitizedString(Expressions.notIn("test", tooLongRange)));

    // The sanitization resulting in an expression tree does not abbreviate
    List<String> expectedValues = Lists.newArrayList();
    expectedValues.addAll(Collections.nCopies(5, "(2-digit-int)"));
    expectedValues.addAll(Collections.nCopies(5, "(3-digit-int)"));
    assertEquals(
        Expressions.notIn("test", expectedValues),
        ExpressionUtil.sanitize(Expressions.notIn("test", tooLongRange)));
  }

  @Test
  public void testSanitizeLessThan() {
    assertEquals(
        Expressions.lessThan("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.lessThan("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test < (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.lessThan("test", 34)));
  }

  @Test
  public void testSanitizeLessThanOrEqual() {
    assertEquals(
        Expressions.lessThanOrEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.lessThanOrEqual("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test <= (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.lessThanOrEqual("test", 34)));
  }

  @Test
  public void testSanitizeGreaterThan() {
    assertEquals(
        Expressions.greaterThan("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.greaterThan("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test > (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.greaterThan("test", 34)));
  }

  @Test
  public void testSanitizeGreaterThanOrEqual() {
    assertEquals(
        Expressions.greaterThanOrEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.greaterThanOrEqual("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test >= (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.greaterThanOrEqual("test", 34)));
  }

  @Test
  public void testSanitizeEqual() {
    assertEquals(
        Expressions.equal("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34)));
  }

  @Test
  public void testSanitizeNotEqual() {
    assertEquals(
        Expressions.notEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notEqual("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test != (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.notEqual("test", 34)));
  }

  @Test
  public void testSanitizeStartsWith() {
    assertEquals(
        Expressions.startsWith("test", "(hash-34d05fb7)"),
        ExpressionUtil.sanitize(Expressions.startsWith("test", "aaa")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test STARTS WITH (hash-34d05fb7)",
        ExpressionUtil.toSanitizedString(Expressions.startsWith("test", "aaa")));
  }

  @Test
  public void testSanitizeNotStartsWith() {
    assertEquals(
        Expressions.notStartsWith("test", "(hash-34d05fb7)"),
        ExpressionUtil.sanitize(Expressions.notStartsWith("test", "aaa")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test NOT STARTS WITH (hash-34d05fb7)",
        ExpressionUtil.toSanitizedString(Expressions.notStartsWith("test", "aaa")));
  }

  @Test
  public void testSanitizeTransformedTerm() {
    assertEquals(
        Expressions.equal(Expressions.truncate("test", 2), "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal(Expressions.truncate("test", 2), 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "truncate[2](test) = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal(Expressions.truncate("test", 2), 34)));
  }

  @Test
  public void testSanitizeLong() {
    assertEquals(
        Expressions.equal("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34L)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34L)));
  }

  @Test
  public void testSanitizeFloat() {
    assertEquals(
        Expressions.equal("test", "(2-digit-float)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34.12F)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-float)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34.12F)));
  }

  @Test
  public void testSanitizeDouble() {
    assertEquals(
        Expressions.equal("test", "(2-digit-float)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34.12D)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-float)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34.12D)));
  }

  @Test
  public void testSanitizeDate() {
    assertEquals(
        Expressions.equal("test", "(date)"),
        ExpressionUtil.sanitize(Expressions.equal("test", "2022-04-29")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (date)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", "2022-04-29")));
  }

  @Test
  public void testSanitizeTime() {
    long micros = DateTimeUtil.microsFromTimestamptz(OffsetDateTime.now()) / 1000000;
    String currentTime = DateTimeUtil.microsToIsoTime(micros);

    assertEquals(
        Expressions.equal("test", "(time)"),
        ExpressionUtil.sanitize(Expressions.equal("test", currentTime)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (time)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", currentTime)));
  }

  @Test
  public void testSanitizeTimestamp() {
    for (String timestamp :
        Lists.newArrayList(
            "2022-04-29T23:49:51",
            "2022-04-29T23:49:51.123456",
            "2022-04-29T23:49:51-07:00",
            "2022-04-29T23:49:51.123456+01:00")) {
      assertEquals(
          Expressions.equal("test", "(timestamp)"),
          ExpressionUtil.sanitize(Expressions.equal("test", timestamp)));

      Assert.assertEquals(
          "Sanitized string should be identical except for descriptive literal",
          "test = (timestamp)",
          ExpressionUtil.toSanitizedString(Expressions.equal("test", timestamp)));
    }
  }

  @Test
  public void testSanitizeTimestampAboutNow() {
    // this string is the current UTC time, without a zone offset
    String nowLocal =
        OffsetDateTime.now().atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime().toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-about-now)"),
        ExpressionUtil.sanitize(Expressions.equal("test", nowLocal)));

    assertEquals(
        Expressions.equal("test", "(timestamp-about-now)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(nowLocal).to(Types.TimestampType.withoutZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-about-now)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", nowLocal)));
  }

  @Test
  public void testSanitizeTimestampPast() {
    String ninetyMinutesAgoLocal =
        OffsetDateTime.now()
            .minusMinutes(90)
            .atZoneSameInstant(ZoneOffset.UTC)
            .toLocalDateTime()
            .toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-ago)"),
        ExpressionUtil.sanitize(Expressions.equal("test", ninetyMinutesAgoLocal)));

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-ago)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesAgoLocal).to(Types.TimestampType.withoutZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-1-hours-ago)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", ninetyMinutesAgoLocal)));
  }

  @Test
  public void testSanitizeTimestampLastWeek() {
    String lastWeekLocal =
        OffsetDateTime.now()
            .minusHours(180)
            .atZoneSameInstant(ZoneOffset.UTC)
            .toLocalDateTime()
            .toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-7-days-ago)"),
        ExpressionUtil.sanitize(Expressions.equal("test", lastWeekLocal)));

    assertEquals(
        Expressions.equal("test", "(timestamp-7-days-ago)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(lastWeekLocal).to(Types.TimestampType.withoutZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-7-days-ago)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", lastWeekLocal)));
  }

  @Test
  public void testSanitizeTimestampFuture() {
    String ninetyMinutesFromNowLocal =
        OffsetDateTime.now()
            .plusMinutes(90)
            .atZoneSameInstant(ZoneOffset.UTC)
            .toLocalDateTime()
            .toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-from-now)"),
        ExpressionUtil.sanitize(Expressions.equal("test", ninetyMinutesFromNowLocal)));

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-from-now)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesFromNowLocal).to(Types.TimestampType.withoutZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-1-hours-from-now)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", ninetyMinutesFromNowLocal)));
  }

  @Test
  public void testSanitizeTimestamptzAboutNow() {
    // this string is the current time with the local zone offset
    String nowUtc = OffsetDateTime.now().toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-about-now)"),
        ExpressionUtil.sanitize(Expressions.equal("test", nowUtc)));

    assertEquals(
        Expressions.equal("test", "(timestamp-about-now)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(nowUtc).to(Types.TimestampType.withZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-about-now)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", nowUtc)));
  }

  @Test
  public void testSanitizeTimestamptzPast() {
    String ninetyMinutesAgoUtc = OffsetDateTime.now().minusMinutes(90).toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-ago)"),
        ExpressionUtil.sanitize(Expressions.equal("test", ninetyMinutesAgoUtc)));

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-ago)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesAgoUtc).to(Types.TimestampType.withZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-1-hours-ago)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", ninetyMinutesAgoUtc)));
  }

  @Test
  public void testSanitizeTimestamptzLastWeek() {
    String lastWeekUtc = OffsetDateTime.now().minusHours(180).toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-7-days-ago)"),
        ExpressionUtil.sanitize(Expressions.equal("test", lastWeekUtc)));

    assertEquals(
        Expressions.equal("test", "(timestamp-7-days-ago)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(lastWeekUtc).to(Types.TimestampType.withZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-7-days-ago)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", lastWeekUtc)));
  }

  @Test
  public void testSanitizeTimestamptzFuture() {
    String ninetyMinutesFromNowUtc = OffsetDateTime.now().plusMinutes(90).toString();

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-from-now)"),
        ExpressionUtil.sanitize(Expressions.equal("test", ninetyMinutesFromNowUtc)));

    assertEquals(
        Expressions.equal("test", "(timestamp-1-hours-from-now)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesFromNowUtc).to(Types.TimestampType.withZone()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (timestamp-1-hours-from-now)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", ninetyMinutesFromNowUtc)));
  }

  @Test
  public void testSanitizeDateToday() {
    String today = LocalDate.now(ZoneOffset.UTC).toString();

    assertEquals(
        Expressions.equal("test", "(date-today)"),
        ExpressionUtil.sanitize(Expressions.equal("test", today)));

    assertEquals(
        Expressions.equal("test", "(date-today)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ, "test", Literal.of(today).to(Types.DateType.get()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (date-today)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", today)));
  }

  @Test
  public void testSanitizeDateLastWeek() {
    String lastWeek = LocalDate.now(ZoneOffset.UTC).minusWeeks(1).toString();

    assertEquals(
        Expressions.equal("test", "(date-7-days-ago)"),
        ExpressionUtil.sanitize(Expressions.equal("test", lastWeek)));

    assertEquals(
        Expressions.equal("test", "(date-7-days-ago)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ, "test", Literal.of(lastWeek).to(Types.DateType.get()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (date-7-days-ago)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", lastWeek)));
  }

  @Test
  public void testSanitizeDateNextWeek() {
    String nextWeek = LocalDate.now(ZoneOffset.UTC).plusWeeks(1).toString();

    assertEquals(
        Expressions.equal("test", "(date-7-days-from-now)"),
        ExpressionUtil.sanitize(Expressions.equal("test", nextWeek)));

    assertEquals(
        Expressions.equal("test", "(date-7-days-from-now)"),
        ExpressionUtil.sanitize(
            Expressions.predicate(
                Expression.Operation.EQ, "test", Literal.of(nextWeek).to(Types.DateType.get()))));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (date-7-days-from-now)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", nextWeek)));
  }

  @Test
  public void testSanitizeStringFallback() {
    Pattern filterPattern = Pattern.compile("^test = \\(hash-[0-9a-fA-F]{8}\\)$");
    for (String filter :
        Lists.newArrayList(
            "2022-20-29",
            "2022-04-29T40:49:51.123456",
            "2022-04-29T23:70:51-07:00",
            "2022-04-29T23:49:51.123456+100:00")) {
      String sanitizedFilter = ExpressionUtil.toSanitizedString(Expressions.equal("test", filter));
      Assertions.assertThat(filterPattern.matcher(sanitizedFilter)).matches();
    }
  }

  @Test
  public void testIdenticalExpressionIsEquivalent() {
    Expression[] exprs =
        new Expression[] {
          Expressions.isNull("data"),
          Expressions.notNull("data"),
          Expressions.isNaN("measurement"),
          Expressions.notNaN("measurement"),
          Expressions.lessThan("id", 5),
          Expressions.lessThanOrEqual("id", 5),
          Expressions.greaterThan("id", 5),
          Expressions.greaterThanOrEqual("id", 5),
          Expressions.equal("id", 5),
          Expressions.notEqual("id", 5),
          Expressions.in("id", 5, 6),
          Expressions.notIn("id", 5, 6),
          Expressions.startsWith("data", "aaa"),
          Expressions.notStartsWith("data", "aaa"),
          Expressions.alwaysTrue(),
          Expressions.alwaysFalse(),
          Expressions.and(Expressions.lessThan("id", 5), Expressions.notNull("data")),
          Expressions.or(Expressions.lessThan("id", 5), Expressions.notNull("data")),
        };

    for (Expression expr : exprs) {
      Assert.assertTrue(
          "Should accept identical expression: " + expr,
          ExpressionUtil.equivalent(expr, expr, STRUCT, true));

      for (Expression other : exprs) {
        if (expr != other) {
          Assert.assertFalse(ExpressionUtil.equivalent(expr, other, STRUCT, true));
        }
      }
    }
  }

  @Test
  public void testIdenticalTermIsEquivalent() {
    UnboundTerm<?>[] terms =
        new UnboundTerm<?>[] {
          Expressions.ref("id"),
          Expressions.truncate("id", 2),
          Expressions.bucket("id", 16),
          Expressions.year("ts"),
          Expressions.month("ts"),
          Expressions.day("ts"),
          Expressions.hour("ts"),
        };

    for (UnboundTerm<?> term : terms) {
      BoundTerm<?> bound = term.bind(STRUCT, true);
      Assert.assertTrue("Should accept identical expression: " + term, bound.isEquivalentTo(bound));

      for (UnboundTerm<?> other : terms) {
        if (term != other) {
          Assert.assertFalse(bound.isEquivalentTo(other.bind(STRUCT, true)));
        }
      }
    }
  }

  @Test
  public void testRefEquivalence() {
    Assert.assertFalse(
        "Should not find different refs equivalent",
        Expressions.ref("val")
            .bind(STRUCT, true)
            .isEquivalentTo(Expressions.ref("val2").bind(STRUCT, true)));
  }

  @Test
  public void testInEquivalence() {
    Assert.assertTrue(
        "Should ignore duplicate longs (in)",
        ExpressionUtil.equivalent(
            Expressions.in("id", 1, 2, 1), Expressions.in("id", 2, 1, 2), STRUCT, true));
    Assert.assertTrue(
        "Should ignore duplicate longs (notIn)",
        ExpressionUtil.equivalent(
            Expressions.notIn("id", 1, 2, 1), Expressions.notIn("id", 2, 1, 2), STRUCT, true));

    Assert.assertTrue(
        "Should ignore duplicate strings (in)",
        ExpressionUtil.equivalent(
            Expressions.in("data", "a", "b", "a"), Expressions.in("data", "b", "a"), STRUCT, true));
    Assert.assertTrue(
        "Should ignore duplicate strings (notIn)",
        ExpressionUtil.equivalent(
            Expressions.notIn("data", "b", "b"), Expressions.notIn("data", "b"), STRUCT, true));

    Assert.assertTrue(
        "Should detect equivalence with equal (in, string)",
        ExpressionUtil.equivalent(
            Expressions.in("data", "a"), Expressions.equal("data", "a"), STRUCT, true));
    Assert.assertTrue(
        "Should detect equivalence with notEqual (notIn, long)",
        ExpressionUtil.equivalent(
            Expressions.notIn("id", 1), Expressions.notEqual("id", 1), STRUCT, true));

    Assert.assertFalse(
        "Should detect different sets (in, long)",
        ExpressionUtil.equivalent(
            Expressions.in("id", 1, 2, 3), Expressions.in("id", 1, 2), STRUCT, true));
    Assert.assertFalse(
        "Should detect different sets (notIn, string)",
        ExpressionUtil.equivalent(
            Expressions.notIn("data", "a", "b"), Expressions.notIn("data", "a"), STRUCT, true));
  }

  @Test
  public void testInequalityEquivalence() {
    String[] cols = new String[] {"id", "val", "ts", "date", "time"};

    for (String col : cols) {
      Assert.assertTrue(
          "Should detect < to <= equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.lessThan(col, 34L), Expressions.lessThanOrEqual(col, 33L), STRUCT, true));
      Assert.assertTrue(
          "Should detect <= to < equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.lessThanOrEqual(col, 34L), Expressions.lessThan(col, 35L), STRUCT, true));
      Assert.assertTrue(
          "Should detect > to >= equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.greaterThan(col, 34L),
              Expressions.greaterThanOrEqual(col, 35L),
              STRUCT,
              true));
      Assert.assertTrue(
          "Should detect >= to > equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.greaterThanOrEqual(col, 34L),
              Expressions.greaterThan(col, 33L),
              STRUCT,
              true));
    }

    Assert.assertFalse(
        "Should not detect equivalence for different columns",
        ExpressionUtil.equivalent(
            Expressions.lessThan("val", 34L),
            Expressions.lessThanOrEqual("val2", 33L),
            STRUCT,
            true));
    Assert.assertFalse(
        "Should not detect equivalence for different types",
        ExpressionUtil.equivalent(
            Expressions.lessThan("val", 34L),
            Expressions.lessThanOrEqual("id", 33L),
            STRUCT,
            true));
  }

  @Test
  public void testAndEquivalence() {
    Assert.assertTrue(
        "Should detect and equivalence in any order",
        ExpressionUtil.equivalent(
            Expressions.and(
                Expressions.lessThan("id", 34), Expressions.greaterThanOrEqual("id", 20)),
            Expressions.and(
                Expressions.greaterThan("id", 19L), Expressions.lessThanOrEqual("id", 33L)),
            STRUCT,
            true));
  }

  @Test
  public void testOrEquivalence() {
    Assert.assertTrue(
        "Should detect or equivalence in any order",
        ExpressionUtil.equivalent(
            Expressions.or(
                Expressions.lessThan("id", 20), Expressions.greaterThanOrEqual("id", 34)),
            Expressions.or(
                Expressions.greaterThan("id", 33L), Expressions.lessThanOrEqual("id", 19L)),
            STRUCT,
            true));
  }

  @Test
  public void testNotEquivalence() {
    Assert.assertTrue(
        "Should detect not equivalence by rewriting",
        ExpressionUtil.equivalent(
            Expressions.not(
                Expressions.or(
                    Expressions.in("data", "a"), Expressions.greaterThanOrEqual("id", 34))),
            Expressions.and(Expressions.lessThan("id", 34L), Expressions.notEqual("data", "a")),
            STRUCT,
            true));
  }

  @Test
  public void testSelectsPartitions() {
    Assert.assertTrue(
        "Should select partitions, on boundary",
        ExpressionUtil.selectsPartitions(
            Expressions.lessThan("ts", "2021-03-09T10:00:00.000000"),
            PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
            true));

    Assert.assertFalse(
        "Should not select partitions, 1 ms off boundary",
        ExpressionUtil.selectsPartitions(
            Expressions.lessThanOrEqual("ts", "2021-03-09T10:00:00.000000"),
            PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
            true));

    Assert.assertFalse(
        "Should not select partitions, on hour not day boundary",
        ExpressionUtil.selectsPartitions(
            Expressions.lessThan("ts", "2021-03-09T10:00:00.000000"),
            PartitionSpec.builderFor(SCHEMA).day("ts").build(),
            true));
  }

  private void assertEquals(Expression expected, Expression actual) {
    Assertions.assertThat(expected).isInstanceOf(UnboundPredicate.class);
    assertEquals((UnboundPredicate<?>) expected, (UnboundPredicate<?>) actual);
  }

  private void assertEquals(UnboundPredicate<?> expected, UnboundPredicate<?> actual) {
    Assert.assertEquals("Operation should match", expected.op(), actual.op());
    assertEquals(expected.term(), actual.term());
    Assert.assertEquals("Literals should match", expected.literals(), actual.literals());
  }

  private void assertEquals(UnboundTerm<?> expected, UnboundTerm<?> actual) {
    Assertions.assertThat(expected)
        .as("Unknown expected term: " + expected)
        .isOfAnyClassIn(NamedReference.class, UnboundTransform.class);

    if (expected instanceof NamedReference) {
      Assert.assertTrue("Should be a NamedReference", actual instanceof NamedReference);
      assertEquals((NamedReference<?>) expected, (NamedReference<?>) actual);
    } else if (expected instanceof UnboundTransform) {
      Assert.assertTrue("Should be an UnboundTransform", actual instanceof UnboundTransform);
      assertEquals((UnboundTransform<?, ?>) expected, (UnboundTransform<?, ?>) actual);
    }
  }

  private void assertEquals(NamedReference<?> expected, NamedReference<?> actual) {
    Assert.assertEquals("Should reference the same field name", expected.name(), actual.name());
  }

  private void assertEquals(UnboundTransform<?, ?> expected, UnboundTransform<?, ?> actual) {
    Assert.assertEquals(
        "Should apply the same transform",
        expected.transform().toString(),
        actual.transform().toString());
    assertEquals(expected.ref(), actual.ref());
  }
}
