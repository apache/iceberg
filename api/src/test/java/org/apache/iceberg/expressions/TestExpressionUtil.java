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

import static org.apache.iceberg.expressions.ExpressionUtil.equivalent;
import static org.apache.iceberg.expressions.ExpressionUtil.sanitize;
import static org.apache.iceberg.expressions.ExpressionUtil.selectsPartitions;
import static org.apache.iceberg.expressions.ExpressionUtil.toSanitizedString;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.predicate;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.Test;

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
      assertEquals(unary, sanitize(unary));
    }
  }

  @Test
  public void testSanitizeIn() {
    assertEquals(
        Expressions.in("test", "(2-digit-int)", "(3-digit-int)"),
        sanitize(Expressions.in("test", 34, 345)));

    assertThat(toSanitizedString(Expressions.in("test", 34, 345)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test IN ((2-digit-int), (3-digit-int))");
  }

  @Test
  public void testSanitizeLongIn() {
    Object[] tooLongRange =
        IntStream.range(95, 95 + ExpressionUtil.LONG_IN_PREDICATE_ABBREVIATION_THRESHOLD)
            .boxed()
            .toArray();
    Object[] almostTooLongRange = Arrays.copyOf(tooLongRange, tooLongRange.length - 1);

    assertThat(toSanitizedString(Expressions.in("test", almostTooLongRange)))
        .as("Sanitized string should be abbreviated")
        .isEqualTo(
            "test IN ((2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (3-digit-int), (3-digit-int), (3-digit-int), (3-digit-int))");

    assertThat(toSanitizedString(Expressions.in("test", tooLongRange)))
        .as("Sanitized string should be abbreviated")
        .isEqualTo("test IN ((2-digit-int), (3-digit-int), ... (8 values hidden, 10 in total))");

    // The sanitization resulting in an expression tree does not abbreviate
    List<String> expectedValues = Lists.newArrayList();
    expectedValues.addAll(Collections.nCopies(5, "(2-digit-int)"));
    expectedValues.addAll(Collections.nCopies(5, "(3-digit-int)"));
    assertEquals(
        Expressions.in("test", expectedValues), sanitize(Expressions.in("test", tooLongRange)));
  }

  @Test
  public void zeroAndNegativeNumberHandling() {
    assertThat(
            toSanitizedString(
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
        sanitize(Expressions.notIn("test", 34, 345)));

    assertThat(toSanitizedString(Expressions.notIn("test", 34, 345)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test NOT IN ((2-digit-int), (3-digit-int))");
  }

  @Test
  public void testSanitizeLongNotIn() {
    Object[] tooLongRange =
        IntStream.range(95, 95 + ExpressionUtil.LONG_IN_PREDICATE_ABBREVIATION_THRESHOLD)
            .boxed()
            .toArray();
    Object[] almostTooLongRange = Arrays.copyOf(tooLongRange, tooLongRange.length - 1);

    assertThat(toSanitizedString(Expressions.notIn("test", almostTooLongRange)))
        .as("Sanitized string should be abbreviated")
        .isEqualTo(
            "test NOT IN ((2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (2-digit-int), (3-digit-int), (3-digit-int), (3-digit-int), (3-digit-int))");

    assertThat(toSanitizedString(Expressions.notIn("test", tooLongRange)))
        .as("Sanitized string should be abbreviated")
        .isEqualTo(
            "test NOT IN ((2-digit-int), (3-digit-int), ... (8 values hidden, 10 in total))");

    // The sanitization resulting in an expression tree does not abbreviate
    List<String> expectedValues = Lists.newArrayList();
    expectedValues.addAll(Collections.nCopies(5, "(2-digit-int)"));
    expectedValues.addAll(Collections.nCopies(5, "(3-digit-int)"));
    assertEquals(
        Expressions.notIn("test", expectedValues),
        sanitize(Expressions.notIn("test", tooLongRange)));
  }

  @Test
  public void testSanitizeLessThan() {
    assertEquals(lessThan("test", "(2-digit-int)"), sanitize(lessThan("test", 34)));

    assertThat(toSanitizedString(lessThan("test", 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test < (2-digit-int)");
  }

  @Test
  public void testSanitizeLessThanOrEqual() {
    assertEquals(lessThanOrEqual("test", "(2-digit-int)"), sanitize(lessThanOrEqual("test", 34)));

    assertThat(toSanitizedString(lessThanOrEqual("test", 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test <= (2-digit-int)");
  }

  @Test
  public void testSanitizeGreaterThan() {
    assertEquals(greaterThan("test", "(2-digit-int)"), sanitize(greaterThan("test", 34)));

    assertThat(toSanitizedString(greaterThan("test", 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test > (2-digit-int)");
  }

  @Test
  public void testSanitizeGreaterThanOrEqual() {
    assertEquals(
        greaterThanOrEqual("test", "(2-digit-int)"), sanitize(greaterThanOrEqual("test", 34)));

    assertThat(toSanitizedString(greaterThanOrEqual("test", 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test >= (2-digit-int)");
  }

  @Test
  public void testSanitizeEqual() {
    assertEquals(equal("test", "(2-digit-int)"), sanitize(equal("test", 34)));

    assertThat(toSanitizedString(equal("test", 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (2-digit-int)");
  }

  @Test
  public void testSanitizeNotEqual() {
    assertEquals(
        Expressions.notEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notEqual("test", 34)));

    assertThat(toSanitizedString(notEqual("test", 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test != (2-digit-int)");
  }

  @Test
  public void testSanitizeStartsWith() {
    assertEquals(
        Expressions.startsWith("test", "(hash-34d05fb7)"),
        sanitize(Expressions.startsWith("test", "aaa")));

    assertThat(toSanitizedString(Expressions.startsWith("test", "aaa")))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test STARTS WITH (hash-34d05fb7)");
  }

  @Test
  public void testSanitizeNotStartsWith() {
    assertEquals(
        Expressions.notStartsWith("test", "(hash-34d05fb7)"),
        sanitize(Expressions.notStartsWith("test", "aaa")));

    assertThat(toSanitizedString(Expressions.notStartsWith("test", "aaa")))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test NOT STARTS WITH (hash-34d05fb7)");
  }

  @Test
  public void testSanitizeTransformedTerm() {
    assertEquals(
        equal(Expressions.truncate("test", 2), "(2-digit-int)"),
        sanitize(equal(Expressions.truncate("test", 2), 34)));

    assertThat(toSanitizedString(equal(Expressions.truncate("test", 2), 34)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("truncate[2](test) = (2-digit-int)");
  }

  @Test
  public void testSanitizeLong() {
    assertEquals(equal("test", "(2-digit-int)"), sanitize(equal("test", 34L)));

    assertThat(toSanitizedString(equal("test", 34L)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (2-digit-int)");
  }

  @Test
  public void testSanitizeFloat() {
    assertEquals(equal("test", "(2-digit-float)"), sanitize(equal("test", 34.12F)));

    assertThat(toSanitizedString(equal("test", 34.12F)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (2-digit-float)");
  }

  @Test
  public void testSanitizeDouble() {
    assertEquals(equal("test", "(2-digit-float)"), sanitize(equal("test", 34.12D)));

    assertThat(toSanitizedString(equal("test", 34.12D)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (2-digit-float)");
  }

  @Test
  public void testSanitizeDate() {
    assertEquals(equal("test", "(date)"), sanitize(equal("test", "2022-04-29")));

    assertThat(toSanitizedString(equal("test", "2022-04-29")))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (date)");
  }

  @Test
  public void testSanitizeTime() {
    long micros = DateTimeUtil.microsFromTimestamptz(OffsetDateTime.now()) / 1000000;
    String currentTime = DateTimeUtil.microsToIsoTime(micros);

    assertEquals(
        Expressions.equal("test", "(time)"),
        ExpressionUtil.sanitize(Expressions.equal("test", currentTime)));

    assertThat(toSanitizedString(equal("test", currentTime)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (time)");
  }

  @Test
  public void testSanitizeTimestamp() {
    for (String timestamp :
        Lists.newArrayList(
            "2022-04-29T23:49:51",
            "2022-04-29T23:49:51.123456",
            "2022-04-29T23:49:51-07:00",
            "2022-04-29T23:49:51.123456+01:00")) {
      assertEquals(equal("test", "(timestamp)"), sanitize(equal("test", timestamp)));

      assertThat(toSanitizedString(equal("test", timestamp)))
          .as("Sanitized string should be identical except for descriptive literal")
          .isEqualTo("test = (timestamp)");
    }
  }

  @Test
  public void testSanitizeTimestampAboutNow() {
    // this string is the current UTC time, without a zone offset
    String nowLocal =
        OffsetDateTime.now().atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime().toString();

    assertEquals(equal("test", "(timestamp-about-now)"), sanitize(equal("test", nowLocal)));

    assertEquals(
        equal("test", "(timestamp-about-now)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(nowLocal).to(Types.TimestampType.withoutZone()))));

    assertThat(toSanitizedString(equal("test", nowLocal)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-about-now)");
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
        equal("test", "(timestamp-1-hours-ago)"), sanitize(equal("test", ninetyMinutesAgoLocal)));

    assertEquals(
        equal("test", "(timestamp-1-hours-ago)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesAgoLocal).to(Types.TimestampType.withoutZone()))));

    assertThat(toSanitizedString(equal("test", ninetyMinutesAgoLocal)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-1-hours-ago)");
  }

  @Test
  public void testSanitizeTimestampLastWeek() {
    String lastWeekLocal =
        OffsetDateTime.now()
            .minusHours(180)
            .atZoneSameInstant(ZoneOffset.UTC)
            .toLocalDateTime()
            .toString();

    assertEquals(equal("test", "(timestamp-7-days-ago)"), sanitize(equal("test", lastWeekLocal)));

    assertEquals(
        equal("test", "(timestamp-7-days-ago)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(lastWeekLocal).to(Types.TimestampType.withoutZone()))));

    assertThat(toSanitizedString(equal("test", lastWeekLocal)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-7-days-ago)");
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
        equal("test", "(timestamp-1-hours-from-now)"),
        sanitize(equal("test", ninetyMinutesFromNowLocal)));

    assertEquals(
        equal("test", "(timestamp-1-hours-from-now)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesFromNowLocal).to(Types.TimestampType.withoutZone()))));

    assertThat(toSanitizedString(equal("test", ninetyMinutesFromNowLocal)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-1-hours-from-now)");
  }

  @Test
  public void testSanitizeTimestamptzAboutNow() {
    // this string is the current time with the local zone offset
    String nowUtc = OffsetDateTime.now().toString();

    assertEquals(equal("test", "(timestamp-about-now)"), sanitize(equal("test", nowUtc)));

    assertEquals(
        equal("test", "(timestamp-about-now)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(nowUtc).to(Types.TimestampType.withZone()))));

    assertThat(toSanitizedString(equal("test", nowUtc)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-about-now)");
  }

  @Test
  public void testSanitizeTimestamptzPast() {
    String ninetyMinutesAgoUtc = OffsetDateTime.now().minusMinutes(90).toString();

    assertEquals(
        equal("test", "(timestamp-1-hours-ago)"), sanitize(equal("test", ninetyMinutesAgoUtc)));

    assertEquals(
        equal("test", "(timestamp-1-hours-ago)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesAgoUtc).to(Types.TimestampType.withZone()))));

    assertThat(toSanitizedString(equal("test", ninetyMinutesAgoUtc)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-1-hours-ago)");
  }

  @Test
  public void testSanitizeTimestamptzLastWeek() {
    String lastWeekUtc = OffsetDateTime.now().minusHours(180).toString();

    assertEquals(equal("test", "(timestamp-7-days-ago)"), sanitize(equal("test", lastWeekUtc)));

    assertEquals(
        equal("test", "(timestamp-7-days-ago)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(lastWeekUtc).to(Types.TimestampType.withZone()))));

    assertThat(toSanitizedString(equal("test", lastWeekUtc)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-7-days-ago)");
  }

  @Test
  public void testSanitizeTimestamptzFuture() {
    String ninetyMinutesFromNowUtc = OffsetDateTime.now().plusMinutes(90).toString();

    assertEquals(
        equal("test", "(timestamp-1-hours-from-now)"),
        sanitize(equal("test", ninetyMinutesFromNowUtc)));

    assertEquals(
        equal("test", "(timestamp-1-hours-from-now)"),
        sanitize(
            predicate(
                Expression.Operation.EQ,
                "test",
                Literal.of(ninetyMinutesFromNowUtc).to(Types.TimestampType.withZone()))));

    assertThat(toSanitizedString(equal("test", ninetyMinutesFromNowUtc)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (timestamp-1-hours-from-now)");
  }

  @Test
  public void testSanitizeDateToday() {
    String today = LocalDate.now(ZoneOffset.UTC).toString();

    assertEquals(equal("test", "(date-today)"), sanitize(equal("test", today)));

    assertEquals(
        equal("test", "(date-today)"),
        sanitize(
            predicate(
                Expression.Operation.EQ, "test", Literal.of(today).to(Types.DateType.get()))));

    assertThat(toSanitizedString(equal("test", today)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (date-today)");
  }

  @Test
  public void testSanitizeDateLastWeek() {
    String lastWeek = LocalDate.now(ZoneOffset.UTC).minusWeeks(1).toString();

    assertEquals(equal("test", "(date-7-days-ago)"), sanitize(equal("test", lastWeek)));

    assertEquals(
        equal("test", "(date-7-days-ago)"),
        sanitize(
            predicate(
                Expression.Operation.EQ, "test", Literal.of(lastWeek).to(Types.DateType.get()))));

    assertThat(toSanitizedString(equal("test", lastWeek)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (date-7-days-ago)");
  }

  @Test
  public void testSanitizeDateNextWeek() {
    String nextWeek = LocalDate.now(ZoneOffset.UTC).plusWeeks(1).toString();

    assertEquals(equal("test", "(date-7-days-from-now)"), sanitize(equal("test", nextWeek)));

    assertEquals(
        equal("test", "(date-7-days-from-now)"),
        sanitize(
            predicate(
                Expression.Operation.EQ, "test", Literal.of(nextWeek).to(Types.DateType.get()))));

    assertThat(toSanitizedString(equal("test", nextWeek)))
        .as("Sanitized string should be identical except for descriptive literal")
        .isEqualTo("test = (date-7-days-from-now)");
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
      String sanitizedFilter = toSanitizedString(equal("test", filter));
      assertThat(filterPattern.matcher(sanitizedFilter)).matches();
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
          lessThan("id", 5),
          lessThanOrEqual("id", 5),
          greaterThan("id", 5),
          greaterThanOrEqual("id", 5),
          equal("id", 5),
          notEqual("id", 5),
          Expressions.in("id", 5, 6),
          Expressions.notIn("id", 5, 6),
          Expressions.startsWith("data", "aaa"),
          Expressions.notStartsWith("data", "aaa"),
          Expressions.alwaysTrue(),
          Expressions.alwaysFalse(),
          and(lessThan("id", 5), Expressions.notNull("data")),
          or(lessThan("id", 5), Expressions.notNull("data")),
        };

    for (Expression expr : exprs) {
      assertThat(equivalent(expr, expr, STRUCT, true))
          .as("Should accept identical expression: " + expr)
          .isTrue();

      for (Expression other : exprs) {
        if (expr != other) {
          assertThat(equivalent(expr, other, STRUCT, true)).isFalse();
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
      assertThat(bound.isEquivalentTo(bound))
          .as("Should accept identical expression: " + term)
          .isTrue();

      for (UnboundTerm<?> other : terms) {
        if (term != other) {
          assertThat(bound.isEquivalentTo(other.bind(STRUCT, true))).isFalse();
        }
      }
    }
  }

  @Test
  public void testRefEquivalence() {
    assertThat(
            Expressions.ref("val")
                .bind(STRUCT, true)
                .isEquivalentTo(Expressions.ref("val2").bind(STRUCT, true)))
        .as("Should not find different refs equivalent")
        .isFalse();
  }

  @Test
  public void testInEquivalence() {
    assertThat(equivalent(in("id", 1, 2, 1), in("id", 2, 1, 2), STRUCT, true))
        .as("Should ignore duplicate longs (in)")
        .isTrue();
    assertThat(equivalent(notIn("id", 1, 2, 1), notIn("id", 2, 1, 2), STRUCT, true))
        .as("Should ignore duplicate longs (notIn)")
        .isTrue();

    assertThat(equivalent(in("data", "a", "b", "a"), in("data", "b", "a"), STRUCT, true))
        .as("Should ignore duplicate strings (in)")
        .isTrue();
    assertThat(equivalent(notIn("data", "b", "b"), notIn("data", "b"), STRUCT, true))
        .as("Should ignore duplicate strings (notIn)")
        .isTrue();

    assertThat(equivalent(in("data", "a"), equal("data", "a"), STRUCT, true))
        .as("Should detect equivalence with equal (in, string)")
        .isTrue();
    assertThat(equivalent(notIn("id", 1), notEqual("id", 1), STRUCT, true))
        .as("Should detect equivalence with notEqual (notIn, long)")
        .isTrue();

    assertThat(equivalent(in("id", 1, 2, 3), in("id", 1, 2), STRUCT, true))
        .as("Should detect different sets (in, long)")
        .isFalse();
    assertThat(equivalent(notIn("data", "a", "b"), notIn("data", "a"), STRUCT, true))
        .as("Should detect different sets (notIn, string)")
        .isFalse();
  }

  @Test
  public void testInequalityEquivalence() {
    String[] cols = new String[] {"id", "val", "ts", "date", "time"};

    for (String col : cols) {
      assertThat(equivalent(lessThan(col, 34L), lessThanOrEqual(col, 33L), STRUCT, true))
          .as("Should detect < to <= equivalence: " + col)
          .isTrue();
      assertThat(equivalent(lessThanOrEqual(col, 34L), lessThan(col, 35L), STRUCT, true))
          .as("Should detect <= to < equivalence: " + col)
          .isTrue();
      assertThat(equivalent(greaterThan(col, 34L), greaterThanOrEqual(col, 35L), STRUCT, true))
          .as("Should detect > to >= equivalence: " + col)
          .isTrue();
      assertThat(equivalent(greaterThanOrEqual(col, 34L), greaterThan(col, 33L), STRUCT, true))
          .as("Should detect >= to > equivalence: " + col)
          .isTrue();
    }

    assertThat(equivalent(lessThan("val", 34L), lessThanOrEqual("val2", 33L), STRUCT, true))
        .as("Should not detect equivalence for different columns")
        .isFalse();
    assertThat(equivalent(lessThan("val", 34L), lessThanOrEqual("id", 33L), STRUCT, true))
        .as("Should not detect equivalence for different types")
        .isFalse();
  }

  @Test
  public void testAndEquivalence() {
    assertThat(
            equivalent(
                and(lessThan("id", 34), greaterThanOrEqual("id", 20)),
                and(greaterThan("id", 19L), lessThanOrEqual("id", 33L)),
                STRUCT,
                true))
        .as("Should detect and equivalence in any order")
        .isTrue();
  }

  @Test
  public void testOrEquivalence() {
    assertThat(
            equivalent(
                or(lessThan("id", 20), greaterThanOrEqual("id", 34)),
                or(greaterThan("id", 33L), lessThanOrEqual("id", 19L)),
                STRUCT,
                true))
        .as("Should detect or equivalence in any order")
        .isTrue();
  }

  @Test
  public void testNotEquivalence() {
    assertThat(
            equivalent(
                not(or(in("data", "a"), greaterThanOrEqual("id", 34))),
                and(lessThan("id", 34L), notEqual("data", "a")),
                STRUCT,
                true))
        .as("Should detect not equivalence by rewriting")
        .isTrue();
  }

  @Test
  public void testSelectsPartitions() {
    assertThat(
            selectsPartitions(
                lessThan("ts", "2021-03-09T10:00:00.000000"),
                PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
                true))
        .as("Should select partitions, on boundary")
        .isTrue();

    assertThat(
            selectsPartitions(
                lessThanOrEqual("ts", "2021-03-09T10:00:00.000000"),
                PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
                true))
        .as("Should not select partitions, 1 ms off boundary")
        .isFalse();

    assertThat(
            selectsPartitions(
                lessThan("ts", "2021-03-09T10:00:00.000000"),
                PartitionSpec.builderFor(SCHEMA).day("ts").build(),
                true))
        .as("Should not select partitions, on hour not day boundary")
        .isFalse();
  }

  private void assertEquals(Expression expected, Expression actual) {
    assertThat(expected).isInstanceOf(UnboundPredicate.class);
    assertEquals((UnboundPredicate<?>) expected, (UnboundPredicate<?>) actual);
  }

  private void assertEquals(UnboundPredicate<?> expected, UnboundPredicate<?> actual) {
    assertThat(actual.op()).as("Operation should match").isEqualTo(expected.op());
    assertEquals(expected.term(), actual.term());
    assertThat(actual.literals()).as("Literals should match").isEqualTo(expected.literals());
  }

  private void assertEquals(UnboundTerm<?> expected, UnboundTerm<?> actual) {
    assertThat(expected)
        .as("Unknown expected term: " + expected)
        .isOfAnyClassIn(NamedReference.class, UnboundTransform.class);

    if (expected instanceof NamedReference) {
      assertThat(actual).as("Should be a NamedReference").isInstanceOf(NamedReference.class);
      assertEquals((NamedReference<?>) expected, (NamedReference<?>) actual);
    } else if (expected instanceof UnboundTransform) {
      assertThat(actual).as("Should be an UnboundTransform").isInstanceOf(UnboundTransform.class);
      assertEquals((UnboundTransform<?, ?>) expected, (UnboundTransform<?, ?>) actual);
    }
  }

  private void assertEquals(NamedReference<?> expected, NamedReference<?> actual) {
    assertThat(actual.name()).as("Should reference the same field name").isEqualTo(expected.name());
  }

  private void assertEquals(UnboundTransform<?, ?> expected, UnboundTransform<?, ?> actual) {
    assertThat(actual.transform())
        .as("Should apply the same transform")
        .hasToString(expected.transform().toString());
    assertEquals(expected.ref(), actual.ref());
  }
}
