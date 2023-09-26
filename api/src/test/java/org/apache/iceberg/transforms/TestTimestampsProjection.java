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

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTimestampsProjection {
  private static final Types.TimestampType TYPE = Types.TimestampType.microsWithoutZone();
  private static final Schema SCHEMA = new Schema(optional(1, "timestamp", TYPE));

  @SuppressWarnings("unchecked")
  public void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<Integer> predicate = assertAndUnwrapUnbound(projection);

    assertThat(predicate.op())
        .isEqualTo(expectedOp)
        .as("Strict projection never runs for IN")
        .isNotEqualTo(Expression.Operation.IN);

    Transform<?, Integer> transform =
        (Transform<?, Integer>) spec.getFieldsBySourceId(1).get(0).transform();
    Type type = spec.partitionType().field(spec.getFieldsBySourceId(1).get(0).fieldId()).type();
    if (predicate.op() == Expression.Operation.NOT_IN) {
      Iterable<Integer> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(v -> transform.toHumanString(type, v))
              .collect(Collectors.toList())
              .toString();
      assertThat(actual).isEqualTo(expectedLiteral);
    } else {
      Literal<Integer> literal = predicate.literal();
      String output = transform.toHumanString(type, literal.value());
      assertThat(output).isEqualTo(expectedLiteral);
    }
  }

  public void assertProjectionStrictValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {

    Expression projection = Projections.strict(spec).project(filter);
    assertThat(projection.op()).isEqualTo(expectedOp);
  }

  public void assertProjectionInclusiveValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {

    Expression projection = Projections.inclusive(spec).project(filter);
    assertThat(projection.op()).isEqualTo(expectedOp);
  }

  @SuppressWarnings("unchecked")
  public void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<Integer> predicate = assertAndUnwrapUnbound(projection);

    assertThat(predicate.op())
        .isEqualTo(expectedOp)
        .as("Inclusive projection never runs for NOT_IN")
        .isNotEqualTo(Expression.Operation.NOT_IN);

    Transform<?, Integer> transform =
        (Transform<?, Integer>) spec.getFieldsBySourceId(1).get(0).transform();
    Type type = spec.partitionType().field(spec.getFieldsBySourceId(1).get(0).fieldId()).type();
    if (predicate.op() == Expression.Operation.IN) {
      Iterable<Integer> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(v -> transform.toHumanString(type, v))
              .collect(Collectors.toList())
              .toString();
      assertThat(actual).isEqualTo(expectedLiteral);
    } else {
      Literal<Integer> literal = predicate.literal();
      String output = transform.toHumanString(type, literal.value());
      assertThat(output).isEqualTo(expectedLiteral);
    }
  }

  @Test
  public void testDayStrictEpoch() {
    Long date = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "1970-01-01");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "1970-01-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "1970-01-02");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "1970-01-01");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "1970-01-01");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("1970-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[1970-01-01, 1970-01-02]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusiveEpoch() {
    Long date = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.EQ, "1970-01-01");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("1970-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[1970-01-01, 1970-01-02]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testMonthStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-11");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", anotherDate, date),
        Expression.Operation.NOT_IN,
        "[2017-12, 2017-12]");
    assertProjectionStrictValue(
        spec, in("timestamp", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "1969-01");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "1969-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "1969-02");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "1969-01");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_IN, "[1969-01, 1969-02]");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("1969-03-01T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", anotherDate, date),
        Expression.Operation.NOT_IN,
        "[1969-01, 1969-02, 1969-03, 1969-04]");
    assertProjectionStrictValue(
        spec, in("timestamp", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2018-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2017-11-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", anotherDate, date),
        Expression.Operation.NOT_IN,
        "[2017-11, 2017-12]");
    assertProjectionStrictValue(
        spec, in("timestamp", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "1969-12");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "1970-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "1970-01");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "1970-01");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_IN, "[1969-12, 1970-01]");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("1970-02-01T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", anotherDate, date),
        Expression.Operation.NOT_IN,
        "[1969-12, 1970-01, 1970-02]");
    assertProjectionStrictValue(
        spec, in("timestamp", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("timestamp", date, anotherDate), Expression.Operation.IN, "[2017-12, 2017-12]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeMonthInclusiveLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "1969-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "1969-02");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "1969-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "1969-01");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.IN, "[1969-01, 1969-02]");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("1969-03-01T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[1969-01, 1969-02, 1969-03, 1969-04]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2017-11-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("timestamp", date, anotherDate), Expression.Operation.IN, "[2017-11, 2017-12]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeMonthInclusiveUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "1969-12");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.IN, "[1969-12, 1970-01]");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("timestamp", date, anotherDate), Expression.Operation.IN, "[1969-12, 1970-01]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testDayStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-11-30");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[2017-12-01, 2017-12-02]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeDayStrictLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "1969-01-01");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "1969-01-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "1969-01-02");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "1969-01-01");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_IN, "[1969-01-01, 1969-01-02]");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("1969-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[1969-01-01, 1969-01-02, 1969-01-03]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testDayStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-02");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12-01");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2017-11-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[2017-11-02, 2017-12-01]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeDayStrictUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "1969-12-31");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "1970-01-01");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "1970-01-01");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "1970-01-01");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_IN, "[1969-12-31, 1970-01-01]");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[1969-12-31, 1970-01-01]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-11-30");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[2017-12-01, 2017-12-02]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeDayInclusiveLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "1969-01-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "1969-01-02");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "1969-01-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "1969-01-01");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.IN, "[1969-01-01, 1969-01-02]");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("1969-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[1969-01-01, 1969-01-02, 1969-01-03]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testDayInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-02");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[2017-12-01, 2017-12-02]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeDayInclusiveUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "1970-01-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "1969-12-31");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.IN, "[1969-12-31, 1970-01-01]");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[1969-12-31, 1970-01-01]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testYearStrictLowerBound() {
    Long date = (long) Literal.of("2017-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2016");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.NOT_IN, "[2016, 2017]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testYearStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2018");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.NOT_IN, "[2016, 2017]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testYearInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("timestamp", date, anotherDate), Expression.Operation.IN, "[2016, 2017]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testYearInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("timestamp", date, anotherDate), Expression.Operation.IN, "[2016, 2017]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testHourStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T10:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01-10");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-01-10");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01-10");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12-01-09");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01-10");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[2016-12-02-00, 2017-12-01-10]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testHourStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-01T10:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionStrict(
        spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01-10");
    assertProjectionStrict(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-01-11");
    assertProjectionStrict(
        spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01-10");
    assertProjectionStrict(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12-01-10");
    assertProjectionStrict(
        spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01-10");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("timestamp", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[2016-12-31-23, 2017-12-01-10]");
    assertProjectionStrictValue(
        spec, in("timestamp", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testHourInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T10:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-09");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01-10");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[2016-12-02-00, 2017-12-01-10]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testHourInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T10:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionInclusive(
        spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(
        spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(
        spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-11");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(
        spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01-10");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("timestamp", date, anotherDate),
        Expression.Operation.IN,
        "[2016-12-31-23, 2017-12-01-10]");
    assertProjectionInclusiveValue(
        spec, notIn("timestamp", date, anotherDate), Expression.Operation.TRUE);
  }
}
