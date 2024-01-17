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

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.junit.Test;

public class TestDatesProjection extends BaseDatesProjection {

  @Test
  public void testMonthStrictEpoch() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "1970-01");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "1970-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "1970-02");
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "1970-01");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "1970-01");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", date, anotherDate), Expression.Operation.NOT_IN, "[1969-12, 1970-01]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveEpoch() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "1970-01");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "1970-01");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[1969-12, 1970-01]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testMonthStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017-01");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2017-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017-01");
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2016-12");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-01");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2017-12-02").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", anotherDate, date), Expression.Operation.NOT_IN, "[2017-01, 2017-12]");
    assertProjectionStrictValue(spec, in("date", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictLowerBound() {
    Integer date = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "1969-01");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "1969-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "1969-02");
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "1969-01");
    assertProjectionStrict(
        spec, notEqual("date", date), Expression.Operation.NOT_IN, "[1969-01, 1969-02]");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[1969-01, 1969-02, 1969-12, 1970-01]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2018-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-12");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", anotherDate, date), Expression.Operation.NOT_IN, "[2017-01, 2017-12]");
    assertProjectionStrictValue(spec, in("date", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "1969-12");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "1970-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "1970-01");
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "1970-01");
    assertProjectionStrict(
        spec, notEqual("date", date), Expression.Operation.NOT_IN, "[1969-12, 1970-01]");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("1969-11-01").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[1969-11, 1969-12, 1970-01]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-12-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[2017-01, 2017-12]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeMonthInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "1969-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "1969-02");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "1969-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "1969-01");
    assertProjectionInclusive(
        spec, equal("date", date), Expression.Operation.IN, "[1969-01, 1969-02]");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("date", date, anotherDate),
        Expression.Operation.IN,
        "[1969-01, 1969-02, 1969-12, 1970-01]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[2017-01, 2017-12]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeMonthInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "1970-01");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "1969-12");
    assertProjectionInclusive(
        spec, equal("date", date), Expression.Operation.IN, "[1969-12, 1970-01]");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        in("date", date, anotherDate),
        Expression.Operation.IN,
        "[1969-01, 1969-02, 1969-12, 1970-01]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testDayStrict() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017-01-01");
    // should be the same date for <=
    assertProjectionStrict(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2017-01-02");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017-01-01");
    // should be the same date for >=
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2016-12-31");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-01-01");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[2017-01-01, 2017-12-31]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeDayStrict() {
    Integer date = (Integer) Literal.of("1969-12-30").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "1969-12-30");
    // should be the same date for <=
    assertProjectionStrict(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT, "1969-12-31");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "1969-12-30");
    // should be the same date for >=
    assertProjectionStrict(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "1969-12-29");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "1969-12-30");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("1969-12-28").to(TYPE).value();
    assertProjectionStrict(
        spec,
        notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN,
        "[1969-12-28, 1969-12-30]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusive() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionInclusive(
        spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016-12-31");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-01-01");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-01-02");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-01-01");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-01-01");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[2017-01-01, 2017-12-31]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeDayInclusive() {
    Integer date = (Integer) Literal.of("1969-12-30").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionInclusive(
        spec, lessThan("date", date), Expression.Operation.LT_EQ, "1969-12-29");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "1969-12-30");
    assertProjectionInclusive(
        spec, greaterThan("date", date), Expression.Operation.GT_EQ, "1969-12-31");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "1969-12-30");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "1969-12-30");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("1969-12-28").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[1969-12-28, 1969-12-30]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testYearStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2016");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2016-12-31").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", date, anotherDate), Expression.Operation.NOT_IN, "[2016, 2017]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeYearStrictLowerBound() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "1970");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "1970");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "1971");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "1970");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "1970");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", date, anotherDate), Expression.Operation.NOT_IN, "[1969, 1970]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testYearStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2018");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2016-01-01").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", date, anotherDate), Expression.Operation.NOT_IN, "[2016, 2017]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeYearStrictUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "1969");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "1970");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "1970");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "1970");
    assertProjectionStrict(
        spec, notEqual("date", date), Expression.Operation.NOT_IN, "[1969, 1970]");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    assertProjectionStrict(
        spec, notIn("date", date, anotherDate), Expression.Operation.NOT_IN, "[1969, 1970]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testYearInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2016-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[2016, 2017]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeYearInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "1970");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "1970");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "1970");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "1970");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "1970");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[1969, 1970]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testYearInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2016-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[2016, 2017]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testNegativeYearInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "1970");
    assertProjectionInclusive(
        spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "1970");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "1970");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "1969");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.IN, "[1969, 1970]");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec, in("date", date, anotherDate), Expression.Operation.IN, "[1969, 1970]");
    assertProjectionInclusiveValue(
        spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }
}
