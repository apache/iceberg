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
package org.apache.iceberg.spark.broadcastvar.expressions.transforms;

import static org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils.createPredicate;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.transforms.BaseDatesProjection;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class TestDatesProjectionWithRangeIn extends BaseDatesProjection {

  @Test
  public void testMonthStrictEpoch() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveEpoch() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[1969-12, 1970-01]");
  }

  @Test
  public void testMonthStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();
    Integer anotherDate = (Integer) Literal.of("2017-12-02").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {anotherDate, date}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictLowerBound() {
    Integer date = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();
    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testMonthStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();
    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {anotherDate, date}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();
    Integer anotherDate = (Integer) Literal.of("1969-11-01").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-12-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();
    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[2017-01, 2017-12]");
  }

  @Test
  public void testNegativeMonthInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[1969-01, 1969-02, 1969-12, 1970-01]");
  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[2017-01, 2017-12]");
  }

  @Test
  public void testNegativeMonthInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    Integer anotherDate = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[1969-01, 1969-02, 1969-12, 1970-01]");
  }

  @Test
  public void testDayStrict() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();
    Integer anotherDate = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeDayStrict() {
    Integer date = (Integer) Literal.of("1969-12-30").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();
    Integer anotherDate = (Integer) Literal.of("1969-12-28").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusive() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();
    Integer anotherDate = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[2017-01-01, 2017-12-31]");
  }

  @Test
  public void testNegativeDayInclusive() {
    Integer date = (Integer) Literal.of("1969-12-30").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();
    Integer anotherDate = (Integer) Literal.of("1969-12-28").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[1969-12-28, 1969-12-30]");
  }

  @Test
  public void testYearStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    Integer anotherDate = (Integer) Literal.of("2016-12-31").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeYearStrictLowerBound() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testYearStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    Integer anotherDate = (Integer) Literal.of("2016-01-01").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeYearStrictUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    Integer anotherDate = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testYearInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();
    Integer anotherDate = (Integer) Literal.of("2016-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[2016, 2017]");
  }

  @Test
  public void testNegativeYearInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("1970-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    Integer anotherDate = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[1969, 1970]");
  }

  @Test
  public void testYearInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();
    Integer anotherDate = (Integer) Literal.of("2016-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[2016, 2017]");
  }

  @Test
  public void testNegativeYearInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("1969-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();
    Integer anotherDate = (Integer) Literal.of("1969-01-01").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("date", new Object[] {date, anotherDate}, DataTypes.DateType),
        Expression.Operation.RANGE_IN,
        "[1969, 1970]");
  }
}
