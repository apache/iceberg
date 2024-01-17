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
import org.apache.iceberg.transforms.BaseTimestampsProjection;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class TestTimestampsProjectionWithRangeIn extends BaseTimestampsProjection {

  @Test
  public void testDayStrictEpoch() {
    Long date = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();
    Long anotherDate = (long) Literal.of("1970-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusiveEpoch() {
    Long date = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();
    Long anotherDate = (long) Literal.of("1970-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[1970-01-01, 1970-01-02]");
  }

  @Test
  public void testMonthStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {anotherDate, date}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("1969-03-01T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {anotherDate, date}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testMonthStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("2017-11-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {anotherDate, date}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeMonthStrictUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("1970-02-01T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {anotherDate, date}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2017-12, 2017-12]");
  }

  @Test
  public void testNegativeMonthInclusiveLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("1969-03-01T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[1969-01, 1969-02, 1969-03, 1969-04]");
  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    Long anotherDate = (long) Literal.of("2017-11-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2017-11, 2017-12]");
  }

  @Test
  public void testNegativeMonthInclusiveUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();
    Long anotherDate = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[1969-12, 1970-01]");
  }

  @Test
  public void testDayStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();
    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();

    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeDayStrictLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();
    Long anotherDate = (long) Literal.of("1969-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testDayStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    Long anotherDate = (long) Literal.of("2017-11-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testNegativeDayStrictUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    Long anotherDate = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2017-12-01, 2017-12-02]");
  }

  @Test
  public void testNegativeDayInclusiveLowerBound() {
    Long date = (long) Literal.of("1969-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    Long anotherDate = (long) Literal.of("1969-01-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[1969-01-01, 1969-01-02, 1969-01-03]");
  }

  @Test
  public void testDayInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    Long anotherDate = (long) Literal.of("2017-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2017-12-01, 2017-12-02]");
  }

  @Test
  public void testNegativeDayInclusiveUpperBound() {
    Long date = (long) Literal.of("1969-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    Long anotherDate = (long) Literal.of("1970-01-01T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}),
        Expression.Operation.RANGE_IN,
        "[1969-12-31, 1970-01-01]");
  }

  @Test
  public void testYearStrictLowerBound() {
    Long date = (long) Literal.of("2017-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testYearStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testYearInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2016, 2017]");
  }

  @Test
  public void testYearInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();
    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2016, 2017]");
  }

  @Test
  public void testHourStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T10:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();
    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testHourStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-01T10:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionStrictValue(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testHourInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T10:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    Long anotherDate = (long) Literal.of("2016-12-02T00:00:00.00000").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2016-12-02-00, 2017-12-01-10]");
  }

  @Test
  public void testHourInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T10:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    Long anotherDate = (long) Literal.of("2016-12-31T23:59:59.999999").to(TYPE).value();
    assertProjectionInclusive(
        spec,
        createPredicate("timestamp", new Object[] {date, anotherDate}, DataTypes.TimestampType),
        Expression.Operation.RANGE_IN,
        "[2016-12-31-23, 2017-12-01-10]");
  }
}
