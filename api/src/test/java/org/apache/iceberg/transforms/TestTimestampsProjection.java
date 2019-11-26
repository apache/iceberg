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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestTimestampsProjection {
  private static final Types.TimestampType TYPE = Types.TimestampType.withoutZone();
  private static final Schema SCHEMA = new Schema(optional(1, "timestamp", TYPE));

  public void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter,
                                     Expression.Operation expectedOp, String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Literal literal = predicate.literal();
    Timestamps transform = (Timestamps) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((int) literal.value());
    Assert.assertEquals(expectedLiteral, output);
  }

  public void assertProjectionStrictValue(PartitionSpec spec, UnboundPredicate<?> filter,
                                          Expression.Operation expectedOp) {

    Expression projection = Projections.strict(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusiveValue(PartitionSpec spec, UnboundPredicate<?> filter,
                                             Expression.Operation expectedOp) {

    Expression projection = Projections.inclusive(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter,
                                        Expression.Operation expectedOp, String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Literal literal = predicate.literal();
    Timestamps transform = (Timestamps) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((int) literal.value());
    Assert.assertEquals(expectedLiteral, output);
  }

  @Test
  public void testMonthStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-11");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2018-01");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testDayStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-01");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-11-30");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testDayStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-02");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12-01");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-11-30");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testDayInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-02");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testYearStrictLowerBound() {
    Long date = (long) Literal.of("2017-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2016");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testYearStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2018");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testYearInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-01-01T00:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testYearInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-31T23:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testHourStrictLowerBound() {
    Long date = (long) Literal.of("2017-12-01T10:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01-10");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-01-10");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01-10");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12-01-09");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01-10");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testHourStrictUpperBound() {
    Long date = (long) Literal.of("2017-12-01T10:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT, "2017-12-01-10");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT, "2017-12-01-11");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT, "2017-12-01-10");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT, "2017-12-01-10");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01-10");
    assertProjectionStrictValue(spec, equal("timestamp", date), Expression.Operation.FALSE);
  }

  @Test
  public void testHourInclusiveLowerBound() {
    Long date = (long) Literal.of("2017-12-01T10:00:00.00000").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-09");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01-10");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }

  @Test
  public void testHourInclusiveUpperBound() {
    Long date = (long) Literal.of("2017-12-01T10:59:59.999999").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-11");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01-10");
    assertProjectionInclusiveValue(spec, notEqual("timestamp", date), Expression.Operation.TRUE);
  }
}
