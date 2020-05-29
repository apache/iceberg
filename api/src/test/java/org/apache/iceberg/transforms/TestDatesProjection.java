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

import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

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

public class TestDatesProjection {
  private static final Types.DateType TYPE = Types.DateType.get();
  private static final Schema SCHEMA = new Schema(optional(1, "date", TYPE));

  public void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter,
                                     Expression.Operation expectedOp, String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Assert.assertNotEquals("Strict projection never runs for IN", Expression.Operation.IN, predicate.op());

    Dates transform = (Dates) spec.getFieldsBySourceId(1).get(0).transform();
    if (predicate.op() == Expression.Operation.NOT_IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual = Lists.newArrayList(values).stream().sorted()
          .map(v -> transform.toHumanString((Integer) v)).collect(Collectors.toList()).toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal literal = predicate.literal();
      String output = transform.toHumanString((int) literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
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

    Assert.assertNotEquals("Inclusive projection never runs for NOT_IN", Expression.Operation.NOT_IN, predicate.op());

    Dates transform = (Dates) spec.getFieldsBySourceId(1).get(0).transform();
    if (predicate.op() == Expression.Operation.IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual = Lists.newArrayList(values).stream().sorted()
          .map(v -> transform.toHumanString((Integer) v)).collect(Collectors.toList()).toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal literal = predicate.literal();
      String output = transform.toHumanString((int) literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }

  @Test
  public void testMonthStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017-01");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2017-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017-01");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2016-12");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-01");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2017-12-02").to(TYPE).value();
    assertProjectionStrict(spec, notIn("date", anotherDate, date),
        Expression.Operation.NOT_IN, "[2017-01, 2017-12]");
    assertProjectionStrictValue(spec, in("date", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017-12");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2018-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2017-12");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-12");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionStrict(spec, notIn("date", anotherDate, date),
        Expression.Operation.NOT_IN, "[2017-01, 2017-12]");
    assertProjectionStrictValue(spec, in("date", anotherDate, date), Expression.Operation.FALSE);
  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-12-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionInclusive(spec, in("date", date, anotherDate),
        Expression.Operation.IN, "[2017-01, 2017-12]");
    assertProjectionInclusiveValue(spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    assertProjectionInclusive(spec, in("date", date, anotherDate),
        Expression.Operation.IN, "[2017-01, 2017-12]");
    assertProjectionInclusiveValue(spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testDayStrict() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT, "2017-01-01");
    // should be the same date for <=
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT, "2017-01-02");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT, "2017-01-01");
    // should be the same date for >=
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT, "2016-12-31");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-01-01");
    assertProjectionStrictValue(spec, equal("date", date), Expression.Operation.FALSE);

    Integer anotherDate = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    assertProjectionStrict(spec, notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN, "[2017-01-01, 2017-12-31]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testDayInclusive() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016-12-31");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-01-01");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-01-02");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-01-01");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-01-01");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    assertProjectionInclusive(spec, in("date", date, anotherDate),
        Expression.Operation.IN, "[2017-01-01, 2017-12-31]");
    assertProjectionInclusiveValue(spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
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
    assertProjectionStrict(spec, notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN, "[2016, 2017]");
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
    assertProjectionStrict(spec, notIn("date", date, anotherDate),
        Expression.Operation.NOT_IN, "[2016, 2017]");
    assertProjectionStrictValue(spec, in("date", date, anotherDate), Expression.Operation.FALSE);
  }

  @Test
  public void testYearInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2016-12-31").to(TYPE).value();
    assertProjectionInclusive(spec, in("date", date, anotherDate),
        Expression.Operation.IN, "[2016, 2017]");
    assertProjectionInclusiveValue(spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }

  @Test
  public void testYearInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017");
    assertProjectionInclusiveValue(spec, notEqual("date", date), Expression.Operation.TRUE);

    Integer anotherDate = (Integer) Literal.of("2016-01-01").to(TYPE).value();
    assertProjectionInclusive(spec, in("date", date, anotherDate),
        Expression.Operation.IN, "[2016, 2017]");
    assertProjectionInclusiveValue(spec, notIn("date", date, anotherDate), Expression.Operation.TRUE);
  }
}
