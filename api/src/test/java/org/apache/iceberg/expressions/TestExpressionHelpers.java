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

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.predicate;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.rewriteNot;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.Callable;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestExpressionHelpers {
  private final UnboundPredicate<?> pred = lessThan("x", 7);

  @Test
  public void testSimplifyOr() {
    assertThat(or(alwaysTrue(), pred))
        .as("alwaysTrue or pred => alwaysTrue")
        .isEqualTo(alwaysTrue());
    assertThat(or(pred, alwaysTrue()))
        .as("pred or alwaysTrue => alwaysTrue")
        .isEqualTo(alwaysTrue());

    assertThat(or(alwaysFalse(), pred)).as("alwaysFalse or pred => pred").isEqualTo(pred);
    assertThat(or(pred, alwaysFalse())).as("pred or alwaysTrue => pred").isEqualTo(pred);
  }

  @Test
  public void testSimplifyAnd() {
    assertThat(and(alwaysTrue(), pred)).as("alwaysTrue and pred => pred").isEqualTo(pred);
    assertThat(and(pred, alwaysTrue())).as("pred and alwaysTrue => pred").isEqualTo(pred);

    assertThat(and(alwaysFalse(), pred))
        .as("alwaysFalse and pred => alwaysFalse")
        .isEqualTo(alwaysFalse());
    assertThat(and(pred, alwaysFalse()))
        .as("pred and alwaysFalse => alwaysFalse")
        .isEqualTo(alwaysFalse());
  }

  @Test
  public void testSimplifyNot() {
    assertThat(not(alwaysTrue())).as("not(alwaysTrue) => alwaysFalse").isEqualTo(alwaysFalse());
    assertThat(not(alwaysFalse())).as("not(alwaysFalse) => alwaysTrue").isEqualTo(alwaysTrue());

    assertThat(not(not(pred))).as("not(not(pred)) => pred").isEqualTo(pred);
  }

  @Test
  public void testRewriteNot() {
    StructType struct =
        StructType.of(
            NestedField.optional(1, "a", Types.IntegerType.get()),
            NestedField.optional(2, "s", Types.StringType.get()));
    Expression[][] expressions =
        new Expression[][] {
          // (rewritten pred, original pred) pairs
          {isNull("a"), isNull("a")},
          {notNull("a"), not(isNull("a"))},
          {notNull("a"), notNull("a")},
          {isNull("a"), not(notNull("a"))},
          {equal("a", 5), equal("a", 5)},
          {notEqual("a", 5), not(equal("a", 5))},
          {notEqual("a", 5), notEqual("a", 5)},
          {equal("a", 5), not(notEqual("a", 5))},
          {in("a", 5, 6), in("a", 5, 6)},
          {notIn("a", 5, 6), not(in("a", 5, 6))},
          {notIn("a", 5, 6), notIn("a", 5, 6)},
          {in("a", 5, 6), not(notIn("a", 5, 6))},
          {lessThan("a", 5), lessThan("a", 5)},
          {greaterThanOrEqual("a", 5), not(lessThan("a", 5))},
          {greaterThanOrEqual("a", 5), greaterThanOrEqual("a", 5)},
          {lessThan("a", 5), not(greaterThanOrEqual("a", 5))},
          {lessThanOrEqual("a", 5), lessThanOrEqual("a", 5)},
          {greaterThan("a", 5), not(lessThanOrEqual("a", 5))},
          {greaterThan("a", 5), greaterThan("a", 5)},
          {lessThanOrEqual("a", 5), not(greaterThan("a", 5))},
          {or(equal("a", 5), isNull("a")), or(equal("a", 5), isNull("a"))},
          {and(notEqual("a", 5), notNull("a")), not(or(equal("a", 5), isNull("a")))},
          {and(notEqual("a", 5), notNull("a")), and(notEqual("a", 5), notNull("a"))},
          {or(equal("a", 5), isNull("a")), not(and(notEqual("a", 5), notNull("a")))},
          {or(equal("a", 5), notNull("a")), or(equal("a", 5), not(isNull("a")))},
          {startsWith("s", "hello"), not(notStartsWith("s", "hello"))},
          {notStartsWith("s", "world"), not(startsWith("s", "world"))}
        };

    for (Expression[] pair : expressions) {
      // unbound rewrite
      assertThat(rewriteNot(pair[1]))
          .as(String.format("rewriteNot(%s) should be %s", pair[1], pair[0]))
          .hasToString(pair[0].toString());

      // bound rewrite
      Expression expectedBound = Binder.bind(struct, pair[0]);
      Expression toRewriteBound = Binder.bind(struct, pair[1]);
      assertThat(rewriteNot(toRewriteBound))
          .as(String.format("rewriteNot(%s) should be %s", toRewriteBound, expectedBound))
          .hasToString(expectedBound.toString());
    }
  }

  @Test
  public void testTransformExpressions() {
    assertThat(equal(year("ts"), "2019"))
        .as("Should produce the correct expression string")
        .hasToString("year(ref(name=\"ts\")) == \"2019\"");
    assertThat(equal(month("ts"), 1234))
        .as("Should produce the correct expression string")
        .hasToString("month(ref(name=\"ts\")) == 1234");
    assertThat(equal(day("ts"), "2019-12-04"))
        .as("Should produce the correct expression string")
        .hasToString("day(ref(name=\"ts\")) == \"2019-12-04\"");
    assertThat(equal(hour("ts"), "2019-12-04-10"))
        .as("Should produce the correct expression string")
        .hasToString("hour(ref(name=\"ts\")) == \"2019-12-04-10\"");
    assertThat(equal(truncate("str", 6), "abcdef"))
        .as("Should produce the correct expression string")
        .hasToString("truncate[6](ref(name=\"str\")) == \"abcdef\"");
    assertThat(equal(truncate("i", 5), 10))
        .as("Should produce the correct expression string")
        .hasToString("truncate[5](ref(name=\"i\")) == 10");
    assertThat(equal(bucket("id", 16), 12))
        .as("Should produce the correct expression string")
        .hasToString("bucket[16](ref(name=\"id\")) == 12");
  }

  @Test
  public void testNullName() {
    assertThatThrownBy(() -> equal((String) null, 5))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Name cannot be null");
  }

  @Test
  public void testNullValueExpr() {
    assertThatThrownBy(() -> equal((UnboundTerm<Integer>) null, 5))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Term cannot be null");
  }

  @Test
  public void testMultiAnd() {
    Expression expected = and(and(equal("a", 1), equal("b", 2)), equal("c", 3));

    Expression actual = and(equal("a", 1), equal("b", 2), equal("c", 3));

    assertThat(actual).hasToString(expected.toString());
  }

  @Test
  public void testInvalidateNaNInput() {
    assertInvalidateNaNThrows(() -> lessThan("a", Double.NaN));
    assertInvalidateNaNThrows(() -> lessThan(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> lessThanOrEqual("a", Double.NaN));
    assertInvalidateNaNThrows(() -> lessThanOrEqual(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> greaterThan("a", Double.NaN));
    assertInvalidateNaNThrows(() -> greaterThan(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> greaterThanOrEqual("a", Double.NaN));
    assertInvalidateNaNThrows(() -> greaterThanOrEqual(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> equal("a", Double.NaN));
    assertInvalidateNaNThrows(() -> equal(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> notEqual("a", Double.NaN));
    assertInvalidateNaNThrows(() -> notEqual(self("a"), Double.NaN));

    assertInvalidateNaNThrows(() -> in("a", 1.0D, 2.0D, Double.NaN));
    assertInvalidateNaNThrows(() -> in(self("a"), 1.0D, 2.0D, Double.NaN));

    assertInvalidateNaNThrows(() -> notIn("a", 1.0D, 2.0D, Double.NaN));
    assertInvalidateNaNThrows(() -> notIn(self("a"), 1.0D, 2.0D, Double.NaN));

    assertInvalidateNaNThrows(() -> predicate(Expression.Operation.EQ, "a", Double.NaN));
  }

  private void assertInvalidateNaNThrows(Callable<UnboundPredicate<Double>> callable) {
    assertThatThrownBy(callable::call)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create expression literal from NaN");
  }

  private <T> UnboundTerm<T> self(String name) {
    return new UnboundTransform<>(ref(name), Transforms.identity());
  }
}
