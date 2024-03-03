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

import static org.apache.iceberg.expressions.Expressions.notContains;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.jupiter.api.Test;

public class TestNotContains extends TestTransformBase {

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    assertProjectionInclusive(
        spec, notContains(COLUMN, "ab"), "ab", Expression.Operation.NOT_CONTAINS);
    assertProjectionInclusive(
        spec, notContains(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    // When literal is longer than partition spec's truncation width, we always read for an
    // inclusive projection when using notContains.
    Expression projection = Projections.inclusive(spec).project(notContains(COLUMN, "ababab"));
    assertThat(projection).isInstanceOf(True.class);

    assertProjectionStrict(
        spec, notContains(COLUMN, "ab"), "ab", Expression.Operation.NOT_CONTAINS);
    assertProjectionStrict(spec, notContains(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    assertProjectionStrict(
        spec, notContains(COLUMN, "abcdab"), "abcd", Expression.Operation.NOT_CONTAINS);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEvaluatorTruncateString() {
    Truncate<String> trunc = Truncate.get(2);
    Expression expr = notContains(COLUMN, "cdg");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);
    // It will truncate the first 2 characters and then compare notContains.
    assertThat(evaluator.eval(TestHelpers.Row.of("abce")))
        .as("notContains(abcde, truncate(cdg,2))  => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsLongerThanWidth() {
    Truncate<String> trunc = Truncate.get(2);
    UnboundPredicate<String> expr = notContains(COLUMN, "abcde");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as("The projected literal should be truncated to the truncation width")
        .isEqualTo("ab");

    assertThat(evaluator.eval(TestHelpers.Row.of("eabcde")))
        .as("notContains('eabcde', truncate('abcde',2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notContains('ab', truncate('abcde', 2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdz")))
        .as("notContains('abcdz', truncate('abcdz', 2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notContains('a', truncate('abcde', 2)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("aczcde")))
        .as("notContains('aczcde', truncate('abcde', 2)) => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsShorterThanWidth() {
    Truncate<String> trunc = Truncate.get(16);
    UnboundPredicate<String> expr = notContains(COLUMN, "bc");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as(
            "The projected literal should not be truncated as its size is shorter than truncation width")
        .isEqualTo("bc");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
        .as("notContains('abcde', truncate('bc', 16)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("bc")))
        .as("notContains('bc', truncate('bc', 16)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notContains('a', truncate('bc', 16)) => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsEqualToWidth() {
    Truncate<String> trunc = Truncate.get(7);
    UnboundPredicate<String> expr = notContains(COLUMN, "abcdefg");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as(
            "The projected literal should not be truncated as its size is equal to truncation width")
        .isEqualTo("abcdefg");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdefg")))
        .as("notContains('abcdefg', truncate('abcdefg', 7)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notContains('ab', truncate('abcdefg', 7)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notContains('a', truncate('abcdefg', 7)) => true")
        .isTrue();
  }

  @Test
  public void testStrictMetricsEvaluatorForNotContains() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, notContains(COLUMN, "bbb")).eval(TEST_FILE);
    assertThat(shouldRead)
        .as("Should not match: strict metrics eval is always false for notContains")
        .isFalse();
  }

  @Test
  public void testInclusiveMetricsEvaluatorForNotContains() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notContains(COLUMN, "aaa")).eval(TEST_FILE);
    assertThat(shouldRead)
        .as("Should match: inclusive metrics evaluator is always true for notContains")
        .isTrue();
  }
}
