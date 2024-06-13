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

import static org.apache.iceberg.expressions.Expressions.notEndsWith;
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

public class TestNotEndsWith extends TestTransformBase {

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    assertProjectionInclusive(
        spec, notEndsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_ENDS_WITH);
    assertProjectionInclusive(
        spec, notEndsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    // When literal is longer than partition spec's truncation width, we always read for an
    // inclusive projection when using notEndsWith.
    Expression projection = Projections.inclusive(spec).project(notEndsWith(COLUMN, "ababab"));
    assertThat(projection).isInstanceOf(True.class);

    assertProjectionStrict(
        spec, notEndsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_ENDS_WITH);
    assertProjectionStrict(spec, notEndsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    assertProjectionStrict(
        spec, notEndsWith(COLUMN, "abcdab"), "abcd", Expression.Operation.NOT_ENDS_WITH);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEvaluatorTruncateString() {
    Truncate<String> trunc = Truncate.get(2);
    Expression expr = notEndsWith(COLUMN, "cdg");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);
    // It will truncate the first 2 characters and then compare from the end.
    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
        .as("notEndsWith('abcde', truncate('cdg',2))  => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsLongerThanWidth() {
    Truncate<String> trunc = Truncate.get(2);
    UnboundPredicate<String> expr = notEndsWith(COLUMN, "abcde");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as("The projected literal should be truncated to the truncation width")
        .isEqualTo("ab");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
        .as("notEndsWith('abcde', truncate('abcde',2)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notEndsWith('ab', truncate('ab', 2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdz")))
        .as("notEndsWith('abcdz', truncate('abcde', 2)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notEndsWith('a', truncate('abcde', 2)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("aczcab")))
        .as("notEndsWith('aczcab', truncate('abcde', 2)) => false")
        .isFalse();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsShorterThanWidth() {
    Truncate<String> trunc = Truncate.get(16);
    UnboundPredicate<String> expr = notEndsWith(COLUMN, "ab");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as(
            "The projected literal should not be truncated as its size is shorter than truncation width")
        .isEqualTo("ab");

    assertThat(evaluator.eval(TestHelpers.Row.of("efcab")))
        .as("notEndsWith('efcab', truncate('ab', 16)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notEndsWith('ab', truncate('ab', 16)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notEndsWith('a', truncate('ab', 16)) => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsEqualToWidth() {
    Truncate<String> trunc = Truncate.get(7);
    UnboundPredicate<String> expr = notEndsWith(COLUMN, "abcdefg");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as(
            "The projected literal should not be truncated as its size is equal to truncation width")
        .isEqualTo("abcdefg");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdefg")))
        .as("notEndsWith('abcdefg', truncate(abcdefg, 7)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notEndsWith('ab', truncate('abcdefg', 7)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notEndsWith('a', truncate('abcdefg', 7)) => true")
        .isTrue();
  }

  @Test
  public void testStrictMetricsEvaluatorForNotEndsWith() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, notEndsWith(COLUMN, "bbb")).eval(TEST_FILE);
    assertThat(shouldRead)
        .as("Should not match: strict metrics eval is always false for notEndsWith")
        .isFalse();
  }

  @Test
  public void testInclusiveMetricsEvaluatorForNotEndsWith() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notEndsWith(COLUMN, "aaa")).eval(TEST_FILE);
    assertThat(shouldRead)
        .as("Should match: inclusive metrics evaluator is always true for notEndsWith")
        .isTrue();
  }
}
