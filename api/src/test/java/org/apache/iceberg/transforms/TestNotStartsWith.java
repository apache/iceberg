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
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Test;

public class TestNotStartsWith {

  private static final String COLUMN = "someStringCol";
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN, Types.StringType.get()));

  // All 50 rows have someStringCol = 'bbb', none are null (despite being optional).
  private static final DataFile FILE_1 =
      new TestDataFile(
          "file_1.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(1, 50L),
          // null value counts
          ImmutableMap.of(1, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(1, toByteBuffer(StringType.get(), "bbb")),
          // upper bounds
          ImmutableMap.of(1, toByteBuffer(StringType.get(), "bbb")));

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    assertProjectionInclusive(
        spec, notStartsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_STARTS_WITH);
    assertProjectionInclusive(
        spec, notStartsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    // When literal is longer than partition spec's truncation width, we always read for an
    // inclusive projection
    // when using notStartsWith.
    Expression projection = Projections.inclusive(spec).project(notStartsWith(COLUMN, "ababab"));
    assertThat(projection).isInstanceOf(True.class);

    assertProjectionStrict(
        spec, notStartsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_STARTS_WITH);
    assertProjectionStrict(
        spec, notStartsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    assertProjectionStrict(
        spec, notStartsWith(COLUMN, "ababab"), "abab", Expression.Operation.NOT_STARTS_WITH);
    assertProjectionStrict(
        spec, notStartsWith(COLUMN, "abcde"), "abcd", Expression.Operation.NOT_STARTS_WITH);
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsLongerThanWidth() {
    Truncate<String> trunc = Truncate.get(2);
    UnboundPredicate<String> expr = notStartsWith(COLUMN, "abcde");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as("The projected literal should be truncated to the truncation width")
        .isEqualTo("ab");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
        .as("notStartsWith(abcde, truncate(abcde,2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notStartsWith(abcde, truncate(ab, 2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdz")))
        .as("notStartsWith(abcde, truncate(abcdz, 2)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notStartsWith(abcde, truncate(a, 2)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("aczcde")))
        .as("notStartsWith(abcde, truncate(aczcde, 2)) => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsShorterThanWidth() {
    Truncate<String> trunc = Truncate.get(16);
    UnboundPredicate<String> expr = notStartsWith(COLUMN, "ab");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as(
            "The projected literal should not be truncated as its size is shorter than truncation width")
        .isEqualTo("ab");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
        .as("notStartsWith(ab, truncate(abcde, 16)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notStartsWith(ab, truncate(ab, 16)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notStartsWith(ab, truncate(a, 16)) => true")
        .isTrue();
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsEqualToWidth() {
    Truncate<String> trunc = Truncate.get(7);
    UnboundPredicate<String> expr = notStartsWith(COLUMN, "abcdefg");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(projected.literal().value())
        .as(
            "The projected literal should not be truncated as its size is equal to truncation width")
        .isEqualTo("abcdefg");

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdefg")))
        .as("notStartsWith(abcdefg, truncate(abcdefg, 7)) => false")
        .isFalse();

    assertThat(evaluator.eval(TestHelpers.Row.of("ab")))
        .as("notStartsWith(abcdefg, truncate(ab, 2)) => true")
        .isTrue();

    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
        .as("notStartsWith(abcdefg, truncate(a, 16)) => true")
        .isTrue();
  }

  @Test
  public void testStrictMetricsEvaluatorForNotStartsWith() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, notStartsWith(COLUMN, "bbb")).eval(FILE_1);
    assertThat(shouldRead)
        .as("Should not match: strict metrics eval is always false for notStartsWith")
        .isFalse();
  }

  @Test
  public void testInclusiveMetricsEvaluatorForNotStartsWith() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith(COLUMN, "aaa")).eval(FILE_1);
    assertThat(shouldRead).as("Should match: some columns meet the filter criteria").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith(COLUMN, "b")).eval(FILE_1);
    assertThat(shouldRead).as("Should not match: no columns match the filter criteria").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith(COLUMN, "bb")).eval(FILE_1);
    assertThat(shouldRead).as("Should not match: no columns match the filter criteria").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith(COLUMN, "bbb")).eval(FILE_1);
    assertThat(shouldRead).as("Should not match: no columns match the filter criteria").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith(COLUMN, "bbbb")).eval(FILE_1);
    assertThat(shouldRead).as("Should match: some columns match the filter criteria").isTrue();
  }

  private void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      String expectedLiteral,
      Expression.Operation expectedOp) {
    Expression projection = Projections.inclusive(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  private void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      String expectedLiteral,
      Expression.Operation expectedOp) {
    Expression projection = Projections.strict(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  @SuppressWarnings("unchecked")
  private void assertProjection(
      PartitionSpec spec,
      String expectedLiteral,
      Expression projection,
      Expression.Operation expectedOp) {
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);
    Literal<?> literal = predicate.literal();
    Truncate<CharSequence> transform =
        (Truncate<CharSequence>) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString(Types.StringType.get(), (String) literal.value());

    assertThat(predicate.op()).isEqualTo(expectedOp);
    assertThat(output).isEqualTo(expectedLiteral);
  }
}
