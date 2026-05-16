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
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.False;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStartsWith {

  private static final String COLUMN = "someStringCol";
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN, Types.StringType.get()));

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    assertProjectionInclusive(
        spec, startsWith(COLUMN, "ab"), "ab", Expression.Operation.STARTS_WITH);
    assertProjectionInclusive(spec, startsWith(COLUMN, "abab"), "abab", Expression.Operation.EQ);
    assertProjectionInclusive(
        spec, startsWith(COLUMN, "ababab"), "abab", Expression.Operation.STARTS_WITH);

    assertProjectionStrict(spec, startsWith(COLUMN, "ab"), "ab", Expression.Operation.STARTS_WITH);
    assertProjectionStrict(spec, startsWith(COLUMN, "abab"), "abab", Expression.Operation.EQ);

    Expression projection = Projections.strict(spec).project(startsWith(COLUMN, "ababab"));
    assertThat(projection).isInstanceOf(False.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTruncateString() {
    Truncate<String> trunc = Truncate.get(2);
    Expression expr = startsWith(COLUMN, "abcde");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.project(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    assertThat(evaluator.eval(TestHelpers.Row.of("abcdg")))
        .as("startsWith(abcde, truncate(abcdg,2))  => true")
        .isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTruncateEqualityRewrite() {
    // len(value) == width: truncate(col, 4) == "abab" is rewritten to col startsWith "abab"
    BoundPredicate<String> prefix =
        (BoundPredicate<String>)
            Binder.bind(SCHEMA.asStruct(), equal(truncate(COLUMN, 4), "abab"), false);
    assertThat(prefix.op()).isEqualTo(Expression.Operation.STARTS_WITH);
    assertThat(prefix.term()).isInstanceOf(BoundReference.class);

    Evaluator prefixEval = new Evaluator(SCHEMA.asStruct(), equal(truncate(COLUMN, 4), "abab"));
    assertThat(prefixEval.eval(TestHelpers.Row.of("ababXYZ")))
        .as("truncate(ababXYZ,4) == abab")
        .isTrue();
    assertThat(prefixEval.eval(TestHelpers.Row.of("abab"))).as("truncate(abab,4) == abab").isTrue();
    assertThat(prefixEval.eval(TestHelpers.Row.of("abXX")))
        .as("truncate(abXX,4) != abab")
        .isFalse();

    // len(value) < width: truncate(col, 6) == "abc" is rewritten to col == "abc" (exact),
    // which must NOT match longer strings the way startsWith would
    BoundPredicate<String> exact =
        (BoundPredicate<String>)
            Binder.bind(SCHEMA.asStruct(), equal(truncate(COLUMN, 6), "abc"), false);
    assertThat(exact.op()).isEqualTo(Expression.Operation.EQ);
    assertThat(exact.term()).isInstanceOf(BoundReference.class);

    Evaluator exactEval = new Evaluator(SCHEMA.asStruct(), equal(truncate(COLUMN, 6), "abc"));
    assertThat(exactEval.eval(TestHelpers.Row.of("abc"))).as("truncate(abc,6) == abc").isTrue();
    assertThat(exactEval.eval(TestHelpers.Row.of("abcd")))
        .as("truncate(abcd,6) != abc (exact, not prefix)")
        .isFalse();

    // len(value) > width: truncate(col, 2) == "abcde" can never match
    Expression none = Binder.bind(SCHEMA.asStruct(), equal(truncate(COLUMN, 2), "abcde"), false);
    assertThat(none).isInstanceOf(False.class);
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
