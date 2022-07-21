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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StringType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.notEndsWith;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestNotEndsWith {

  private static final String COLUMN = "someStringCol";
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN, StringType.get()));

  // All 50 rows have someStringCol = 'bbb', none are null (despite being optional).
  private static final DataFile FILE_1 = new TestDataFile("file_1.avro", Row.of(), 50,
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

    assertProjectionInclusive(spec, notEndsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_ENDS_WITH);
    assertProjectionInclusive(spec, notEndsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    // When literal is longer than partition spec's truncation width, we always read for an inclusive projection
    // when using notStartsWith.
    Expression projection = Projections.inclusive(spec).project(notEndsWith(COLUMN, "ababab"));
    Assert.assertTrue(projection instanceof True);

    assertProjectionStrict(spec, notEndsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_ENDS_WITH);
    assertProjectionStrict(spec, notEndsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    assertProjectionStrict(spec, notEndsWith(COLUMN, "ababab"), "abab", Expression.Operation.NOT_ENDS_WITH);
    assertProjectionStrict(spec, notEndsWith(COLUMN, "abcde"), "abcd", Expression.Operation.NOT_ENDS_WITH);
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsLongerThanWidth() {
    Truncate<String> trunc = Truncate.get(StringType.get(), 2);
    UnboundPredicate<String> expr = notEndsWith(COLUMN, "abcde");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    Assert.assertEquals("The projected literal should be truncated to the truncation width",
        projected.literal().value(), "ab");

    Assert.assertFalse("notEndsWith(abcde, truncate(edcab,2)) => false",
        evaluator.eval(Row.of("edcab")));

    Assert.assertFalse("notEndsWith(abcde, truncate(ab, 2)) => false",
        evaluator.eval(Row.of("ab")));

    Assert.assertTrue("notEndsWith(abcde, truncate(de, 2)) => true",
        evaluator.eval(Row.of("de")));

    Assert.assertTrue("notEndsWith(abcde, truncate(e, 2)) => true",
        evaluator.eval(Row.of("e")));

    Assert.assertTrue("notEndsWith(abcde, truncate(edcabz, 2)) => true",
        evaluator.eval(Row.of("edcabz")));
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsShorterThanWidth() {
    Truncate<String> trunc = Truncate.get(StringType.get(), 16);
    UnboundPredicate<String> expr = notEndsWith(COLUMN, "ab");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    Assert.assertEquals("The projected literal should not be truncated as its size is shorter than truncation width",
        projected.literal().value(), "ab");

    Assert.assertTrue("notEndsWith(ab, truncate(abcde, 16)) => true",
        evaluator.eval(Row.of("abcde")));

    Assert.assertFalse("notEndsWith(ab, truncate(ab, 16)) => false",
        evaluator.eval(Row.of("ab")));

    Assert.assertTrue("notEndsWith(ab, truncate(a, 16)) => true",
        evaluator.eval(Row.of("a")));
  }

  @Test
  public void testTruncateStringWhenProjectedPredicateTermIsEqualToWidth() {
    Truncate<String> trunc = Truncate.get(StringType.get(), 7);
    UnboundPredicate<String> expr = notEndsWith(COLUMN, "abcdefg");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);

    UnboundPredicate<String> projected = trunc.projectStrict(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    Assert.assertEquals("The projected literal should not be truncated as its size is equal to truncation width",
        projected.literal().value(), "abcdefg");

    Assert.assertFalse("notEndsWith(abcdefg, truncate(abcdefg, 7)) => false",
        evaluator.eval(Row.of("abcdefg")));

    Assert.assertTrue("notEndsWith(abcdefg, truncate(ab, 2)) => true",
        evaluator.eval(Row.of("ab")));

    Assert.assertTrue("notEndsWith(abcdefg, truncate(a, 16)) => true",
        evaluator.eval(Row.of("a")));
  }

  @Test
  public void testStrictMetricsEvaluatorForNotStartsWith() {
    boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, notEndsWith(COLUMN, "bbb")).eval(FILE_1);
    Assert.assertFalse("Should not match: strict metrics eval is always false for notEndsWith", shouldRead);
  }

  private void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter,
                                       String expectedLiteral, Expression.Operation expectedOp) {
    Expression projection = Projections.inclusive(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  private void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter,
                                    String expectedLiteral, Expression.Operation expectedOp) {
    Expression projection = Projections.strict(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  private void assertProjection(PartitionSpec spec, String expectedLiteral, Expression projection,
                              Expression.Operation expectedOp) {
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);
    Literal<?> literal = predicate.literal();
    Truncate<CharSequence> transform = (Truncate<CharSequence>) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((String) literal.value());

    Assert.assertEquals(expectedOp, predicate.op());
    Assert.assertEquals(expectedLiteral, output);
  }
}
