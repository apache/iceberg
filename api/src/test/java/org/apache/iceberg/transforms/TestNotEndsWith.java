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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestNotEndsWith {

  private static final String COLUMN = "someStringCol";
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN, Types.StringType.get()));

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    TransformTestHelpers.assertProjectionInclusive(
        spec, notEndsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_ENDS_WITH);
    TransformTestHelpers.assertProjectionInclusive(
        spec, notEndsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    // When literal is longer than partition spec's truncation width, we always read for an
    // inclusive projection when using notStartsWith.
    Expression projection = Projections.inclusive(spec).project(notEndsWith(COLUMN, "ababab"));
    assertThat(projection).isInstanceOf(True.class);

    TransformTestHelpers.assertProjectionStrict(
        spec, notEndsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_ENDS_WITH);
    TransformTestHelpers.assertProjectionStrict(
        spec, notEndsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);
    TransformTestHelpers.assertProjectionStrict(
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
        .as("notEndsWith(abcde, truncate(cdg,2))  => true")
        .isTrue();
  }
}
