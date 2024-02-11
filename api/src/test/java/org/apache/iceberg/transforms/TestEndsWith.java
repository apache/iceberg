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

import static org.apache.iceberg.expressions.Expressions.endsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.False;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEndsWith {

  private static final String COLUMN = "someStringCol";
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN, Types.StringType.get()));

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    TransformTestHelpers.assertProjectionInclusive(
        spec, endsWith(COLUMN, "ab"), "ab", Expression.Operation.ENDS_WITH);
    TransformTestHelpers.assertProjectionInclusive(
        spec, endsWith(COLUMN, "abab"), "abab", Expression.Operation.EQ);
    // It will truncate the first 4 characters and then compare from the end.
    TransformTestHelpers.assertProjectionInclusive(
        spec, endsWith(COLUMN, "abcdab"), "abcd", Expression.Operation.ENDS_WITH);

    TransformTestHelpers.assertProjectionStrict(
        spec, endsWith(COLUMN, "ab"), "ab", Expression.Operation.ENDS_WITH);
    TransformTestHelpers.assertProjectionStrict(
        spec, endsWith(COLUMN, "abab"), "abab", Expression.Operation.EQ);

    Expression projection = Projections.strict(spec).project(endsWith(COLUMN, "abcdab"));
    Assertions.assertThat(projection).isInstanceOf(False.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEvaluatorTruncateString() {
    Truncate<String> trunc = Truncate.get(2);
    Expression expr = endsWith(COLUMN, "degf");
    BoundPredicate<String> boundExpr =
        (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(), expr, false);

    UnboundPredicate<String> projected = trunc.project(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);
    // It will truncate the first 2 characters and then compare from the end.
    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
        .as("endsWith(abcde, truncate(degf,2))  => true")
        .isTrue();
  }
}
