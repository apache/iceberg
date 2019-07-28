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
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestStartsWith {

  private static final Schema SCHEMA = new Schema(optional(1, "someStringCol", Types.StringType.get()));

  @Test
  public void assertTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate("someStringCol", 4).build();

    assertProjectionInclusive(spec, startsWith("someStringCol", "ab"), "ab");
    assertProjectionInclusive(spec, startsWith("someStringCol", "abab"), "abab");
    assertProjectionInclusive(spec, startsWith("someStringCol", "ababab"), "abab");

    assertProjectionStrict(spec, startsWith("someStringCol", "ab"), "ab");
    assertProjectionStrict(spec, startsWith("someStringCol", "abab"), "abab");
  }

  @Test
  public void assertTruncateString() {
    Types.StructType struct = Types.StructType.of(required(0, "s", Types.StringType.get()));
    Truncate<String> trunc = Truncate.get(Types.StringType.get(), 2);
    Expression expr = startsWith("s", "abcde");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(struct,  expr, false);

    UnboundPredicate<String> projected = trunc.project("s", boundExpr);
    Evaluator evaluator = new Evaluator(struct, projected);

    Assert.assertTrue("startsWith(abcde, truncate(abcde,2))  => true",
        evaluator.eval(TestHelpers.Row.of("abcde")));
  }

  private void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter,
                                         String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection);
  }

  private void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter,
                                         String expectedLiteral) {
    Expression projection = Projections.strict(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection);
  }

  private void assertProjection(PartitionSpec spec, String expectedLiteral, Expression projection) {
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), Expression.Operation.STARTS_WITH);

    Literal literal = predicate.literal();
    Truncate<CharSequence> transform = (Truncate<CharSequence>) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((String) literal.value());
    Assert.assertEquals(expectedLiteral, output);
  }
}
