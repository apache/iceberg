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
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;

public class TransformTestHelpers {

  private TransformTestHelpers() {}

  public static void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      String expectedLiteral,
      Expression.Operation expectedOp) {
    Expression projection = Projections.inclusive(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  public static void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      String expectedLiteral,
      Expression.Operation expectedOp) {
    Expression projection = Projections.strict(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  @SuppressWarnings("unchecked")
  public static void assertProjection(
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
