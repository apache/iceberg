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

import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;

public class BaseBucketingProjection {

  public void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Assert.assertNotEquals(
        "Strict projection never runs for IN", Expression.Operation.IN, predicate.op());

    if (predicate.op() == Expression.Operation.NOT_IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(String::valueOf)
              .collect(Collectors.toList())
              .toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal<?> literal = predicate.literal();
      String output = String.valueOf(literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }

  public void assertProjectionStrictValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {
    Expression projection = Projections.strict(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusiveValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {
    Expression projection = Projections.inclusive(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Assert.assertNotEquals(
        "Inclusive projection never runs for NOT_IN", Expression.Operation.NOT_IN, predicate.op());

    if (predicate.op() == Expression.Operation.IN
        || predicate.op() == Expression.Operation.RANGE_IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(String::valueOf)
              .collect(Collectors.toList())
              .toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal<?> literal = predicate.literal();
      String output = String.valueOf(literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }
}
