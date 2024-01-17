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
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

public abstract class BaseDatesProjection {
  protected static final Types.DateType TYPE = Types.DateType.get();
  protected static final Schema SCHEMA = new Schema(optional(1, "date", TYPE));

  @SuppressWarnings("unchecked")
  public void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<Integer> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Assert.assertNotEquals(
        "Strict projection never runs for IN", Expression.Operation.IN, predicate.op());

    Transform<?, Integer> transform =
        (Transform<?, Integer>) spec.getFieldsBySourceId(1).get(0).transform();
    Type type = spec.partitionType().field(spec.getFieldsBySourceId(1).get(0).fieldId()).type();
    if (predicate.op() == Expression.Operation.NOT_IN) {
      Iterable<Integer> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(v -> transform.toHumanString(type, v))
              .collect(Collectors.toList())
              .toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal<Integer> literal = predicate.literal();
      String output = transform.toHumanString(type, literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }

  public void assertProjectionStrictValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {

    Expression projection = Projections.strict(spec).project(filter);
    Assert.assertEquals(expectedOp, projection.op());
  }

  public void assertProjectionInclusiveValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {

    Expression projection = Projections.inclusive(spec).project(filter);
    Assert.assertEquals(expectedOp, projection.op());
  }

  @SuppressWarnings("unchecked")
  public void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<Integer> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Assert.assertNotEquals(
        "Inclusive projection never runs for NOT_IN", Expression.Operation.NOT_IN, predicate.op());

    Transform<?, Integer> transform =
        (Transform<?, Integer>) spec.getFieldsBySourceId(1).get(0).transform();
    Type type = spec.partitionType().field(spec.getFieldsBySourceId(1).get(0).fieldId()).type();
    if (predicate.op() == Expression.Operation.IN
        || predicate.op() == Expression.Operation.RANGE_IN) {
      Iterable<Integer> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(v -> transform.toHumanString(type, v))
              .collect(Collectors.toList())
              .toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal<Integer> literal = predicate.literal();
      String output = transform.toHumanString(type, literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }
}
