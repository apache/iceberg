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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.expressions.Expression.Operation.COUNT;
import static org.apache.iceberg.expressions.Expression.Operation.COUNT_STAR;
import static org.apache.iceberg.expressions.Expression.Operation.MAX;
import static org.apache.iceberg.expressions.Expression.Operation.MIN;
import static org.apache.iceberg.expressions.Expressions.max;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestAggregateBinding {
  private static final List<Expression.Operation> AGGREGATES = Arrays.asList(COUNT, MAX, MIN);
  private static final StructType struct =
      StructType.of(required(10, "x", Types.IntegerType.get()));

  @Test
  @SuppressWarnings("unchecked")
  public void testAggregateBinding() {
    for (Expression.Operation op : AGGREGATES) {
      UnboundAggregate unbound = new UnboundAggregate(op, ref("x"));

      Expression expr = unbound.bind(struct, false);
      BoundAggregate bound = assertAndUnwrapAggregate(expr);

      Assert.assertEquals("Should reference correct field ID", 10, bound.ref().fieldId());
      Assert.assertEquals("Should not change the comparison operation", op, bound.op());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCountStarBinding() {
    UnboundAggregate unbound = new UnboundAggregate(COUNT_STAR, null);
    Expression expr = unbound.bind(null, false);
    BoundAggregate bound = assertAndUnwrapAggregate(expr);

    Assert.assertEquals("Should not change the comparison operation", COUNT_STAR, bound.op());
  }

  @Test
  public void testBoundAggregateFails() {
    UnboundAggregate unbound = new UnboundAggregate(COUNT, ref("x"));
    Assertions.assertThatThrownBy(() -> Binder.bind(struct, Binder.bind(struct, unbound)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Found already bound aggregate");
  }

  @Test
  public void testCaseInsensitiveReference() {
    Expression expr = max("X");
    Expression boundExpr = Binder.bind(struct, expr, false);
    BoundAggregate bound = assertAndUnwrapAggregate(boundExpr);
    Assert.assertEquals("Should reference correct field ID", 10, bound.ref().fieldId());
    Assert.assertEquals("Should not change the comparison operation", MAX, bound.op());
  }

  @Test
  public void testCaseSensitiveReference() {
    Expression expr = max("X");
    Assertions.assertThatThrownBy(() -> Binder.bind(struct, expr, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'X' in struct");
  }

  @Test
  public void testMissingField() {
    UnboundAggregate unbound = new UnboundAggregate(COUNT, ref("missing"));
    try {
      unbound.bind(struct, false);
      Assert.fail("Binding a missing field should fail");
    } catch (ValidationException e) {
      Assert.assertTrue(
          "Validation should complain about missing field",
          e.getMessage().contains("Cannot find field 'missing' in struct:"));
    }
  }

  private static <T, C> BoundAggregate<T, C> assertAndUnwrapAggregate(Expression expr) {
    Assert.assertTrue(
        "Expression should be a bound aggregate: " + expr, expr instanceof BoundAggregate);
    return (BoundAggregate<T, C>) expr;
  }
}
