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

import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestAggregateBinding {
  private static final List<UnboundAggregate<Integer>> list =
      ImmutableList.of(Expressions.count("x"), Expressions.max("x"), Expressions.min("x"));
  private static final StructType struct =
      StructType.of(Types.NestedField.required(10, "x", Types.IntegerType.get()));

  @Test
  public void testAggregateBinding() {
    for (UnboundAggregate<Integer> unbound : list) {
      Expression expr = unbound.bind(struct, true);
      BoundAggregate<Integer, ?> bound = assertAndUnwrapAggregate(expr);
      Assert.assertEquals("Should reference correct field ID", 10, bound.ref().fieldId());
      Assert.assertEquals("Should not change the comparison operation", unbound.op(), bound.op());
    }
  }

  @Test
  public void testCountStarBinding() {
    UnboundAggregate<?> unbound = Expressions.countStar();
    Expression expr = unbound.bind(null, false);
    BoundAggregate<?, Long> bound = assertAndUnwrapAggregate(expr);

    Assert.assertEquals(
        "Should not change the comparison operation", Expression.Operation.COUNT_STAR, bound.op());
  }

  @Test
  public void testBoundAggregateFails() {
    Expression unbound = Expressions.count("x");
    Assertions.assertThatThrownBy(() -> Binder.bind(struct, Binder.bind(struct, unbound)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Found already bound aggregate");
  }

  @Test
  public void testCaseInsensitiveReference() {
    Expression expr = Expressions.max("X");
    Expression boundExpr = Binder.bind(struct, expr, false);
    BoundAggregate<Integer, Integer> bound = assertAndUnwrapAggregate(boundExpr);
    Assert.assertEquals("Should reference correct field ID", 10, bound.ref().fieldId());
    Assert.assertEquals(
        "Should not change the comparison operation", Expression.Operation.MAX, bound.op());
  }

  @Test
  public void testCaseSensitiveReference() {
    Expression expr = Expressions.max("X");
    Assertions.assertThatThrownBy(() -> Binder.bind(struct, expr, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'X' in struct");
  }

  @Test
  public void testMissingField() {
    UnboundAggregate<?> unbound = Expressions.count("missing");
    Assertions.assertThatThrownBy(() -> unbound.bind(struct, false))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing' in struct:");
  }

  private static <T, C> BoundAggregate<T, C> assertAndUnwrapAggregate(Expression expr) {
    Assertions.assertThat(expr).isInstanceOf(BoundAggregate.class);
    return (BoundAggregate<T, C>) expr;
  }
}
