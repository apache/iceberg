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

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static com.netflix.iceberg.expressions.Expression.Operation.EQ;
import static com.netflix.iceberg.expressions.Expression.Operation.GT;
import static com.netflix.iceberg.expressions.Expression.Operation.GT_EQ;
import static com.netflix.iceberg.expressions.Expression.Operation.IS_NULL;
import static com.netflix.iceberg.expressions.Expression.Operation.LT;
import static com.netflix.iceberg.expressions.Expression.Operation.LT_EQ;
import static com.netflix.iceberg.expressions.Expression.Operation.NOT_EQ;
import static com.netflix.iceberg.expressions.Expression.Operation.NOT_NULL;
import static com.netflix.iceberg.expressions.Expressions.ref;
import static com.netflix.iceberg.TestHelpers.assertAndUnwrap;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

public class TestPredicateBinding {
  private static List<Expression.Operation> COMPARISONS = Arrays.asList(
      LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ);

  @Test
  @SuppressWarnings("unchecked")
  public void testMultipleFields() {
    StructType struct = StructType.of(
        required(10, "x", Types.IntegerType.get()),
        required(11, "y", Types.IntegerType.get()),
        required(12, "z", Types.IntegerType.get())
    );

    UnboundPredicate<Integer> unbound = new UnboundPredicate<>(LT, ref("y"), 6);

    Expression expr = unbound.bind(struct);
    BoundPredicate<Integer> bound = assertAndUnwrap(expr);

    Assert.assertEquals("Should reference correct field ID", 11, bound.ref().fieldId());
    Assert.assertEquals("Should not change the comparison operation", LT, bound.op());
    Assert.assertEquals("Should not alter literal value",
        Integer.valueOf(6), bound.literal().value());
  }

  @Test
  public void testMissingField() {
    StructType struct = StructType.of(
        required(13, "x", Types.IntegerType.get())
    );

    UnboundPredicate<Integer> unbound = new UnboundPredicate<>(LT, ref("missing"), 6);
    try {
      unbound.bind(struct);
      Assert.fail("Binding a missing field should fail");
    } catch (ValidationException e) {
      Assert.assertTrue("Validation should complain about missing field",
          e.getMessage().contains("Cannot find field 'missing' in struct:"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComparisonPredicateBinding() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    for (Expression.Operation op : COMPARISONS) {
      UnboundPredicate<Integer> unbound = new UnboundPredicate<>(op, ref("x"), 5);

      Expression expr = unbound.bind(struct);
      BoundPredicate<Integer> bound = assertAndUnwrap(expr);

      Assert.assertEquals("Should not alter literal value",
          Integer.valueOf(5), bound.literal().value());
      Assert.assertEquals("Should reference correct field ID", 14, bound.ref().fieldId());
      Assert.assertEquals("Should not change the comparison operation", op, bound.op());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLiteralConversion() {
    StructType struct = StructType.of(required(15, "d", Types.DecimalType.of(9, 2)));

    for (Expression.Operation op : COMPARISONS) {
      UnboundPredicate<String> unbound = new UnboundPredicate<>(op, ref("d"), "12.40");

      Expression expr = unbound.bind(struct);
      BoundPredicate<BigDecimal> bound = assertAndUnwrap(expr);
      Assert.assertEquals("Should convert literal value to decimal",
          new BigDecimal("12.40"), bound.literal().value());
      Assert.assertEquals("Should reference correct field ID", 15, bound.ref().fieldId());
      Assert.assertEquals("Should not change the comparison operation", op, bound.op());
    }
  }

  @Test
  public void testInvalidConversions() {
    StructType struct = StructType.of(required(16, "f", Types.FloatType.get()));

    for (Expression.Operation op : COMPARISONS) {
      UnboundPredicate<String> unbound = new UnboundPredicate<>(op, ref("f"), "12.40");

      try {
        unbound.bind(struct);
        Assert.fail("Should not convert string to float");
      } catch (ValidationException e) {
        Assert.assertEquals("Should ",
            e.getMessage(),
            "Invalid value for comparison inclusive type float: 12.40 (java.lang.String)");
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLongToIntegerConversion() {
    StructType struct = StructType.of(required(17, "i", Types.IntegerType.get()));

    UnboundPredicate<Long> lt = new UnboundPredicate<>(
        LT, ref("i"), (long) Integer.MAX_VALUE + 1L);
    Assert.assertEquals("Less than above max should be alwaysTrue",
        Expressions.alwaysTrue(), lt.bind(struct));

    UnboundPredicate<Long> lteq = new UnboundPredicate<>(
        LT_EQ, ref("i"), (long) Integer.MAX_VALUE + 1L);
    Assert.assertEquals("Less than or equal above max should be alwaysTrue",
        Expressions.alwaysTrue(), lteq.bind(struct));

    UnboundPredicate<Long> gt = new UnboundPredicate<>(
        GT, ref("i"), (long) Integer.MIN_VALUE - 1L);
    Assert.assertEquals("Greater than below min should be alwaysTrue",
        Expressions.alwaysTrue(), gt.bind(struct));

    UnboundPredicate<Long> gteq = new UnboundPredicate<>(
        GT_EQ, ref("i"), (long) Integer.MIN_VALUE - 1L);
    Assert.assertEquals("Greater than or equal below min should be alwaysTrue",
        Expressions.alwaysTrue(), gteq.bind(struct));

    UnboundPredicate<Long> gtMax = new UnboundPredicate<>(
        GT, ref("i"), (long) Integer.MAX_VALUE + 1L);
    Assert.assertEquals("Greater than above max should be alwaysFalse",
        Expressions.alwaysFalse(), gtMax.bind(struct));

    UnboundPredicate<Long> gteqMax = new UnboundPredicate<>(
        GT_EQ, ref("i"), (long) Integer.MAX_VALUE + 1L);
    Assert.assertEquals("Greater than or equal above max should be alwaysFalse",
        Expressions.alwaysFalse(), gteqMax.bind(struct));

    UnboundPredicate<Long> ltMin = new UnboundPredicate<>(
        LT, ref("i"), (long) Integer.MIN_VALUE - 1L);
    Assert.assertEquals("Less than below min should be alwaysFalse",
        Expressions.alwaysFalse(), ltMin.bind(struct));

    UnboundPredicate<Long> lteqMin = new UnboundPredicate<>(
        LT_EQ, ref("i"), (long) Integer.MIN_VALUE - 1L);
    Assert.assertEquals("Less than or equal below min should be alwaysFalse",
        Expressions.alwaysFalse(), lteqMin.bind(struct));

    Expression ltExpr = new UnboundPredicate<>(LT, ref("i"), (long) Integer.MAX_VALUE).bind(struct, true);
    BoundPredicate<Integer> ltMax = assertAndUnwrap(ltExpr);
    Assert.assertEquals("Should translate bound to Integer",
        (Integer) Integer.MAX_VALUE, ltMax.literal().value());

    Expression lteqExpr = new UnboundPredicate<>(LT_EQ, ref("i"), (long) Integer.MAX_VALUE)
        .bind(struct);
    BoundPredicate<Integer> lteqMax = assertAndUnwrap(lteqExpr);
    Assert.assertEquals("Should translate bound to Integer",
        (Integer) Integer.MAX_VALUE, lteqMax.literal().value());

    Expression gtExpr = new UnboundPredicate<>(GT, ref("i"), (long) Integer.MIN_VALUE).bind(struct);
    BoundPredicate<Integer> gtMin = assertAndUnwrap(gtExpr);
    Assert.assertEquals("Should translate bound to Integer",
        (Integer) Integer.MIN_VALUE, gtMin.literal().value());

    Expression gteqExpr = new UnboundPredicate<>(GT_EQ, ref("i"), (long) Integer.MIN_VALUE)
        .bind(struct);
    BoundPredicate<Integer> gteqMin = assertAndUnwrap(gteqExpr);
    Assert.assertEquals("Should translate bound to Integer",
        (Integer) Integer.MIN_VALUE, gteqMin.literal().value());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDoubleToFloatConversion() {
    StructType struct = StructType.of(required(18, "f", Types.FloatType.get()));

    UnboundPredicate<Double> lt = new UnboundPredicate<>(
        LT, ref("f"), (double) Float.MAX_VALUE * 2);
    Assert.assertEquals("Less than above max should be alwaysTrue",
        Expressions.alwaysTrue(), lt.bind(struct));

    UnboundPredicate<Double> lteq = new UnboundPredicate<>(
        LT_EQ, ref("f"), (double) Float.MAX_VALUE * 2);
    Assert.assertEquals("Less than or equal above max should be alwaysTrue",
        Expressions.alwaysTrue(), lteq.bind(struct));

    UnboundPredicate<Double> gt = new UnboundPredicate<>(
        GT, ref("f"), (double) Float.MAX_VALUE * -2);
    Assert.assertEquals("Greater than below min should be alwaysTrue",
        Expressions.alwaysTrue(), gt.bind(struct));

    UnboundPredicate<Double> gteq = new UnboundPredicate<>(
        GT_EQ, ref("f"), (double) Float.MAX_VALUE * -2);
    Assert.assertEquals("Greater than or equal below min should be alwaysTrue",
        Expressions.alwaysTrue(), gteq.bind(struct));

    UnboundPredicate<Double> gtMax = new UnboundPredicate<>(
        GT, ref("f"), (double) Float.MAX_VALUE * 2);
    Assert.assertEquals("Greater than above max should be alwaysFalse",
        Expressions.alwaysFalse(), gtMax.bind(struct));

    UnboundPredicate<Double> gteqMax = new UnboundPredicate<>(
        GT_EQ, ref("f"), (double) Float.MAX_VALUE * 2);
    Assert.assertEquals("Greater than or equal above max should be alwaysFalse",
        Expressions.alwaysFalse(), gteqMax.bind(struct));

    UnboundPredicate<Double> ltMin = new UnboundPredicate<>(
        LT, ref("f"), (double) Float.MAX_VALUE * -2);
    Assert.assertEquals("Less than below min should be alwaysFalse",
        Expressions.alwaysFalse(), ltMin.bind(struct));

    UnboundPredicate<Double> lteqMin = new UnboundPredicate<>(
        LT_EQ, ref("f"), (double) Float.MAX_VALUE * -2);
    Assert.assertEquals("Less than or equal below min should be alwaysFalse",
        Expressions.alwaysFalse(), lteqMin.bind(struct));

    Expression ltExpr = new UnboundPredicate<>(LT, ref("f"), (double) Float.MAX_VALUE).bind(struct);
    BoundPredicate<Float> ltMax = assertAndUnwrap(ltExpr);
    Assert.assertEquals("Should translate bound to Float",
        (Float) Float.MAX_VALUE, ltMax.literal().value());

    Expression lteqExpr = new UnboundPredicate<>(LT_EQ, ref("f"), (double) Float.MAX_VALUE)
        .bind(struct);
    BoundPredicate<Float> lteqMax = assertAndUnwrap(lteqExpr);
    Assert.assertEquals("Should translate bound to Float",
        (Float) Float.MAX_VALUE, lteqMax.literal().value());

    Expression gtExpr = new UnboundPredicate<>(GT, ref("f"), (double) -Float.MAX_VALUE).bind(struct);
    BoundPredicate<Float> gtMin = assertAndUnwrap(gtExpr);
    Assert.assertEquals("Should translate bound to Float",
        Float.valueOf(-Float.MAX_VALUE), gtMin.literal().value());

    Expression gteqExpr = new UnboundPredicate<>(GT_EQ, ref("f"), (double) -Float.MAX_VALUE)
        .bind(struct);
    BoundPredicate<Float> gteqMin = assertAndUnwrap(gteqExpr);
    Assert.assertEquals("Should translate bound to Float",
        Float.valueOf(-Float.MAX_VALUE), gteqMin.literal().value());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIsNull() {
    StructType optional = StructType.of(optional(19, "s", Types.StringType.get()));

    UnboundPredicate<?> unbound = new UnboundPredicate<>(IS_NULL, ref("s"));
    Expression expr = unbound.bind(optional);
    BoundPredicate<?> bound = assertAndUnwrap(expr);
    Assert.assertEquals("Should use the same operation", IS_NULL, bound.op());
    Assert.assertEquals("Should use the correct field", 19, bound.ref().fieldId());
    Assert.assertNull("Should not have a literal value", bound.literal());

    StructType required = StructType.of(required(20, "s", Types.StringType.get()));
    Assert.assertEquals("IsNull inclusive a required field should be alwaysFalse",
        Expressions.alwaysFalse(), unbound.bind(required));
  }

  @Test
  public void testNotNull() {
    StructType optional = StructType.of(optional(21, "s", Types.StringType.get()));

    UnboundPredicate<?> unbound = new UnboundPredicate<>(NOT_NULL, ref("s"));
    Expression expr = unbound.bind(optional);
    BoundPredicate<?> bound = assertAndUnwrap(expr);
    Assert.assertEquals("Should use the same operation", NOT_NULL, bound.op());
    Assert.assertEquals("Should use the correct field", 21, bound.ref().fieldId());
    Assert.assertNull("Should not have a literal value", bound.literal());

    StructType required = StructType.of(required(22, "s", Types.StringType.get()));
    Assert.assertEquals("NotNull inclusive a required field should be alwaysTrue",
        Expressions.alwaysTrue(), unbound.bind(required));
  }
}
