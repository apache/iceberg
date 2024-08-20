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

import static org.apache.iceberg.TestHelpers.assertAndUnwrap;
import static org.apache.iceberg.TestHelpers.assertAndUnwrapBoundSet;
import static org.apache.iceberg.expressions.Expression.Operation.EQ;
import static org.apache.iceberg.expressions.Expression.Operation.GT;
import static org.apache.iceberg.expressions.Expression.Operation.GT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.IN;
import static org.apache.iceberg.expressions.Expression.Operation.IS_NAN;
import static org.apache.iceberg.expressions.Expression.Operation.IS_NULL;
import static org.apache.iceberg.expressions.Expression.Operation.LT;
import static org.apache.iceberg.expressions.Expression.Operation.LT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_IN;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_NAN;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_NULL;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_STARTS_WITH;
import static org.apache.iceberg.expressions.Expression.Operation.STARTS_WITH;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestPredicateBinding {
  private static final List<Expression.Operation> COMPARISONS =
      Arrays.asList(LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ);

  @Test
  public void testMultipleFields() {
    StructType struct =
        StructType.of(
            required(10, "x", Types.IntegerType.get()),
            required(11, "y", Types.IntegerType.get()),
            required(12, "z", Types.IntegerType.get()));

    UnboundPredicate<Integer> unbound = new UnboundPredicate<>(LT, ref("y"), 6);

    Expression expr = unbound.bind(struct);
    BoundPredicate<Integer> bound = assertAndUnwrap(expr);

    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(11);
    assertThat(bound.op()).as("Should not change the comparison operation").isEqualTo(LT);
    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should not alter literal value")
        .isEqualTo(6);
  }

  @Test
  public void testMissingField() {
    StructType struct = StructType.of(required(13, "x", Types.IntegerType.get()));

    UnboundPredicate<Integer> unbound = new UnboundPredicate<>(LT, ref("missing"), 6);
    assertThatThrownBy(() -> unbound.bind(struct))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing' in struct:");
  }

  @Test
  public void testComparisonPredicateBinding() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    for (Expression.Operation op : COMPARISONS) {
      UnboundPredicate<Integer> unbound = new UnboundPredicate<>(op, ref("x"), 5);

      Expression expr = unbound.bind(struct);
      BoundPredicate<Integer> bound = assertAndUnwrap(expr);

      assertThat(bound.isLiteralPredicate()).isTrue();
      assertThat(bound.asLiteralPredicate().literal().value())
          .as("Should not alter literal value")
          .isEqualTo(5);
      assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(14);
      assertThat(bound.op()).as("Should not change the comparison operation").isEqualTo(op);
    }
  }

  @Test
  public void testPredicateBindingForStringPrefixComparisons() {
    StructType struct = StructType.of(required(17, "x", Types.StringType.get()));

    for (Expression.Operation op : Arrays.asList(STARTS_WITH, NOT_STARTS_WITH)) {
      UnboundPredicate<String> unbound = new UnboundPredicate<>(op, ref("x"), "s");

      Expression expr = unbound.bind(struct);
      BoundPredicate<Integer> bound = assertAndUnwrap(expr);

      assertThat(bound.isLiteralPredicate()).isTrue();
      assertThat(String.valueOf(bound.asLiteralPredicate().literal().value()))
          .as("Should not alter literal value")
          .isEqualTo("s");
      assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(17);
      assertThat(bound.op()).as("Should not change the comparison operation").isEqualTo(op);
    }
  }

  @Test
  public void testLiteralConversion() {
    StructType struct = StructType.of(required(15, "d", Types.DecimalType.of(9, 2)));

    for (Expression.Operation op : COMPARISONS) {
      UnboundPredicate<String> unbound = new UnboundPredicate<>(op, ref("d"), "12.40");

      Expression expr = unbound.bind(struct);
      BoundPredicate<BigDecimal> bound = assertAndUnwrap(expr);
      assertThat(bound.isLiteralPredicate()).isTrue();
      assertThat(bound.asLiteralPredicate().literal().value())
          .as("Should convert literal value to decimal")
          .isEqualTo(new BigDecimal("12.40"));
      assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(15);
      assertThat(bound.op()).as("Should not change the comparison operation").isEqualTo(op);
    }
  }

  @Test
  public void testInvalidConversions() {
    StructType struct = StructType.of(required(16, "f", Types.FloatType.get()));

    for (Expression.Operation op : COMPARISONS) {
      UnboundPredicate<String> unbound = new UnboundPredicate<>(op, ref("f"), "12.40");

      assertThatThrownBy(() -> unbound.bind(struct))
          .isInstanceOf(ValidationException.class)
          .hasMessage("Invalid value for conversion to type float: 12.40 (java.lang.String)");
    }
  }

  @Test
  public void testLongToIntegerConversion() {
    StructType struct = StructType.of(required(17, "i", Types.IntegerType.get()));

    UnboundPredicate<Long> lt = new UnboundPredicate<>(LT, ref("i"), (long) Integer.MAX_VALUE + 1L);
    assertThat(lt.bind(struct))
        .as("Less than above max should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Long> lteq =
        new UnboundPredicate<>(LT_EQ, ref("i"), (long) Integer.MAX_VALUE + 1L);
    assertThat(lteq.bind(struct))
        .as("Less than or equal above max should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Long> gt = new UnboundPredicate<>(GT, ref("i"), (long) Integer.MIN_VALUE - 1L);
    assertThat(gt.bind(struct))
        .as("Greater than below min should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Long> gteq =
        new UnboundPredicate<>(GT_EQ, ref("i"), (long) Integer.MIN_VALUE - 1L);
    assertThat(gteq.bind(struct))
        .as("Greater than or equal below min should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Long> gtMax =
        new UnboundPredicate<>(GT, ref("i"), (long) Integer.MAX_VALUE + 1L);
    assertThat(gtMax.bind(struct))
        .as("Greater than above max should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    UnboundPredicate<Long> gteqMax =
        new UnboundPredicate<>(GT_EQ, ref("i"), (long) Integer.MAX_VALUE + 1L);
    assertThat(gteqMax.bind(struct))
        .as("Greater than or equal above max should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    UnboundPredicate<Long> ltMin =
        new UnboundPredicate<>(LT, ref("i"), (long) Integer.MIN_VALUE - 1L);
    assertThat(ltMin.bind(struct))
        .as("Less than below min should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    UnboundPredicate<Long> lteqMin =
        new UnboundPredicate<>(LT_EQ, ref("i"), (long) Integer.MIN_VALUE - 1L);
    assertThat(lteqMin.bind(struct))
        .as("Less than or equal below min should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    Expression ltExpr =
        new UnboundPredicate<>(LT, ref("i"), (long) Integer.MAX_VALUE).bind(struct, true);
    BoundPredicate<Integer> ltMax = assertAndUnwrap(ltExpr);
    assertThat(ltMax.isLiteralPredicate()).isTrue();
    assertThat(ltMax.asLiteralPredicate().literal().value())
        .as("Should translate bound to Integer")
        .isEqualTo(Integer.MAX_VALUE);

    Expression lteqExpr =
        new UnboundPredicate<>(LT_EQ, ref("i"), (long) Integer.MAX_VALUE).bind(struct);
    BoundPredicate<Integer> lteqMax = assertAndUnwrap(lteqExpr);
    assertThat(lteqMax.isLiteralPredicate()).isTrue();
    assertThat(lteqMax.asLiteralPredicate().literal().value())
        .as("Should translate bound to Integer")
        .isEqualTo(Integer.MAX_VALUE);

    Expression gtExpr = new UnboundPredicate<>(GT, ref("i"), (long) Integer.MIN_VALUE).bind(struct);
    BoundPredicate<Integer> gtMin = assertAndUnwrap(gtExpr);
    assertThat(gtMin.isLiteralPredicate()).isTrue();
    assertThat(gtMin.asLiteralPredicate().literal().value())
        .as("Should translate bound to Integer")
        .isEqualTo(Integer.MIN_VALUE);

    Expression gteqExpr =
        new UnboundPredicate<>(GT_EQ, ref("i"), (long) Integer.MIN_VALUE).bind(struct);
    BoundPredicate<Integer> gteqMin = assertAndUnwrap(gteqExpr);
    assertThat(gteqMin.isLiteralPredicate()).isTrue();
    assertThat(gteqMin.asLiteralPredicate().literal().value())
        .as("Should translate bound to Integer")
        .isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void testDoubleToFloatConversion() {
    StructType struct = StructType.of(required(18, "f", Types.FloatType.get()));

    UnboundPredicate<Double> lt =
        new UnboundPredicate<>(LT, ref("f"), (double) Float.MAX_VALUE * 2);
    assertThat(lt.bind(struct))
        .as("Less than above max should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Double> lteq =
        new UnboundPredicate<>(LT_EQ, ref("f"), (double) Float.MAX_VALUE * 2);
    assertThat(lteq.bind(struct))
        .as("Less than or equal above max should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Double> gt =
        new UnboundPredicate<>(GT, ref("f"), (double) Float.MAX_VALUE * -2);
    assertThat(gt.bind(struct))
        .as("Greater than below min should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Double> gteq =
        new UnboundPredicate<>(GT_EQ, ref("f"), (double) Float.MAX_VALUE * -2);
    assertThat(gteq.bind(struct))
        .as("Greater than or equal below min should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());

    UnboundPredicate<Double> gtMax =
        new UnboundPredicate<>(GT, ref("f"), (double) Float.MAX_VALUE * 2);
    assertThat(gtMax.bind(struct))
        .as("Greater than above max should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    UnboundPredicate<Double> gteqMax =
        new UnboundPredicate<>(GT_EQ, ref("f"), (double) Float.MAX_VALUE * 2);
    assertThat(gteqMax.bind(struct))
        .as("Greater than or equal above max should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    UnboundPredicate<Double> ltMin =
        new UnboundPredicate<>(LT, ref("f"), (double) Float.MAX_VALUE * -2);
    assertThat(ltMin.bind(struct))
        .as("Less than below min should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    UnboundPredicate<Double> lteqMin =
        new UnboundPredicate<>(LT_EQ, ref("f"), (double) Float.MAX_VALUE * -2);
    assertThat(lteqMin.bind(struct))
        .as("Less than or equal below min should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());

    Expression ltExpr = new UnboundPredicate<>(LT, ref("f"), (double) Float.MAX_VALUE).bind(struct);
    BoundPredicate<Float> ltMax = assertAndUnwrap(ltExpr);
    assertThat(ltMax.isLiteralPredicate()).isTrue();
    assertThat(ltMax.asLiteralPredicate().literal().value())
        .as("Should translate bound to Float")
        .isEqualTo(Float.MAX_VALUE);

    Expression lteqExpr =
        new UnboundPredicate<>(LT_EQ, ref("f"), (double) Float.MAX_VALUE).bind(struct);
    BoundPredicate<Float> lteqMax = assertAndUnwrap(lteqExpr);
    assertThat(lteqMax.isLiteralPredicate()).isTrue();
    assertThat(lteqMax.asLiteralPredicate().literal().value())
        .as("Should translate bound to Float")
        .isEqualTo(Float.MAX_VALUE);

    Expression gtExpr =
        new UnboundPredicate<>(GT, ref("f"), (double) -Float.MAX_VALUE).bind(struct);
    BoundPredicate<Float> gtMin = assertAndUnwrap(gtExpr);
    assertThat(gtMin.isLiteralPredicate()).isTrue();
    assertThat(gtMin.asLiteralPredicate().literal().value())
        .as("Should translate bound to Float")
        .isEqualTo(-Float.MAX_VALUE);

    Expression gteqExpr =
        new UnboundPredicate<>(GT_EQ, ref("f"), (double) -Float.MAX_VALUE).bind(struct);
    BoundPredicate<Float> gteqMin = assertAndUnwrap(gteqExpr);
    assertThat(gteqMin.isLiteralPredicate()).isTrue();
    assertThat(gteqMin.asLiteralPredicate().literal().value())
        .as("Should translate bound to Float")
        .isEqualTo(-Float.MAX_VALUE);
  }

  @Test
  public void testIsNull() {
    StructType optional = StructType.of(optional(19, "s", Types.StringType.get()));

    UnboundPredicate<?> unbound = new UnboundPredicate<>(IS_NULL, ref("s"));
    Expression expr = unbound.bind(optional);
    BoundPredicate<?> bound = assertAndUnwrap(expr);
    assertThat(bound.op()).as("Should use the same operation").isEqualTo(IS_NULL);
    assertThat(bound.ref().fieldId()).as("Should use the correct field").isEqualTo(19);
    assertThat(bound.isUnaryPredicate()).as("Should be a unary predicate").isTrue();

    StructType required = StructType.of(required(20, "s", Types.StringType.get()));
    assertThat(unbound.bind(required))
        .as("IsNull inclusive a required field should be alwaysFalse")
        .isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void testNotNull() {
    StructType optional = StructType.of(optional(21, "s", Types.StringType.get()));

    UnboundPredicate<?> unbound = new UnboundPredicate<>(NOT_NULL, ref("s"));
    Expression expr = unbound.bind(optional);
    BoundPredicate<?> bound = assertAndUnwrap(expr);
    assertThat(bound.op()).as("Should use the same operation").isEqualTo(NOT_NULL);
    assertThat(bound.ref().fieldId()).as("Should use the correct field").isEqualTo(21);
    assertThat(bound.isUnaryPredicate()).as("Should be a unary predicate").isTrue();

    StructType required = StructType.of(required(22, "s", Types.StringType.get()));
    assertThat(unbound.bind(required))
        .as("NotNull inclusive a required field should be alwaysTrue")
        .isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testIsNaN() {
    // double
    StructType struct = StructType.of(optional(21, "d", Types.DoubleType.get()));

    UnboundPredicate<?> unbound = new UnboundPredicate<>(IS_NAN, ref("d"));
    Expression expr = unbound.bind(struct);
    BoundPredicate<?> bound = assertAndUnwrap(expr);
    assertThat(bound.op()).as("Should use the same operation").isEqualTo(IS_NAN);
    assertThat(bound.ref().fieldId()).as("Should use the correct field").isEqualTo(21);
    assertThat(bound.isUnaryPredicate()).as("Should be a unary predicate").isTrue();

    // float
    struct = StructType.of(optional(21, "f", Types.FloatType.get()));

    unbound = new UnboundPredicate<>(IS_NAN, ref("f"));
    expr = unbound.bind(struct);
    bound = assertAndUnwrap(expr);
    assertThat(bound.op()).as("Should use the same operation").isEqualTo(IS_NAN);
    assertThat(bound.ref().fieldId()).as("Should use the correct field").isEqualTo(21);
    assertThat(bound.isUnaryPredicate()).as("Should be a unary predicate").isTrue();

    // string (non-compatible)
    StructType strStruct = StructType.of(optional(21, "s", Types.StringType.get()));
    assertThatThrownBy(() -> new UnboundPredicate<>(IS_NAN, ref("s")).bind(strStruct))
        .isInstanceOf(ValidationException.class)
        .hasMessage("IsNaN cannot be used with a non-floating-point column");
  }

  @Test
  public void testNotNaN() {
    // double
    StructType struct = StructType.of(optional(21, "d", Types.DoubleType.get()));

    UnboundPredicate<?> unbound = new UnboundPredicate<>(NOT_NAN, ref("d"));
    Expression expr = unbound.bind(struct);
    BoundPredicate<?> bound = assertAndUnwrap(expr);
    assertThat(bound.op()).as("Should use the same operation").isEqualTo(NOT_NAN);
    assertThat(bound.ref().fieldId()).as("Should use the correct field").isEqualTo(21);
    assertThat(bound.isUnaryPredicate()).as("Should be a unary predicate").isTrue();

    // float
    struct = StructType.of(optional(21, "f", Types.FloatType.get()));

    unbound = new UnboundPredicate<>(NOT_NAN, ref("f"));
    expr = unbound.bind(struct);
    bound = assertAndUnwrap(expr);
    assertThat(bound.op()).as("Should use the same operation").isEqualTo(NOT_NAN);
    assertThat(bound.ref().fieldId()).as("Should use the correct field").isEqualTo(21);
    assertThat(bound.isUnaryPredicate()).as("Should be a unary predicate").isTrue();

    // string (non-compatible)
    StructType strStruct = StructType.of(optional(21, "s", Types.StringType.get()));
    assertThatThrownBy(() -> new UnboundPredicate<>(NOT_NAN, ref("s")).bind(strStruct))
        .isInstanceOf(ValidationException.class)
        .hasMessage("NotNaN cannot be used with a non-floating-point column");
  }

  @Test
  public void testInPredicateBinding() {
    StructType struct =
        StructType.of(
            required(10, "x", Types.IntegerType.get()),
            required(11, "y", Types.IntegerType.get()),
            required(12, "z", Types.IntegerType.get()));

    UnboundPredicate<Integer> unbound = Expressions.in("y", 6, 7, 11);

    Expression expr = unbound.bind(struct);
    BoundSetPredicate<Integer> bound = assertAndUnwrapBoundSet(expr);

    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(11);
    assertThat(bound.op()).as("Should not change the IN operation").isEqualTo(IN);
    assertThat(
            bound.literalSet().stream()
                .sorted()
                .collect(Collectors.toList())
                .toArray(new Integer[2]))
        .as("Should not alter literal set values")
        .isEqualTo(new Integer[] {6, 7, 11});
  }

  @Test
  public void testInPredicateBindingConversion() {
    StructType struct = StructType.of(required(15, "d", Types.DecimalType.of(9, 2)));
    UnboundPredicate<String> unbound = Expressions.in("d", "12.40", "1.23", "99.99", "1.23");
    Expression expr = unbound.bind(struct);
    BoundSetPredicate<BigDecimal> bound = assertAndUnwrapBoundSet(expr);
    assertThat(
            bound.literalSet().stream()
                .sorted()
                .collect(Collectors.toList())
                .toArray(new BigDecimal[2]))
        .as("Should convert literal set values to decimal")
        .isEqualTo(
            new BigDecimal[] {
              new BigDecimal("1.23"), new BigDecimal("12.40"), new BigDecimal("99.99")
            });
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(15);
    assertThat(bound.op()).as("Should not change the IN operation").isEqualTo(IN);
  }

  @Test
  public void testInToEqPredicate() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    UnboundPredicate<Integer> unbound = Expressions.in("x", 5);

    assertThat(unbound.op()).as("Should create an IN predicate with a single item").isEqualTo(IN);
    assertThat(unbound.literals())
        .as("Should create an IN predicate with a single item")
        .hasSize(1);

    Expression expr = unbound.bind(struct);
    BoundPredicate<Integer> bound = assertAndUnwrap(expr);

    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should not alter literal value")
        .isEqualTo(5);
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(14);
    assertThat(bound.op()).as("Should change the operation from IN to EQ").isEqualTo(EQ);
  }

  @Test
  public void testInPredicateBindingConversionToEq() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    UnboundPredicate<Long> unbound = Expressions.in("x", 5L, Long.MAX_VALUE);

    Expression.Operation op = unbound.op();
    assertThat(op).as("Should create an IN unbound predicate").isEqualTo(IN);

    Expression expr = unbound.bind(struct);
    BoundPredicate<Integer> bound = assertAndUnwrap(expr);

    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should remove aboveMax literal value")
        .isEqualTo(5);
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(14);
    assertThat(bound.op()).as("Should change the IN operation to EQ").isEqualTo(EQ);
  }

  @Test
  public void testInPredicateBindingConversionDedupToEq() {
    StructType struct = StructType.of(required(15, "d", Types.DecimalType.of(9, 2)));
    UnboundPredicate<Double> unbound = Expressions.in("d", 12.40, 12.401, 12.402);
    assertThat(unbound.op()).as("Should create an IN unbound predicate").isEqualTo(IN);

    Expression expr = unbound.bind(struct);
    BoundPredicate<BigDecimal> bound = assertAndUnwrap(expr);
    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should convert literal set values to a single decimal")
        .isEqualTo(new BigDecimal("12.40"));
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(15);
    assertThat(bound.op()).as("Should change the IN operation to EQ").isEqualTo(EQ);
  }

  @Test
  public void testInPredicateBindingConversionToExpression() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    UnboundPredicate<Long> unbound = Expressions.in("x", Long.MAX_VALUE - 1, Long.MAX_VALUE);

    Expression.Operation op = unbound.op();
    assertThat(op).as("Should create an IN predicate").isEqualTo(IN);

    Expression expr = unbound.bind(struct);
    assertThat(expr)
        .as("Should change IN to alwaysFalse expression")
        .isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void testNotInPredicateBinding() {
    StructType struct =
        StructType.of(
            required(10, "x", Types.IntegerType.get()),
            required(11, "y", Types.IntegerType.get()),
            required(12, "z", Types.IntegerType.get()));

    UnboundPredicate<Integer> unbound = Expressions.notIn("y", 6, 7, 11);

    Expression expr = unbound.bind(struct);
    BoundSetPredicate<Integer> bound = assertAndUnwrapBoundSet(expr);

    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(11);
    assertThat(bound.op()).as("Should not change the NOT_IN operation").isEqualTo(NOT_IN);
    assertThat(
            bound.literalSet().stream()
                .sorted()
                .collect(Collectors.toList())
                .toArray(new Integer[2]))
        .as("Should not alter literal set values")
        .isEqualTo(new Integer[] {6, 7, 11});
  }

  @Test
  public void testNotInPredicateBindingConversion() {
    StructType struct = StructType.of(required(15, "d", Types.DecimalType.of(9, 2)));
    UnboundPredicate<String> unbound = Expressions.notIn("d", "12.40", "1.23", "99.99", "1.23");
    Expression expr = unbound.bind(struct);
    BoundSetPredicate<BigDecimal> bound = assertAndUnwrapBoundSet(expr);
    assertThat(
            bound.literalSet().stream()
                .sorted()
                .collect(Collectors.toList())
                .toArray(new BigDecimal[2]))
        .as("Should convert literal set values to decimal")
        .isEqualTo(
            new BigDecimal[] {
              new BigDecimal("1.23"), new BigDecimal("12.40"), new BigDecimal("99.99")
            });
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(15);
    assertThat(bound.op()).as("Should not change the NOT_IN operation").isEqualTo(NOT_IN);
  }

  @Test
  public void testNotInToNotEqPredicate() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    UnboundPredicate<Integer> unbound = Expressions.notIn("x", 5);

    assertThat(unbound.op())
        .as("Should create a NOT_IN predicate with a single item")
        .isEqualTo(NOT_IN);
    assertThat(unbound.literals())
        .as("Should create a NOT_IN predicate with a single item")
        .hasSize(1);

    Expression expr = unbound.bind(struct);
    BoundPredicate<Integer> bound = assertAndUnwrap(expr);

    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should not alter literal value")
        .isEqualTo(5);
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(14);
    assertThat(bound.op())
        .as("Should change the operation from NOT_IN to NOT_EQ")
        .isEqualTo(NOT_EQ);
  }

  @Test
  public void testNotInPredicateBindingConversionToNotEq() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    UnboundPredicate<Long> unbound = Expressions.notIn("x", 5L, Long.MAX_VALUE);

    Expression.Operation op = unbound.op();
    assertThat(op).as("Should create a NOT_IN unbound predicate").isEqualTo(NOT_IN);

    Expression expr = unbound.bind(struct);
    BoundPredicate<Integer> bound = assertAndUnwrap(expr);

    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should remove aboveMax literal value")
        .isEqualTo(5);
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(14);
    assertThat(bound.op()).as("Should change the NOT_IN operation to NOT_EQ").isEqualTo(NOT_EQ);
  }

  @Test
  public void testNotInPredicateBindingConversionDedupToNotEq() {
    StructType struct = StructType.of(required(15, "d", Types.DecimalType.of(9, 2)));
    UnboundPredicate<Double> unbound = Expressions.notIn("d", 12.40, 12.401, 12.402);
    assertThat(unbound.op()).as("Should create a NOT_IN unbound predicate").isEqualTo(NOT_IN);

    Expression expr = unbound.bind(struct);
    BoundPredicate<BigDecimal> bound = assertAndUnwrap(expr);
    assertThat(bound.isLiteralPredicate()).isTrue();
    assertThat(bound.asLiteralPredicate().literal().value())
        .as("Should convert literal set values to a single decimal")
        .isEqualTo(new BigDecimal("12.40"));
    assertThat(bound.ref().fieldId()).as("Should reference correct field ID").isEqualTo(15);
    assertThat(bound.op()).as("Should change the NOT_IN operation to NOT_EQ").isEqualTo(NOT_EQ);
  }

  @Test
  public void testNotInPredicateBindingConversionToExpression() {
    StructType struct = StructType.of(required(14, "x", Types.IntegerType.get()));

    UnboundPredicate<Long> unbound = Expressions.notIn("x", Long.MAX_VALUE - 1, Long.MAX_VALUE);

    Expression.Operation op = unbound.op();
    assertThat(op).as("Should create an NOT_IN predicate").isEqualTo(NOT_IN);

    Expression expr = unbound.bind(struct);
    assertThat(expr)
        .as("Should change NOT_IN to alwaysTrue expression")
        .isEqualTo(Expressions.alwaysTrue());
  }
}
