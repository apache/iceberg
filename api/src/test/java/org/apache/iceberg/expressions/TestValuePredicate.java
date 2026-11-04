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

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestValuePredicate {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "other", Types.LongType.get()),
          required(3, "data", Types.StringType.get()),
          required(4, "ts", Types.TimestampType.withoutZone()));
  private static final Types.StructType STRUCT = SCHEMA.asStruct();
  private static final PartitionSpec ID_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("id", "id_part").build();
  private static final Types.StructType OPTIONAL_STRUCT =
      Types.StructType.of(
          optional(1, "x", Types.LongType.get()),
          optional(2, "y", Types.LongType.get()),
          optional(3, "s", Types.StringType.get()),
          optional(4, "t", Types.StringType.get()),
          optional(5, "d", Types.DoubleType.get()),
          optional(6, "e", Types.DoubleType.get()));
  private static final Types.StructType INT_STRUCT =
      Types.StructType.of(required(5, "i", Types.IntegerType.get()));
  private static final Types.StructType PROMOTION_STRUCT =
      Types.StructType.of(
          optional(1, "i", Types.IntegerType.get()),
          optional(2, "l", Types.LongType.get()),
          optional(3, "f", Types.FloatType.get()),
          optional(4, "d", Types.DoubleType.get()),
          optional(5, "dec", Types.DecimalType.of(9, 2)),
          optional(6, "wide_dec", Types.DecimalType.of(18, 2)),
          optional(7, "scaled_dec", Types.DecimalType.of(18, 3)));
  private static final Types.StructType UUID_STRUCT =
      Types.StructType.of(optional(1, "u", Types.UUIDType.get()));
  private static final FunctionReference FUNCTION =
      new FunctionReference("cat", CatalogObjectIdentifier.of("ns", "f"));
  private static final Object[][] LONG_VALUES = {
    {null, null}, {null, 1L}, {1L, null}, {1L, 1L}, {1L, 2L}, {2L, 1L}
  };
  private static final Object[][] DOUBLE_VALUES = {
    {null, null},
    {null, 1.0},
    {Double.NaN, null},
    {Double.NaN, Double.NaN},
    {Double.NaN, 1.0},
    {1.0, Double.NaN},
    {1.0, 1.0},
    {1.0, 2.0},
    {2.0, 1.0},
    {-0.0, 0.0}
  };

  private static FunctionBinder returning(Type type) {
    return (ref, args) -> new BoundFunctionReference<>(ref.catalog(), ref.identifier(), type);
  }

  private static <T> UnboundApply<T> applyToId() {
    return new UnboundApply<>(FUNCTION, List.of(Expressions.ref("id")));
  }

  private static UnboundValuePredicate<Long> idLessThanOther() {
    return new UnboundValuePredicate<>(
        Operation.LT, Expressions.ref("id"), Expressions.<Long>ref("other"));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static UnboundValuePredicate<Object> literals(
      Operation op, Literal<?> left, Literal<?> right) {
    return new UnboundValuePredicate<>(op, (Literal) left, (Literal) right);
  }

  private static <T> Expression bind(UnboundValuePredicate<T> pred, Type functionType) {
    return Binder.bind(STRUCT, pred, true, returning(functionType));
  }

  @Test
  void termAndLiteralBindToLegacyPredicate() {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.LT, Expressions.ref("id"), Literal.of(5L));

    Expression bound = Binder.bind(STRUCT, pred, true);

    assertThat(bound).isInstanceOf(BoundLiteralPredicate.class);
    BoundLiteralPredicate<?> literalPred = (BoundLiteralPredicate<?>) bound;
    assertThat(literalPred.ref().fieldId()).isEqualTo(1);
    assertThat(literalPred.literal().value()).isEqualTo(5L);
  }

  @Test
  void literalAndTermBindToFlippedLegacyPredicate() {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.LT, Literal.of(5L), Expressions.ref("id"));

    Expression bound = Binder.bind(STRUCT, pred, true);

    assertThat(bound).isInstanceOf(BoundLiteralPredicate.class);
    assertThat(bound.op()).isEqualTo(Operation.GT);
    assertThat(((BoundLiteralPredicate<?>) bound).literal().value()).isEqualTo(5L);
  }

  @Test
  void unaryTestOnRequiredTermUsesLegacyRewrite() {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.IS_NULL, Expressions.ref("id"));

    assertThat(Binder.bind(STRUCT, pred, true)).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  void setTestOnTermKeepsLiteralTypes() {
    Literal<Long> first = Literal.of("2024-01-01T00:00:00.000001001").to(nanos());
    Literal<Long> second = Literal.of("2024-01-01T00:00:00.000002001").to(nanos());
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.IN, Expressions.ref("ts"), List.of(first, second));

    Expression bound = Binder.bind(STRUCT, pred, true);

    assertThat(bound).isInstanceOf(BoundSetPredicate.class);
    assertThat(((BoundSetPredicate<Long>) bound).literalSet())
        .containsExactlyInAnyOrder(
            first.<Long>to(Types.TimestampType.withoutZone()).value(),
            second.<Long>to(Types.TimestampType.withoutZone()).value());
  }

  @Test
  void functionComparisonBindsToValuePredicate() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5));

    Expression bound = bind(pred, Types.IntegerType.get());

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
    BoundValuePredicate<?> valuePred = (BoundValuePredicate<?>) bound;
    assertThat(valuePred.op()).isEqualTo(Operation.LT);
    assertThat(valuePred.left()).isInstanceOf(BoundApply.class);
    assertThat(((Literal<?>) valuePred.right()).value()).isEqualTo(5);
  }

  @Test
  void unaryTestOnFunctionBindsToValuePredicate() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.IS_NULL, applyToId());

    Expression bound = bind(pred, Types.IntegerType.get());

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
    assertThat(((BoundValuePredicate<?>) bound).child()).isInstanceOf(BoundApply.class);
  }

  @Test
  void literalIsConvertedToFunctionReturnType() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.EQ, applyToId(), Literal.of(5));

    BoundValuePredicate<?> bound = (BoundValuePredicate<?>) bind(pred, Types.LongType.get());

    assertThat(((Literal<?>) bound.right()).value()).isEqualTo(5L);
  }

  @Test
  void leftLiteralIsConvertedToFunctionReturnType() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.LT, Literal.of(5), applyToId());

    BoundValuePredicate<?> bound = (BoundValuePredicate<?>) bind(pred, Types.LongType.get());

    assertThat(((Literal<?>) bound.left()).value()).isEqualTo(5L);
  }

  @Test
  void setLiteralsAreConvertedToFunctionReturnType() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(
            Operation.IN, applyToId(), List.of(Literal.of(1), Literal.of(2)));

    BoundValuePredicate<Long> bound = (BoundValuePredicate<Long>) bind(pred, Types.LongType.get());

    assertThat(bound.literalSet()).containsExactlyInAnyOrder(1L, 2L);
  }

  @Test
  void unconvertibleLiteralFails() {
    UnboundValuePredicate<CharSequence> pred =
        new UnboundValuePredicate<>(Operation.EQ, applyToId(), Literal.of("abc"));

    assertThatThrownBy(() -> bind(pred, Types.LongType.get()))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Invalid value for conversion to type long");
  }

  private static Stream<Arguments> outOfRangeComparisons() {
    return Stream.of(
            Operation.LT,
            Operation.LT_EQ,
            Operation.GT,
            Operation.GT_EQ,
            Operation.EQ,
            Operation.NOT_EQ)
        .flatMap(
            op -> Stream.of(Arguments.of(op, Long.MAX_VALUE), Arguments.of(op, Long.MIN_VALUE)));
  }

  @ParameterizedTest
  @MethodSource("outOfRangeComparisons")
  void outOfRangeRightLiteralFoldsLikeLegacyPredicate(Operation op, long value) {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(op, applyToId(), Literal.of(value));
    Expression expected = Binder.bind(INT_STRUCT, Expressions.predicate(op, "i", value), true);

    assertThat(bind(pred, Types.IntegerType.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("outOfRangeComparisons")
  void outOfRangeLeftLiteralFoldsLikeFlippedLegacyPredicate(Operation op, long value) {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(op, Literal.of(value), applyToId());
    Expression expected =
        Binder.bind(INT_STRUCT, Expressions.predicate(op.flipLR(), "i", value), true);

    assertThat(bind(pred, Types.IntegerType.get())).isEqualTo(expected);
  }

  @Test
  void outOfRangeSetLiteralsAreDropped() {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(
            Operation.IN, applyToId(), List.of(Literal.of(1L), Literal.of(1L << 40)));

    Expression bound = bind(pred, Types.IntegerType.get());

    assertThat(bound.op()).isEqualTo(Operation.EQ);
    assertThat(((Literal<?>) ((BoundValuePredicate<?>) bound).right()).value()).isEqualTo(1);
  }

  @ParameterizedTest
  @CsvSource({"IN, EQ", "NOT_IN, NOT_EQ"})
  void setWithOneDistinctValueBindsToEquality(Operation setOp, Operation expectedOp) {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(setOp, applyToId(), List.of(Literal.of(5), Literal.of(5)));

    BoundValuePredicate<?> bound = (BoundValuePredicate<?>) bind(pred, Types.IntegerType.get());

    assertThat(bound.op()).isEqualTo(expectedOp);
    assertThat(((Literal<?>) bound.right()).value()).isEqualTo(5);
  }

  @ParameterizedTest
  @CsvSource({"IN, false", "NOT_IN, true"})
  void emptySetBindsToConstant(Operation setOp, boolean expected) {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(setOp, applyToId(), List.of());

    assertThat(bind(pred, Types.IntegerType.get()))
        .isEqualTo(expected ? Expressions.alwaysTrue() : Expressions.alwaysFalse());
  }

  @ParameterizedTest
  @CsvSource({"IN, false", "NOT_IN, true"})
  void setWithAllValuesOutOfRangeBindsToConstant(Operation setOp, boolean expected) {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(setOp, applyToId(), List.of(Literal.of(1L << 40)));

    assertThat(bind(pred, Types.IntegerType.get()))
        .isEqualTo(expected ? Expressions.alwaysTrue() : Expressions.alwaysFalse());
  }

  @Test
  void nanComparisonLiteralIsRejected() {
    assertThatThrownBy(
            () -> new UnboundValuePredicate<>(Operation.LT, Literal.of(Double.NaN), applyToId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid literal: NaN, use isNaN or notNaN instead");
  }

  @Test
  void nanSetLiteralIsRejected() {
    assertThatThrownBy(
            () ->
                new UnboundValuePredicate<>(
                    Operation.IN, Expressions.ref("id"), List.of(Literal.of(Double.NaN))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid literal: NaN, use isNaN or notNaN instead");
  }

  @Test
  void nanRightLiteralIsRejected() {
    assertThatThrownBy(
            () ->
                new UnboundValuePredicate<>(
                    Operation.LT, Expressions.ref("id"), Literal.of(Double.NaN)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid literal: NaN, use isNaN or notNaN instead");
  }

  @Test
  void nonPrimitiveComparisonFails() {
    Types.StructType struct =
        Types.StructType.of(
            optional(1, "point", Types.StructType.of(optional(2, "x", Types.LongType.get()))));
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(
            Operation.EQ, Expressions.ref("point"), Expressions.<Object>ref("point"));

    assertThatThrownBy(() -> Binder.bind(struct, pred, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot compare non-primitive type");
  }

  @ParameterizedTest
  @CsvSource({"LT, true", "EQ, false", "GT_EQ, false"})
  void literalComparisonWithPromotionFoldsToConstant(Operation op, boolean expected) {
    UnboundValuePredicate<Object> pred = literals(op, Literal.of(1), Literal.of(2L));

    assertThat(Binder.bind(STRUCT, pred, true))
        .isEqualTo(expected ? Expressions.alwaysTrue() : Expressions.alwaysFalse());
  }

  @Test
  void literalStartsWithFoldsToConstant() {
    UnboundValuePredicate<Object> pred =
        literals(Operation.STARTS_WITH, Literal.of("abc"), Literal.of("ab"));

    assertThat(Binder.bind(STRUCT, pred, true)).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  void literalComparisonWithIncompatibleTypesFails() {
    UnboundValuePredicate<Object> pred = literals(Operation.LT, Literal.of(1), Literal.of("a"));

    assertThatThrownBy(() -> Binder.bind(STRUCT, pred, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot compare");
  }

  @Test
  void setTestOnNonPrimitiveFunctionFails() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.IN, applyToId(), List.of(Literal.of(1)));
    Type structType = Types.StructType.of(optional(10, "x", Types.LongType.get()));

    assertThatThrownBy(() -> bind(pred, structType))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot test set membership of non-primitive type");
  }

  @Test
  void nullSetLiteralIsRejected() {
    List<Literal<Long>> values = Arrays.asList(Literal.of(1L), null);

    assertThatThrownBy(() -> new UnboundValuePredicate<>(Operation.IN, applyToId(), values))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid literal: null");
  }

  @Test
  void mismatchedOperandTypesFail() {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(
            Operation.LT, Expressions.ref("id"), Expressions.<Object>ref("data"));

    assertThatThrownBy(() -> Binder.bind(STRUCT, pred, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot compare");
  }

  @Test
  void isNanRejectsNonFloatingPointFunction() {
    UnboundValuePredicate<Long> pred = new UnboundValuePredicate<>(Operation.IS_NAN, applyToId());

    assertThatThrownBy(() -> bind(pred, Types.LongType.get()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("cannot be used with a non-floating-point expression");
  }

  @Test
  void startsWithRejectsNonStringFunction() {
    UnboundValuePredicate<CharSequence> pred =
        new UnboundValuePredicate<>(Operation.STARTS_WITH, applyToId(), Literal.of("abc"));

    assertThatThrownBy(() -> bind(pred, Types.LongType.get()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("operands must be strings");
  }

  @Test
  void startsWithOnStringFunctionBindsToValuePredicate() {
    UnboundValuePredicate<CharSequence> pred =
        new UnboundValuePredicate<>(Operation.STARTS_WITH, applyToId(), Literal.of("abc"));

    Expression bound = bind(pred, Types.StringType.get());

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
    assertThat(bound.op()).isEqualTo(Operation.STARTS_WITH);
  }

  @Test
  void startsWithLiteralOnLeftIsNotFlipped() {
    UnboundValuePredicate<CharSequence> pred =
        new UnboundValuePredicate<>(
            Operation.STARTS_WITH, Literal.of("abc"), Expressions.ref("data"));

    Expression bound = Binder.bind(STRUCT, pred, true);

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
    assertThat(((BoundValuePredicate<?>) bound).left()).isInstanceOf(Literal.class);
    assertThat(((BoundValuePredicate<?>) bound).right()).isInstanceOf(BoundReference.class);
  }

  @Test
  void startsWithRejectsNonStringReference() {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(
            Operation.STARTS_WITH, Expressions.ref("data"), Expressions.<Object>ref("id"));

    assertThatThrownBy(() -> Binder.bind(STRUCT, pred, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("operands must be strings");
  }

  @Test
  void alreadyBoundOperandFails() {
    BoundTerm<Long> boundId = Expressions.<Long>ref("id").bind(STRUCT, true);
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.LT, boundId, Expressions.<Long>ref("other"));

    assertThatThrownBy(() -> Binder.bind(STRUCT, pred, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Found already bound operand");
  }

  @Test
  void bindFunctionWithoutBinderFails() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5));

    assertThatThrownBy(() -> Binder.bind(STRUCT, pred, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot bind function without a function binder");
  }

  @Test
  void bindValuePredicateInCompoundFilter() {
    Expression filter =
        Expressions.and(
            new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5)),
            Expressions.equal("other", 3L));

    Expression bound = Binder.bind(STRUCT, filter, true, returning(Types.IntegerType.get()));

    assertThat(bound).isInstanceOf(And.class);
    assertThat(((And) bound).left()).isInstanceOf(BoundValuePredicate.class);
    assertThat(((And) bound).right()).isInstanceOf(BoundLiteralPredicate.class);
  }

  @Test
  void rewriteNotNegatesUnboundValuePredicate() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.IN, applyToId(), List.of(Literal.of(1)));

    Expression negated = ExpressionVisitors.visit(Expressions.not(pred), RewriteNot.get());

    assertThat(negated).isInstanceOf(UnboundValuePredicate.class);
    UnboundValuePredicate<?> negatedPred = (UnboundValuePredicate<?>) negated;
    assertThat(negatedPred.op()).isEqualTo(Operation.NOT_IN);
    assertThat(negatedPred.child()).isSameAs(pred.child());
    assertThat(negatedPred.literals()).isEqualTo(pred.literals());
  }

  @Test
  void rewriteNotNegatesBoundValuePredicate() {
    BoundValuePredicate<?> bound = (BoundValuePredicate<?>) Binder.bind(STRUCT, idLessThanOther());

    Expression negated = ExpressionVisitors.visit(Expressions.not(bound), RewriteNot.get());

    assertThat(negated).isInstanceOf(Or.class);
    BoundValuePredicate<?> negatedPred = (BoundValuePredicate<?>) ((Or) negated).left();
    assertThat(negatedPred.op()).isEqualTo(Operation.GT_EQ);
    assertThat(negatedPred.left()).isSameAs(bound.left());
    assertThat(negatedPred.right()).isSameAs(bound.right());
  }

  @Test
  void rewriteNotMarksUnboundOrderingAsNegated() {
    Expression negated = Expressions.rewriteNot(Expressions.not(idLessThanOther()));

    assertThat(negated).isInstanceOf(UnboundValuePredicate.class);
    assertThat(negated.op()).isEqualTo(Operation.LT);
    assertThat(((UnboundValuePredicate<?>) negated).isNegated()).isTrue();
    assertThat(negated).hasToString("not(ref(name=\"id\") < ref(name=\"other\"))");
  }

  @Test
  void doubleNegationRestoresOrdering() {
    Expression negated = idLessThanOther().negate().negate();

    assertThat(((UnboundValuePredicate<?>) negated).isNegated()).isFalse();
    assertThat(negated.op()).isEqualTo(Operation.LT);
  }

  @Test
  void negatedTermComparisonIsNotProjected() {
    Expression filter =
        new UnboundValuePredicate<>(Operation.LT, Expressions.ref("id"), Literal.of(5L)).negate();

    assertThat(Projections.inclusive(ID_SPEC).project(filter)).isEqualTo(Expressions.alwaysTrue());
    assertThat(ResidualEvaluator.of(ID_SPEC, filter, true).residualFor(Row.of(5L)))
        .isSameAs(filter);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static UnboundValuePredicate<Object> comparedToOne(Operation op, String name) {
    Literal one = name.equals("x") ? Literal.of(1L) : Literal.of(1.0);
    return new UnboundValuePredicate<>(op, Expressions.ref(name), one);
  }

  private static Stream<Arguments> orderingNegations() {
    return Stream.of(Operation.LT, Operation.LT_EQ, Operation.GT, Operation.GT_EQ)
        .flatMap(
            op ->
                Stream.concat(
                    Arrays.stream(LONG_VALUES)
                        .map(values -> Arguments.of(op, "x", "y", values[0], values[1])),
                    Arrays.stream(DOUBLE_VALUES)
                        .map(values -> Arguments.of(op, "d", "e", values[0], values[1]))));
  }

  private static Row optionalRow(String left, Object leftValue, Object rightValue) {
    return left.equals("x")
        ? Row.of(leftValue, rightValue, null, null, null, null)
        : Row.of(null, null, null, null, leftValue, rightValue);
  }

  @ParameterizedTest
  @MethodSource("orderingNegations")
  void unboundNegationIsExact(
      Operation op, String left, String right, Object leftValue, Object rightValue) {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(op, Expressions.ref(left), Expressions.ref(right));
    Row row = optionalRow(left, leftValue, rightValue);
    boolean expected = !new Evaluator(OPTIONAL_STRUCT, pred).eval(row);

    Expression negated = Expressions.rewriteNot(Expressions.not(pred));

    assertThat(new Evaluator(OPTIONAL_STRUCT, negated).eval(row)).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("orderingNegations")
  void unboundNegationWithLiteralIsExact(
      Operation op, String left, String right, Object leftValue, Object rightValue) {
    UnboundValuePredicate<Object> pred = comparedToOne(op, left);
    Row row = optionalRow(left, leftValue, rightValue);
    boolean expected = !new Evaluator(OPTIONAL_STRUCT, pred).eval(row);

    Expression negated = Expressions.rewriteNot(Expressions.not(pred));

    assertThat(new Evaluator(OPTIONAL_STRUCT, negated).eval(row)).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("orderingNegations")
  void boundNegationIsExact(
      Operation op, String left, String right, Object leftValue, Object rightValue) {
    Expression bound =
        Binder.bind(
            OPTIONAL_STRUCT,
            new UnboundValuePredicate<>(op, Expressions.ref(left), Expressions.ref(right)));
    Row row = optionalRow(left, leftValue, rightValue);
    boolean expected = !Evaluator.ofBound(bound).eval(row);

    Expression negated = Expressions.rewriteNot(Expressions.not(bound));

    assertThat(Evaluator.ofBound(negated).eval(row)).isEqualTo(expected);
  }

  @Test
  void boundReferencesIncludeFunctionArguments() {
    Expression bound =
        bind(
            new UnboundValuePredicate<>(Operation.LT, applyToId(), Expressions.ref("other")),
            Types.LongType.get());

    assertThat(Binder.boundReferences(STRUCT, bound, true)).containsExactlyInAnyOrder(1, 2);
  }

  @Test
  void boundReferencesIncludeSetPredicateChild() {
    Expression bound =
        bind(
            new UnboundValuePredicate<>(
                Operation.IN,
                new UnboundApply<>(FUNCTION, List.of(Expressions.ref("other"))),
                List.of(Literal.of(1), Literal.of(2))),
            Types.IntegerType.get());

    assertThat(Binder.boundReferences(STRUCT, bound, true)).containsExactly(2);
  }

  @Test
  void unboundValuePredicateIsNotBound() {
    assertThat(Binder.isBound(idLessThanOther())).isFalse();
  }

  @Test
  void inclusiveMetricsEvaluatorKeepsFilesForValuePredicate() {
    boolean mightMatch =
        new InclusiveMetricsEvaluator(SCHEMA, idLessThanOther())
            .eval(new TestDataFile("file.parquet", Row.of(), 10));

    assertThat(mightMatch).isTrue();
  }

  @Test
  void inclusiveMetricsEvaluatorPrunesWithOtherConjunct() {
    Expression filter = Expressions.and(idLessThanOther(), Expressions.greaterThan("id", 10L));

    boolean mightMatch = new InclusiveMetricsEvaluator(SCHEMA, filter).eval(fileWithIdUpTo(3L));

    assertThat(mightMatch).isFalse();
  }

  @Test
  void strictMetricsEvaluatorNeverMatchesValuePredicate() {
    Expression filter = Expressions.or(idLessThanOther(), Expressions.lessThan("id", 0L));

    boolean mustMatch = new StrictMetricsEvaluator(SCHEMA, filter).eval(fileWithIdUpTo(3L));

    assertThat(mustMatch).isFalse();
  }

  @ParameterizedTest
  @CsvSource({"1, 2, true", "2, 2, false", "3, 2, false"})
  void evaluatorComparesReferences(long id, long other, boolean expected) {
    Evaluator evaluator = new Evaluator(STRUCT, idLessThanOther());

    assertThat(evaluator.eval(Row.of(id, other, "a", 0L))).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "EQ, , , true",
    "EQ, 34, , false",
    "EQ, , 34, false",
    "EQ, 1, 1, true",
    "EQ, 1, 2, false",
    "NOT_EQ, , , false",
    "NOT_EQ, 34, , true",
    "NOT_EQ, 1, 2, true",
    "LT, , , false",
    "LT, 34, , false",
    "LT, , 34, false",
    "LT, 1, 2, true",
    "LT, 2, 1, false",
    "LT_EQ, , , true",
    "LT_EQ, 34, , false",
    "LT_EQ, , 34, false",
    "LT_EQ, 1, 1, true",
    "LT_EQ, 2, 1, false",
    "GT, , , false",
    "GT, , 34, false",
    "GT, 2, 1, true",
    "GT_EQ, , , true",
    "GT_EQ, , 34, false",
    "GT_EQ, 1, 1, true",
    "GT_EQ, 1, 2, false"
  })
  void evaluatorComparisonsAreNullSafe(Operation op, Long left, Long right, boolean expected) {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(op, Expressions.ref("x"), Expressions.<Long>ref("y"));

    assertThat(
            new Evaluator(OPTIONAL_STRUCT, pred).eval(Row.of(left, right, null, null, null, null)))
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "EQ, NaN, NaN, true",
    "EQ, NaN, 1.0, false",
    "NOT_EQ, NaN, NaN, false",
    "LT, NaN, 1.0, false",
    "LT, 1.0, NaN, false",
    "GT, NaN, 1.0, false",
    "LT_EQ, NaN, NaN, true",
    "GT_EQ, NaN, 1.0, false",
    "EQ, -0.0, 0.0, true",
    "LT, -0.0, 0.0, false"
  })
  void evaluatorComparisonsFollowNaNRules(
      Operation op, double left, double right, boolean expected) {
    UnboundValuePredicate<Double> pred =
        new UnboundValuePredicate<>(op, Expressions.ref("d"), Expressions.<Double>ref("e"));

    assertThat(
            new Evaluator(OPTIONAL_STRUCT, pred).eval(Row.of(null, null, null, null, left, right)))
        .isEqualTo(expected);
  }

  @Test
  void evaluatorTestsNaNLiteral() {
    UnboundValuePredicate<Double> pred =
        new UnboundValuePredicate<>(Operation.IS_NAN, Literal.of(Double.NaN));

    assertThat(
            new Evaluator(OPTIONAL_STRUCT, pred).eval(Row.of(null, null, null, null, null, null)))
        .isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "STARTS_WITH, ab, a, true",
    "STARTS_WITH, ab, b, false",
    "STARTS_WITH, , nu, false",
    "STARTS_WITH, nullable, , false",
    "NOT_STARTS_WITH, ab, a, false",
    "NOT_STARTS_WITH, ab, b, true",
    "NOT_STARTS_WITH, , nu, true",
    "NOT_STARTS_WITH, nullable, , true"
  })
  void evaluatorAppliesStartsWithToNullableReferences(
      Operation op, String value, String prefix, boolean expected) {
    UnboundValuePredicate<CharSequence> pred =
        new UnboundValuePredicate<>(op, Expressions.ref("s"), Expressions.ref("t"));

    assertThat(
            new Evaluator(OPTIONAL_STRUCT, pred)
                .eval(Row.of(null, null, value, prefix, null, null)))
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({"ab, true", "abd, false", ", false"})
  void evaluatorAppliesStartsWithLiteralOnLeft(String prefix, boolean expected) {
    UnboundValuePredicate<CharSequence> pred =
        new UnboundValuePredicate<>(Operation.STARTS_WITH, Literal.of("abc"), Expressions.ref("t"));

    assertThat(
            new Evaluator(OPTIONAL_STRUCT, pred).eval(Row.of(null, null, null, prefix, null, null)))
        .isEqualTo(expected);
  }

  @Test
  void boundVisitorRejectsUnboundValuePredicate() {
    assertThatThrownBy(
            () ->
                ExpressionVisitors.visitEvaluator(
                    idLessThanOther(), new ExpressionVisitors.BoundVisitor<Boolean>() {}))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith("Not a bound predicate");
  }

  @Test
  void inclusiveProjectionOfFunctionPredicateIsAlwaysTrue() {
    Expression filter = new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5));

    assertThat(Projections.inclusive(ID_SPEC).project(filter)).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  void strictProjectionOfValuePredicateIsAlwaysFalse() {
    assertThat(Projections.strict(ID_SPEC).project(idLessThanOther()))
        .isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  void strictProjectionOfFunctionPredicateIsAlwaysFalse() {
    Expression filter = new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5));

    assertThat(Projections.strict(ID_SPEC).project(filter)).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  void strictProjectionOfTermValuePredicateUsesPartitionField() {
    Expression filter =
        new UnboundValuePredicate<>(Operation.EQ, Expressions.ref("id"), Literal.of(5L));

    Expression projected = Projections.strict(ID_SPEC).project(filter);

    assertThat(projected).isInstanceOf(UnboundPredicate.class);
    assertThat(((UnboundPredicate<?>) projected).ref().name()).isEqualTo("id_part");
  }

  @Test
  void inclusiveProjectionOfTermValuePredicateUsesPartitionField() {
    Expression filter =
        new UnboundValuePredicate<>(Operation.EQ, Expressions.ref("id"), Literal.of(5L));

    Expression projected = Projections.inclusive(ID_SPEC).project(filter);

    assertThat(projected).isInstanceOf(UnboundPredicate.class);
    assertThat(((UnboundPredicate<?>) projected).ref().name()).isEqualTo("id_part");
  }

  @Test
  void residualKeepsValuePredicateForPartitionedSpec() {
    UnboundValuePredicate<Long> valuePred = idLessThanOther();
    Expression filter = Expressions.and(valuePred, Expressions.equal("id", 5L));

    Expression residual = ResidualEvaluator.of(ID_SPEC, filter, true).residualFor(Row.of(5L));

    assertThat(residual).isSameAs(valuePred);
  }

  @Test
  void residualReducesTermValuePredicate() {
    Expression filter =
        new UnboundValuePredicate<>(Operation.EQ, Expressions.ref("id"), Literal.of(5L));

    Expression residual = ResidualEvaluator.of(ID_SPEC, filter, true).residualFor(Row.of(5L));

    assertThat(residual).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  void residualKeepsTermValuePredicateOnNonPartitionField() {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.EQ, Expressions.ref("other"), Literal.of(5L));

    Expression residual = ResidualEvaluator.of(ID_SPEC, pred, true).residualFor(Row.of(5L));

    assertThat(residual).isSameAs(pred);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static UnboundValuePredicate<Object> comparedTo(
      Operation op, String name, Literal<?> literal) {
    return new UnboundValuePredicate<>(op, Expressions.ref(name), (Literal) literal);
  }

  private static Stream<Arguments> zeroLiterals() {
    return Stream.of(Literal.of(0.0), Literal.of(-0.0), Literal.of(0L)).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("zeroLiterals")
  void zeroComparisonOnDoubleTermBindsToValuePredicate(Literal<?> zero) {
    Expression bound = Binder.bind(OPTIONAL_STRUCT, comparedTo(Operation.EQ, "d", zero));

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
  }

  @ParameterizedTest
  @MethodSource("zeroLiterals")
  void zeroEqualityOnDoubleTermMatchesBothZeros(Literal<?> zero) {
    Evaluator evaluator = new Evaluator(OPTIONAL_STRUCT, comparedTo(Operation.EQ, "d", zero));

    assertThat(evaluator.eval(Row.of(null, null, null, null, 0.0, null))).isTrue();
    assertThat(evaluator.eval(Row.of(null, null, null, null, -0.0, null))).isTrue();
  }

  @ParameterizedTest
  @MethodSource("zeroLiterals")
  void negativeZeroIsNotLessThanZeroLiteral(Literal<?> zero) {
    Evaluator evaluator = new Evaluator(OPTIONAL_STRUCT, comparedTo(Operation.LT, "d", zero));

    assertThat(evaluator.eval(Row.of(null, null, null, null, -0.0, null))).isFalse();
  }

  @Test
  void nonZeroComparisonOnDoubleTermBindsToLegacyPredicate() {
    Expression bound = Binder.bind(OPTIONAL_STRUCT, comparedTo(Operation.LT, "d", Literal.of(1.0)));

    assertThat(bound).isInstanceOf(BoundLiteralPredicate.class);
  }

  @Test
  void zeroComparisonOnDoubleTermIsNotProjected() {
    Schema schema = new Schema(optional(1, "d", Types.DoubleType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("d").build();
    Expression filter = comparedTo(Operation.LT_EQ, "d", Literal.of(-0.0));

    assertThat(Projections.inclusive(spec).project(filter)).isEqualTo(Expressions.alwaysTrue());
  }

  @ParameterizedTest
  @CsvSource({"0.0, -0.0", "-0.0, 0.0"})
  void setWithZeroOnDoubleTermMatchesBothZeros(double literal, double value) {
    UnboundValuePredicate<Double> pred =
        new UnboundValuePredicate<>(
            Operation.IN, Expressions.ref("d"), List.of(Literal.of(literal), Literal.of(2.0)));

    assertThat(Binder.bind(OPTIONAL_STRUCT, pred)).isInstanceOf(BoundValuePredicate.class);
    assertThat(new Evaluator(OPTIONAL_STRUCT, pred).eval(Row.of(null, null, null, null, value)))
        .isTrue();
  }

  @Test
  void setWithZeroOnDoubleFunctionContainsBothZeros() {
    UnboundValuePredicate<Double> pred =
        new UnboundValuePredicate<>(
            Operation.IN, applyToId(), List.of(Literal.of(0.0), Literal.of(2.0)));

    BoundValuePredicate<Double> bound =
        (BoundValuePredicate<Double>) bind(pred, Types.DoubleType.get());

    assertThat(bound.literalSet()).containsExactlyInAnyOrder(0.0, -0.0, 2.0);
  }

  @Test
  void uuidOrderingBindsToValuePredicate() {
    Expression bound =
        Binder.bind(UUID_STRUCT, comparedTo(Operation.GT, "u", Literal.of(UUID.randomUUID())));

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
  }

  @Test
  void uuidEqualityBindsToLegacyPredicate() {
    Expression bound =
        Binder.bind(UUID_STRUCT, comparedTo(Operation.EQ, "u", Literal.of(UUID.randomUUID())));

    assertThat(bound).isInstanceOf(BoundLiteralPredicate.class);
  }

  @ParameterizedTest
  @CsvSource({
    "GT, 80000000-0000-0000-0000-000000000000, 00000000-0000-0000-0000-000000000001, true",
    "LT, 80000000-0000-0000-0000-000000000000, 00000000-0000-0000-0000-000000000001, false",
    "GT, 00000000-0000-0000-8000-000000000000, 00000000-0000-0000-0000-000000000001, true"
  })
  void uuidOrderingIsUnsigned(Operation op, UUID value, UUID literal, boolean expected) {
    Evaluator evaluator = new Evaluator(UUID_STRUCT, comparedTo(op, "u", Literal.of(literal)));

    assertThat(evaluator.eval(Row.of(value))).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "LT, 1, 2, true",
    "LT, 2, 2, false",
    "LT, , 2, false",
    "LT, 1, , false",
    "LT_EQ, , , true",
    "EQ, 2, 2, true",
    "GT, 3, 2, true"
  })
  void intComparesWithPromotedLong(
      Operation op, Integer intValue, Long longValue, boolean expected) {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(op, Expressions.ref("i"), Expressions.ref("l"));

    assertThat(new Evaluator(PROMOTION_STRUCT, pred).eval(Row.of(intValue, longValue)))
        .isEqualTo(expected);
  }

  @Test
  void longComparesWithPromotedInt() {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(Operation.LT, Expressions.ref("l"), Expressions.ref("i"));

    assertThat(new Evaluator(PROMOTION_STRUCT, pred).eval(Row.of(2, 1L))).isTrue();
  }

  @ParameterizedTest
  @CsvSource({"1.5, 1.5, true", "-0.0, 0.0, true", "1.5, 2.5, false"})
  void floatComparesWithPromotedDouble(float floatValue, double doubleValue, boolean expected) {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(Operation.EQ, Expressions.ref("f"), Expressions.ref("d"));

    assertThat(
            new Evaluator(PROMOTION_STRUCT, pred).eval(Row.of(null, null, floatValue, doubleValue)))
        .isEqualTo(expected);
  }

  @Test
  void decimalComparesWithWiderPrecision() {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(
            Operation.EQ, Expressions.ref("dec"), Expressions.ref("wide_dec"));
    Row row = Row.of(null, null, null, null, new BigDecimal("1.50"), new BigDecimal("1.50"));

    assertThat(new Evaluator(PROMOTION_STRUCT, pred).eval(row)).isTrue();
  }

  @Test
  void decimalWithDifferentScaleFails() {
    UnboundValuePredicate<Object> pred =
        new UnboundValuePredicate<>(
            Operation.EQ, Expressions.ref("dec"), Expressions.ref("scaled_dec"));

    assertThatThrownBy(() -> Binder.bind(PROMOTION_STRUCT, pred))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot compare");
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void mixedSetLiteralTypesAreRejected() {
    List values = List.of(Literal.of(1), Literal.of(2L));

    assertThatThrownBy(() -> new UnboundValuePredicate<>(Operation.IN, applyToId(), values))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid literals: mixed types IntegerLiteral and LongLiteral");
  }

  @Test
  void andRejectsValueExpression() {
    assertThatThrownBy(() -> Expressions.and(Expressions.ref("id"), Expressions.equal("other", 3L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid left operand, not a predicate");
  }

  @Test
  void orRejectsValueExpression() {
    assertThatThrownBy(() -> Expressions.or(Expressions.equal("other", 3L), Literal.of(true)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid right operand, not a predicate");
  }

  @Test
  void notRejectsValueExpression() {
    assertThatThrownBy(() -> Expressions.not(applyToId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid operand, not a predicate");
  }

  private static UnboundApply<Long> ifElse() {
    return new UnboundApply<>(
        FUNCTION, List.of(Expressions.lessThan("id", 5L), Literal.of(1L), Literal.of(0L)));
  }

  private static BoundPredicateArgument boundCondition() {
    BoundValuePredicate<?> bound =
        (BoundValuePredicate<?>)
            bind(
                new UnboundValuePredicate<>(Operation.EQ, ifElse(), Literal.of(1L)),
                Types.LongType.get());
    return (BoundPredicateArgument) ((BoundApply<?>) bound.left()).arguments().get(0);
  }

  @Test
  void predicateFunctionArgumentIsBound() {
    assertThat(boundCondition().predicate()).isInstanceOf(BoundLiteralPredicate.class);
  }

  @ParameterizedTest
  @CsvSource({"4, true", "5, false"})
  void predicateFunctionArgumentEvaluatesPredicate(long id, boolean expected) {
    assertThat(boundCondition().eval(Row.of(id, 0L, "a", 0L))).isEqualTo(expected);
  }

  @Test
  void boundReferencesIncludePredicateFunctionArguments() {
    Expression bound =
        bind(
            new UnboundValuePredicate<>(Operation.EQ, ifElse(), Literal.of(1L)),
            Types.LongType.get());

    assertThat(Binder.boundReferences(STRUCT, bound, true)).containsExactly(1);
  }

  @Test
  void transformFunctionArgumentIsBound() {
    UnboundApply<Integer> apply =
        new UnboundApply<>(FUNCTION, List.of(Expressions.bucket("id", 16)));

    BoundApply<Integer> bound = apply.bind(returning(Types.IntegerType.get()), STRUCT, true);

    assertThat(bound.arguments().get(0)).isInstanceOf(BoundTransform.class);
  }

  private static Types.TimestampNanoType nanos() {
    return Types.TimestampNanoType.withoutZone();
  }

  private static TestDataFile fileWithIdUpTo(long upper) {
    return new TestDataFile(
        "file.parquet",
        Row.of(),
        10,
        Map.of(1, 10L, 2, 10L),
        Map.of(1, 0L, 2, 0L),
        null,
        Map.of(1, toByteBuffer(Types.LongType.get(), 0L)),
        Map.of(1, toByteBuffer(Types.LongType.get(), upper)));
  }
}
