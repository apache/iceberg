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
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.params.provider.CsvSource;

class TestValuePredicate {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "other", Types.LongType.get()),
          required(3, "data", Types.StringType.get()),
          required(4, "ts", Types.TimestampType.withoutZone()));
  private static final Types.StructType STRUCT = SCHEMA.asStruct();
  private static final PartitionSpec ID_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("id").build();
  private static final FunctionReference FUNCTION =
      new FunctionReference("cat", CatalogObjectIdentifier.of("ns", "f"));

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

  @Test
  void outOfRangeComparisonFoldsToConstant() {
    UnboundValuePredicate<Long> pred =
        new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(Long.MAX_VALUE));

    assertThat(bind(pred, Types.IntegerType.get())).isEqualTo(Expressions.alwaysTrue());
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

    assertThat(negated).isInstanceOf(BoundValuePredicate.class);
    BoundValuePredicate<?> negatedPred = (BoundValuePredicate<?>) negated;
    assertThat(negatedPred.op()).isEqualTo(Operation.GT_EQ);
    assertThat(negatedPred.left()).isSameAs(bound.left());
    assertThat(negatedPred.right()).isSameAs(bound.right());
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

  @Test
  void evaluatorRejectsValuePredicate() {
    Evaluator evaluator = new Evaluator(STRUCT, idLessThanOther());

    assertThatThrownBy(() -> evaluator.eval(Row.of(1L, 2L, "a", 0L)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support value predicates");
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
  void inclusiveProjectionOfTermValuePredicateUsesPartitionField() {
    Expression filter =
        new UnboundValuePredicate<>(Operation.EQ, Expressions.ref("id"), Literal.of(5L));

    Expression projected = Projections.inclusive(ID_SPEC).project(filter);

    assertThat(projected).isInstanceOf(UnboundPredicate.class);
    assertThat(((UnboundPredicate<?>) projected).ref().name()).isEqualTo("id");
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
