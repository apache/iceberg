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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestValuePredicate {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "other", Types.LongType.get()));
  private static final Types.StructType STRUCT = SCHEMA.asStruct();
  private static final FunctionReference FUNCTION =
      new FunctionReference("cat", CatalogObjectIdentifier.of("ns", "f"));

  private static FunctionBinder returning(Type type) {
    return (ref, args) -> new BoundFunctionReference<>(ref.catalog(), ref.identifier(), type);
  }

  private static <T> UnboundApply<T> applyToId() {
    return new UnboundApply<>(FUNCTION, List.of(Expressions.ref("id")));
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
  void functionComparisonBindsToValuePredicate() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5));

    Expression bound = Binder.bind(STRUCT, pred, true, returning(Types.IntegerType.get()));

    assertThat(bound).isInstanceOf(BoundValuePredicate.class);
    BoundValuePredicate<?> valuePred = (BoundValuePredicate<?>) bound;
    assertThat(valuePred.op()).isEqualTo(Operation.LT);
    assertThat(valuePred.left()).isInstanceOf(BoundApply.class);
    assertThat(((Literal<?>) valuePred.right()).value()).isEqualTo(5);
  }

  @Test
  void literalIsConvertedToFunctionReturnType() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.EQ, applyToId(), Literal.of(5));

    BoundValuePredicate<?> bound =
        (BoundValuePredicate<?>) Binder.bind(STRUCT, pred, true, returning(Types.LongType.get()));

    assertThat(((Literal<?>) bound.right()).value()).isEqualTo(5L);
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
  void negateFlipsOperation() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.LT, applyToId(), Literal.of(5));

    Expression negated = ExpressionVisitors.visit(Expressions.not(pred), RewriteNot.get());

    assertThat(negated).isInstanceOf(UnboundValuePredicate.class);
    assertThat(negated.op()).isEqualTo(Operation.GT_EQ);
  }

  @Test
  void emptyInBindsToAlwaysFalse() {
    UnboundValuePredicate<Integer> pred =
        new UnboundValuePredicate<>(Operation.IN, applyToId(), List.of());

    Expression bound = Binder.bind(STRUCT, pred, true, returning(Types.IntegerType.get()));

    assertThat(bound).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  void boundReferencesIncludeFunctionArguments() {
    Expression bound =
        Binder.bind(
            STRUCT,
            new UnboundValuePredicate<>(Operation.LT, applyToId(), Expressions.ref("other")),
            true,
            returning(Types.LongType.get()));

    assertThat(Binder.boundReferences(STRUCT, bound, true)).containsExactlyInAnyOrder(1, 2);
  }

  @Test
  void metricsEvaluatorKeepsFilesForValuePredicate() {
    Expression columnComparison =
        new UnboundValuePredicate<>(
            Operation.LT, Expressions.ref("id"), Expressions.<Long>ref("other"));

    boolean mightMatch =
        new InclusiveMetricsEvaluator(SCHEMA, columnComparison)
            .eval(new TestDataFile("file.parquet", Row.of(), 10));

    assertThat(mightMatch).isTrue();
  }

  @Test
  void evaluatorRejectsValuePredicate() {
    Expression columnComparison =
        new UnboundValuePredicate<>(
            Operation.LT, Expressions.ref("id"), Expressions.<Long>ref("other"));
    Evaluator evaluator = new Evaluator(STRUCT, columnComparison);

    assertThatThrownBy(() -> evaluator.eval(Row.of(1L, 2L)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support value predicates");
  }
}
