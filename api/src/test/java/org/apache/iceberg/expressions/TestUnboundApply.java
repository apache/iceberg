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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestUnboundApply {
  private static final Types.StructType STRUCT =
      Types.StructType.of(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Test
  public void constantArgumentsAreConvertedToLiterals() {
    UnboundTerm<?> ref = Expressions.ref("id");
    UnboundApply<?> apply =
        new UnboundApply<>(Expressions.function("bucket"), ImmutableList.of(16, ref));

    assertThat(apply.arguments()).hasSize(2);
    assertThat(apply.arguments().get(0)).isInstanceOf(Literal.class);
    assertThat(((Literal<?>) apply.arguments().get(0)).value()).isEqualTo(16);
    assertThat(apply.arguments().get(1)).isSameAs(ref);
  }

  @Test
  public void valueExpressionAndPredicateArgumentsArePreserved() {
    UnboundTerm<?> nested =
        new UnboundApply<>(Expressions.function("year"), ImmutableList.of(Expressions.ref("ts")));
    Expression predicate = Expressions.isNull("id");
    UnboundTerm<?> ref = Expressions.ref("id");
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("if_else"), ImmutableList.of(predicate, nested, ref));

    assertThat(apply.arguments()).containsExactly(predicate, nested, ref);
  }

  @Test
  public void literalArgumentsArePreserved() {
    Literal<Integer> lit = Expressions.lit(16);
    UnboundApply<?> apply =
        new UnboundApply<>(Expressions.function("bucket"), ImmutableList.of(lit));

    assertThat(apply.arguments()).containsExactly(lit);
  }

  @Test
  public void nullArgumentIsRejected() {
    assertThatThrownBy(
            () ->
                new UnboundApply<>(
                    Expressions.function("my_func"), Collections.singletonList((Object) null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function argument: null");
  }

  @Test
  public void argumentThatIsNotAnExpressionIsRejected() {
    assertThatThrownBy(
            () ->
                new UnboundApply<>(
                    Expressions.function("my_func"),
                    Arrays.asList((Object) LocalDate.parse("2024-01-01"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create expression literal from java.time.LocalDate: 2024-01-01");
  }

  @Test
  public void nullFunctionIsRejected() {
    assertThatThrownBy(() -> new UnboundApply<>(null, ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function: null");
  }

  @Test
  public void refIsNotSupported() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"), ImmutableList.of(Expressions.ref("id")));

    assertThatThrownBy(apply::ref)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot determine reference for function: my_func");
  }

  @Test
  public void bindWithoutResultTypeFails() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"), ImmutableList.of(Expressions.ref("id")));

    assertThatThrownBy(() -> apply.bind(STRUCT, false))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot bind function without a result type: my_func");
  }

  @Test
  public void bindUsesResultType() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"),
            ImmutableList.of(Expressions.ref("id")),
            Types.StringType.get());

    assertThat(apply.bind(STRUCT, false).type()).isEqualTo(Types.StringType.get());
  }

  @Test
  public void bindBindsReferenceArguments() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"),
            ImmutableList.of(Expressions.ref("id")),
            Types.StringType.get());

    BoundApply<?> bound = (BoundApply<?>) apply.bind(STRUCT, false);
    assertThat(bound.arguments()).hasSize(1);
    assertThat(bound.arguments().get(0)).isInstanceOf(BoundReference.class);
    assertThat(((BoundReference<?>) bound.arguments().get(0)).fieldId()).isEqualTo(1);
  }

  @Test
  public void bindBindsNestedApply() {
    UnboundApply<?> nested =
        new UnboundApply<>(
            Expressions.function("inner"),
            ImmutableList.of(Expressions.ref("id")),
            Types.IntegerType.get());
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("outer"), ImmutableList.of(nested), Types.StringType.get());

    BoundApply<?> bound = (BoundApply<?>) apply.bind(STRUCT, false);
    assertThat(bound.arguments().get(0)).isInstanceOf(BoundApply.class);
    BoundApply<?> boundNested = (BoundApply<?>) bound.arguments().get(0);
    assertThat(boundNested.type()).isEqualTo(Types.IntegerType.get());
    assertThat(boundNested.arguments().get(0)).isInstanceOf(BoundReference.class);
  }

  @Test
  public void bindBindsPredicateArguments() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"),
            ImmutableList.of(Expressions.equal("id", 5)),
            Types.StringType.get());

    BoundApply<?> bound = (BoundApply<?>) apply.bind(STRUCT, false);
    assertThat(bound.arguments().get(0)).isInstanceOf(BoundPredicate.class);
  }

  @Test
  public void boundApplyEvalIsNotSupported() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"),
            ImmutableList.of(Expressions.ref("id")),
            Types.StringType.get());
    BoundTerm<?> bound = apply.bind(STRUCT, false);

    assertThatThrownBy(() -> bound.eval(null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot evaluate " + bound);
  }

  @Test
  public void boundApplyEquivalence() {
    UnboundApply<?> apply =
        new UnboundApply<>(
            Expressions.function("my_func"),
            ImmutableList.of(16, Expressions.ref("id")),
            Types.StringType.get());
    UnboundApply<?> otherFunction =
        new UnboundApply<>(
            Expressions.function("other_func"),
            ImmutableList.of(16, Expressions.ref("id")),
            Types.StringType.get());

    BoundTerm<?> bound = apply.bind(STRUCT, false);
    assertThat(bound.isEquivalentTo(apply.bind(STRUCT, false))).isTrue();
    assertThat(bound.isEquivalentTo(otherFunction.bind(STRUCT, false))).isFalse();
  }

  @Test
  public void transformIsApplyOfIcebergFunction() {
    UnboundApply<?> apply = (UnboundApply<?>) Expressions.bucket("id", 16);

    assertThat(apply.function()).hasToString("iceberg_functions.bucket");
    assertThat(apply.arguments()).hasSize(2);
    assertThat(apply.arguments().get(0)).isEqualTo(Expressions.lit(16));
    assertThat(apply.arguments().get(1)).isSameAs(apply.ref());
  }

  @Test
  public void voidTransformIsApplyWithoutCatalog() {
    UnboundApply<?> apply = (UnboundApply<?>) Expressions.transform("id", Transforms.alwaysNull());

    assertThat(apply.function().catalog()).isNull();
    assertThat(apply.function().identifier()).containsExactly("void");
  }

  @Test
  public void boundTransformIsApplyOfIcebergFunction() {
    BoundApply<?> bound = (BoundApply<?>) Expressions.bucket("id", 16).bind(STRUCT, false);

    assertThat(bound.function()).hasToString("iceberg_functions.bucket");
    assertThat(bound.arguments()).hasSize(2);
    assertThat(bound.arguments().get(0)).isEqualTo(Expressions.lit(16));
    assertThat(bound.arguments().get(1)).isSameAs(bound.ref());
    assertThat(bound.type()).isEqualTo(Types.IntegerType.get());
  }
}
