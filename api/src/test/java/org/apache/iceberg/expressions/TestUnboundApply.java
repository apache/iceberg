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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestUnboundApply {
  private static final Types.StructType STRUCT =
      Types.StructType.of(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Test
  public void testConstantArgumentsAreConvertedToLiterals() {
    UnboundTerm<?> ref = Expressions.ref("id");
    UnboundApply<?> apply =
        Expressions.apply(Expressions.function("bucket"), ImmutableList.of(16, ref));

    assertThat(apply.arguments()).hasSize(2);
    assertThat(apply.arguments().get(0)).isInstanceOf(Literal.class);
    assertThat(((Literal<?>) apply.arguments().get(0)).value()).isEqualTo(16);
    assertThat(apply.arguments().get(1)).isSameAs(ref);
  }

  @Test
  public void testValueExpressionAndPredicateArgumentsArePreserved() {
    UnboundTerm<?> nested =
        Expressions.apply(Expressions.function("year"), ImmutableList.of(Expressions.ref("ts")));
    Expression predicate = Expressions.isNull("id");
    UnboundTerm<?> ref = Expressions.ref("id");
    UnboundApply<?> apply =
        Expressions.apply(
            Expressions.function("if_else"), ImmutableList.of(predicate, nested, ref));

    assertThat(apply.arguments()).containsExactly(predicate, nested, ref);
  }

  @Test
  public void testLiteralArgumentsArePreserved() {
    Literal<Integer> lit = Expressions.lit(16);
    UnboundApply<?> apply =
        Expressions.apply(Expressions.function("bucket"), ImmutableList.of(lit));

    assertThat(apply.arguments()).containsExactly(lit);
  }

  @Test
  public void testNullArgumentIsRejected() {
    assertThatThrownBy(
            () ->
                Expressions.apply(
                    Expressions.function("my_func"), Collections.singletonList((Object) null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function argument: null");
  }

  @Test
  public void testArgumentThatIsNotAnExpressionIsRejected() {
    assertThatThrownBy(
            () ->
                Expressions.apply(
                    Expressions.function("my_func"),
                    Arrays.asList((Object) LocalDate.parse("2024-01-01"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create expression literal from java.time.LocalDate: 2024-01-01");
  }

  @Test
  public void testNullFunctionIsRejected() {
    assertThatThrownBy(() -> Expressions.apply(null, ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function: null");
  }

  @Test
  public void testRefIsNotSupported() {
    UnboundApply<?> apply =
        Expressions.apply(Expressions.function("my_func"), ImmutableList.of(Expressions.ref("id")));

    assertThatThrownBy(apply::ref)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot determine reference for function: my_func");
  }

  @Test
  public void testBindIsNotSupported() {
    UnboundApply<?> apply =
        Expressions.apply(
            Expressions.function("iceberg_functions", ImmutableList.of("year")),
            ImmutableList.of(Expressions.ref("id")));

    assertThatThrownBy(() -> apply.bind(STRUCT, false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot bind function: iceberg_functions.year");
  }
}
