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

import java.util.Arrays;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestFunctionReference {

  @Test
  public void testName() {
    assertThat(Expressions.function("bucket").name()).isEqualTo("bucket");
    assertThat(Expressions.function(ImmutableList.of("ns", "sub", "func")).name())
        .isEqualTo("func");
  }

  @Test
  public void testIdentifier() {
    FunctionReference ref = Expressions.function("cat", ImmutableList.of("ns", "func"));
    assertThat(ref.catalog()).isEqualTo("cat");
    assertThat(ref.identifier()).containsExactly("ns", "func");
    assertThat(Expressions.function("func").catalog()).isNull();
  }

  @Test
  public void testToString() {
    assertThat(Expressions.function("func")).hasToString("func");
    assertThat(Expressions.function(ImmutableList.of("ns", "func"))).hasToString("ns.func");
    assertThat(Expressions.function("cat", ImmutableList.of("ns", "func")))
        .hasToString("cat.ns.func");
  }

  @Test
  public void testInvalidIdentifier() {
    assertThatThrownBy(() -> Expressions.function(ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function identifier: []");

    assertThatThrownBy(() -> Expressions.function("cat", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function identifier: null");

    assertThatThrownBy(() -> Expressions.function(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function identifier (empty or null part): []");

    assertThatThrownBy(() -> Expressions.function(ImmutableList.of("ns", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid function identifier (empty or null part): [ns, ]");
  }

  @Test
  public void testJavaSerialization() throws Exception {
    for (FunctionReference ref : references()) {
      FunctionReference roundTripped = TestHelpers.roundTripSerialize(ref);
      assertThat(roundTripped).isEqualTo(ref);
      assertThat(roundTripped.catalog()).isEqualTo(ref.catalog());
      assertThat(roundTripped.identifier()).isEqualTo(ref.identifier());
    }
  }

  @Test
  public void testKryoSerialization() throws Exception {
    for (FunctionReference ref : references()) {
      FunctionReference roundTripped = TestHelpers.KryoHelpers.roundTripSerialize(ref);
      assertThat(roundTripped).isEqualTo(ref);
      assertThat(roundTripped.catalog()).isEqualTo(ref.catalog());
      assertThat(roundTripped.identifier()).isEqualTo(ref.identifier());
    }
  }

  private static Iterable<FunctionReference> references() {
    return Arrays.asList(
        Expressions.function("bucket"),
        Expressions.function(ImmutableList.of("ns", "func")),
        Expressions.function("iceberg_functions", ImmutableList.of("bucket")));
  }
}
