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

import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestIDReference {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(34, "a", Types.IntegerType.get()),
          Types.NestedField.required(35, "s", Types.StringType.get()));

  @Test
  public void testIDReferenceBind() {
    IDReference<Integer> ref = new IDReference<>("a", 34);
    BoundReference<Integer> bound = ref.bind(SCHEMA.asStruct(), true);

    assertThat(bound).isInstanceOf(BoundReference.class);
    assertThat(bound.fieldId()).isEqualTo(34);
    assertThat(bound.name()).isEqualTo("a");
    assertThat(bound.type()).isEqualTo(Types.IntegerType.get());
  }

  @Test
  public void testIDReferenceBindIgnoresCaseSensitivity() {
    IDReference<Integer> ref = new IDReference<>("A", 34);

    // Should work regardless of case sensitivity since we use fieldId
    BoundReference<Integer> bound1 = ref.bind(SCHEMA.asStruct(), true);
    BoundReference<Integer> bound2 = ref.bind(SCHEMA.asStruct(), false);

    assertThat(bound1).isInstanceOf(BoundReference.class);
    assertThat(bound2).isInstanceOf(BoundReference.class);
    assertThat(bound1.fieldId()).isEqualTo(34);
    assertThat(bound2.fieldId()).isEqualTo(34);
  }

  @Test
  public void testIDReferenceBindWithInvalidId() {
    IDReference<Integer> ref = new IDReference<>("invalid", 999);

    assertThatThrownBy(() -> ref.bind(SCHEMA.asStruct(), true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Cannot find field by id 999 in struct: struct<34: a: optional int, 35: s: required string>");
  }

  @Test
  public void testIDReferenceRef() {
    IDReference<Integer> ref = new IDReference<>("a", 34);
    NamedReference<?> namedRef = ref.ref();

    assertThat(namedRef.name()).isEqualTo("a");
  }

  @Test
  public void testIDReferenceToString() {
    IDReference<Integer> ref = new IDReference<>("a", 34);

    assertThat(ref.toString()).isEqualTo("ref(name=\"a\", id=\"34\")");
  }

  @Test
  public void testIDReferenceExpressionIntegration() {
    // Test that IDReference works in expression predicates
    UnboundPredicate<?> expr = Expressions.equal(Expressions.ref("a", 34), 5);
    assertThat(expr).isInstanceOf(UnboundPredicate.class);

    assertThat(expr.term()).isInstanceOf(IDReference.class);

    IDReference<?> idRef = (IDReference<?>) expr.term();
    assertThat(idRef.name()).isEqualTo("a");
    assertThat(idRef.id()).isEqualTo(34);
  }

  @Test
  public void testIDReferenceUnbind() {
    // Test that unbinding a bound reference returns a NamedReference for compatibility
    Expression expr = Expressions.equal(Expressions.ref("a", 34), 5);
    Expression boundExpr = Binder.bind(SCHEMA.asStruct(), expr, true);

    assertThat(boundExpr).isInstanceOf(BoundPredicate.class);
    BoundPredicate<?> boundPred = (BoundPredicate<?>) boundExpr;

    UnboundTerm<?> unbound = ExpressionUtil.unbind(boundPred.term());
    assertThat(unbound).isInstanceOf(NamedReference.class);

    NamedReference<?> namedRef = (NamedReference<?>) unbound;
    assertThat(namedRef.name()).isEqualTo("a");
  }
}
