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

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestExpressionBinding {
  private static final StructType STRUCT =
      StructType.of(
          required(0, "x", Types.IntegerType.get()),
          required(1, "y", Types.IntegerType.get()),
          required(2, "z", Types.IntegerType.get()),
          required(3, "data", Types.StringType.get()));

  @Test
  public void testMissingReference() {
    Expression expr = and(equal("t", 5), equal("x", 7));
    assertThatThrownBy(() -> Binder.bind(STRUCT, expr))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 't' in struct");
  }

  @Test
  public void testBoundExpressionFails() {
    Expression expr = not(equal("x", 7));
    assertThatThrownBy(() -> Binder.bind(STRUCT, Binder.bind(STRUCT, expr)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Found already bound predicate");
  }

  @Test
  public void testSingleReference() {
    Expression expr = not(equal("x", 7));
    TestHelpers.assertAllReferencesBound("Single reference", Binder.bind(STRUCT, expr, true));
  }

  @Test
  public void testCaseInsensitiveReference() {
    Expression expr = not(equal("X", 7));
    TestHelpers.assertAllReferencesBound("Single reference", Binder.bind(STRUCT, expr, false));
  }

  @Test
  public void testCaseSensitiveReference() {
    Expression expr = not(equal("X", 7));
    assertThatThrownBy(() -> Binder.bind(STRUCT, expr, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'X' in struct");
  }

  @Test
  public void testMultipleReferences() {
    Expression expr = or(and(equal("x", 7), lessThan("y", 100)), greaterThan("z", -100));
    TestHelpers.assertAllReferencesBound("Multiple references", Binder.bind(STRUCT, expr));
  }

  @Test
  public void testAnd() {
    Expression expr = and(equal("x", 7), lessThan("y", 100));
    Expression boundExpr = Binder.bind(STRUCT, expr);
    TestHelpers.assertAllReferencesBound("And", boundExpr);

    // make sure the result is an And
    And and = TestHelpers.assertAndUnwrap(boundExpr, And.class);

    // make sure the refs are for the right fields
    BoundPredicate<?> left = TestHelpers.assertAndUnwrap(and.left());
    assertThat(left.term().ref().fieldId()).as("Should bind x correctly").isZero();
    BoundPredicate<?> right = TestHelpers.assertAndUnwrap(and.right());
    assertThat(right.term().ref().fieldId()).as("Should bind y correctly").isOne();
  }

  @Test
  public void testOr() {
    Expression expr = or(greaterThan("z", -100), lessThan("y", 100));
    Expression boundExpr = Binder.bind(STRUCT, expr);
    TestHelpers.assertAllReferencesBound("Or", boundExpr);

    // make sure the result is an Or
    Or or = TestHelpers.assertAndUnwrap(boundExpr, Or.class);

    // make sure the refs are for the right fields
    BoundPredicate<?> left = TestHelpers.assertAndUnwrap(or.left());
    assertThat(left.term().ref().fieldId()).as("Should bind z correctly").isEqualTo(2);
    BoundPredicate<?> right = TestHelpers.assertAndUnwrap(or.right());
    assertThat(right.term().ref().fieldId()).as("Should bind y correctly").isOne();
  }

  @Test
  public void testNot() {
    Expression expr = not(equal("x", 7));
    Expression boundExpr = Binder.bind(STRUCT, expr);
    TestHelpers.assertAllReferencesBound("Not", boundExpr);

    // make sure the result is a Not
    Not not = TestHelpers.assertAndUnwrap(boundExpr, Not.class);

    // make sure the refs are for the right fields
    BoundPredicate<?> child = TestHelpers.assertAndUnwrap(not.child());
    assertThat(child.term().ref().fieldId()).as("Should bind x correctly").isZero();
  }

  @Test
  public void testStartsWith() {
    StructType struct = StructType.of(required(0, "s", Types.StringType.get()));
    Expression expr = startsWith("s", "abc");
    Expression boundExpr = Binder.bind(struct, expr);
    TestHelpers.assertAllReferencesBound("StartsWith", boundExpr);
    // make sure the expression is a StartsWith
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(boundExpr, BoundPredicate.class);
    assertThat(pred.op())
        .as("Should be right operation")
        .isEqualTo(Expression.Operation.STARTS_WITH);
    assertThat(pred.term().ref().fieldId()).as("Should bind s correctly").isZero();
  }

  @Test
  public void testNotStartsWith() {
    StructType struct = StructType.of(required(21, "s", Types.StringType.get()));
    Expression expr = notStartsWith("s", "abc");
    Expression boundExpr = Binder.bind(struct, expr);
    TestHelpers.assertAllReferencesBound("NotStartsWith", boundExpr);
    // Make sure the expression is a NotStartsWith
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(boundExpr, BoundPredicate.class);
    assertThat(pred.op())
        .as("Should be right operation")
        .isEqualTo(Expression.Operation.NOT_STARTS_WITH);
    assertThat(pred.term().ref().fieldId())
        .as("Should bind term to correct field id")
        .isEqualTo(21);
  }

  @Test
  public void testAlwaysTrue() {
    assertThat(Binder.bind(STRUCT, alwaysTrue()))
        .as("Should not change alwaysTrue")
        .isEqualTo(alwaysTrue());
  }

  @Test
  public void testAlwaysFalse() {
    assertThat(Binder.bind(STRUCT, alwaysFalse()))
        .as("Should not change alwaysFalse")
        .isEqualTo(alwaysFalse());
  }

  @Test
  public void testBasicSimplification() {
    // this tests that a basic simplification is done by calling the helpers in Expressions. those
    // are more thoroughly tested in TestExpressionHelpers.

    // the second predicate is always true once it is bound because z is an integer and the literal
    // is less than any 32-bit integer value
    assertThat(Binder.bind(STRUCT, or(lessThan("y", 100), greaterThan("z", -9999999999L))))
        .as("Should simplify or expression to alwaysTrue")
        .isEqualTo(alwaysTrue());
    // similarly, the second predicate is always false
    assertThat(Binder.bind(STRUCT, and(lessThan("y", 100), lessThan("z", -9999999999L))))
        .as("Should simplify and expression to predicate")
        .isEqualTo(alwaysFalse());

    Expression bound = Binder.bind(STRUCT, not(not(lessThan("y", 100))));
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.term().ref().fieldId()).as("Should have the correct bound field").isOne();
  }

  @Test
  public void testTransformExpressionBinding() {
    Expression bound = Binder.bind(STRUCT, equal(bucket("x", 16), 10));
    TestHelpers.assertAllReferencesBound("BoundTransform", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.term())
        .as("Should use a BoundTransform child")
        .isInstanceOf(BoundTransform.class);
    BoundTransform<?, ?> transformExpr = (BoundTransform<?, ?>) pred.term();
    assertThat(transformExpr.transform())
        .as("Should use a bucket[16] transform")
        .hasToString("bucket[16]");
  }
}
