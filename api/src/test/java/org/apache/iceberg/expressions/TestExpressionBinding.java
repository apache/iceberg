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
import static org.apache.iceberg.expressions.Expressions.extract;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestExpressionBinding {
  private static final StructType STRUCT =
      StructType.of(
          required(0, "x", Types.IntegerType.get()),
          required(1, "y", Types.IntegerType.get()),
          required(2, "z", Types.IntegerType.get()),
          required(3, "data", Types.StringType.get()),
          required(4, "var", Types.VariantType.get()),
          optional(5, "nullable", Types.IntegerType.get()),
          optional(6, "always_null", Types.UnknownType.get()));

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

  @Test
  public void testIsNullWithUnknown() {
    Expression bound = Binder.bind(STRUCT, isNull("always_null"));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    assertThat(bound).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testNotNullWithUnknown() {
    Expression bound = Binder.bind(STRUCT, notNull("always_null"));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    assertThat(bound).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void testIsNullWithNullable() {
    Expression bound = Binder.bind(STRUCT, isNull("nullable"));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.IS_NULL);
  }

  @Test
  public void testIsNullWithRequired() {
    Expression bound = Binder.bind(STRUCT, isNull("x"));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    assertThat(bound).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void testNotNullWithNullable() {
    Expression bound = Binder.bind(STRUCT, notNull("nullable"));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.NOT_NULL);
  }

  @Test
  public void testNotNullWithRequired() {
    Expression bound = Binder.bind(STRUCT, notNull("x"));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    assertThat(bound).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testIsNullWithNullableTransformedOrderPreserving() {
    Expression bound = Binder.bind(STRUCT, isNull(truncate("nullable", 10)));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.IS_NULL);
  }

  @Test
  public void testIsNullWithRequiredTransformedOrderPreserving() {
    Expression bound = Binder.bind(STRUCT, isNull(truncate("x", 10)));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    assertThat(bound).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void testNotNullWithNullableTransformedOrderPreserving() {
    Expression bound = Binder.bind(STRUCT, notNull(truncate("nullable", 10)));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.NOT_NULL);
  }

  @Test
  public void testNotNullWithRequiredTransformedOrderPreserving() {
    Expression bound = Binder.bind(STRUCT, notNull(truncate("x", 10)));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    assertThat(bound).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void testIsNullWithRequiredTransformedNonOrderPreserving() {
    Expression bound = Binder.bind(STRUCT, isNull(bucket("x", 10)));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.IS_NULL);
  }

  @Test
  public void testNotNullWithRequiredTransformedNonOrderPreserving() {
    Expression bound = Binder.bind(STRUCT, notNull(bucket("x", 10)));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.NOT_NULL);
  }

  @Test
  public void testIsNullWithRequiredVariant() {
    Expression bound = Binder.bind(STRUCT, isNull(extract("var", "$.event_id", "long")));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.IS_NULL);
  }

  @Test
  public void testNotNullWithRequiredVariant() {
    Expression bound = Binder.bind(STRUCT, notNull(extract("var", "$.event_id", "long")));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.NOT_NULL);
  }

  @Test
  public void testExtractExpressionBinding() {
    Expression bound = Binder.bind(STRUCT, lessThan(extract("var", "$.event_id", "long"), 100));
    TestHelpers.assertAllReferencesBound("BoundExtract", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.op()).isEqualTo(Expression.Operation.LT);
    assertThat(pred.asLiteralPredicate().literal().value()).isEqualTo(100L); // cast to long
    assertThat(pred.term()).as("Should use a BoundExtract").isInstanceOf(BoundExtract.class);
    BoundExtract<?> boundExtract = (BoundExtract<?>) pred.term();
    assertThat(boundExtract.ref().fieldId()).isEqualTo(4);
    assertThat(boundExtract.path()).isEqualTo("$['event_id']");
    assertThat(boundExtract.type()).isEqualTo(Types.LongType.get());
  }

  @Test
  public void testExtractExpressionNonVariant() {
    assertThatThrownBy(() -> Binder.bind(STRUCT, lessThan(extract("x", "$.event_id", "long"), 100)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot bind extract, not a variant: x");
  }

  private static final String[] VALID_PATHS =
      new String[] {
        "$", // root path
        "$.event_id",
        "$.event.id"
      };

  @ParameterizedTest
  @FieldSource("VALID_PATHS")
  public void testExtractExpressionBindingPaths(String path) {
    Expression bound = Binder.bind(STRUCT, lessThan(extract("var", path, "long"), 100));
    TestHelpers.assertAllReferencesBound("BoundExtract", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.term()).as("Should use a BoundExtract").isInstanceOf(BoundExtract.class);
  }

  private static final String[] UNSUPPORTED_PATHS =
      new String[] {
        null,
        "",
        "event_id", // missing root
        "$['event_id']", // uses bracket notation
        "$..event_id", // uses recursive descent
        "$.events[0].event_id", // uses position accessor
        "$.events.*" // uses wildcard
      };

  @ParameterizedTest
  @FieldSource("UNSUPPORTED_PATHS")
  public void testExtractBindingWithInvalidPath(String path) {
    assertThatThrownBy(() -> Binder.bind(STRUCT, lessThan(extract("var", path, "long"), 100)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("(Unsupported|Invalid) path.*");
  }

  @Test
  public void testExtractUnknown() {
    assertThatThrownBy(() -> Binder.bind(STRUCT, isNull(extract("var", "$.field", "unknown"))))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid type to extract: unknown");
  }

  private static final String[] VALID_TYPES =
      new String[] {
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "decimal(9,2)",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "timestamp_ns",
        "timestamptz_ns",
        "string",
        "uuid",
        "fixed[4]",
        "binary",
      };

  @ParameterizedTest
  @FieldSource("VALID_TYPES")
  public void testExtractBindingWithTypes(String typeName) {
    Expression bound = Binder.bind(STRUCT, notNull(extract("var", "$.field", typeName)));
    TestHelpers.assertAllReferencesBound("BoundExtract", bound);
    BoundPredicate<?> pred = TestHelpers.assertAndUnwrap(bound);
    assertThat(pred.term()).as("Should use a BoundExtract").isInstanceOf(BoundExtract.class);
    assertThat(pred.term().type()).isEqualTo(Types.fromPrimitiveString(typeName));
  }

  private static Stream<Arguments> nullCasesWithNestedStructs() {
    // the test cases specify two arguments:
    // - the first is a list of booleans that indicate whether fields in the nested sequence of
    //   structs are optional or required. For example, [true, false, true] will construct a
    //   struct like s1.s2.s3 with s1 being required, s2 being optional, and s3 being required.
    // - the second is an expression that indicates what is expected.
    return Stream.of(
        // basic fields, no struct levels
        Arguments.of(Arrays.asList(true), Expressions.alwaysFalse()),
        Arguments.of(Arrays.asList(false), Expressions.isNull("leaf")),
        // one level
        Arguments.of(Arrays.asList(true, true), Expressions.alwaysFalse()),
        Arguments.of(Arrays.asList(true, false), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(false, true), Expressions.isNull("leaf")),
        // two levels
        Arguments.of(Arrays.asList(true, true, true), Expressions.alwaysFalse()),
        Arguments.of(Arrays.asList(true, true, false), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(false, true, true), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(true, false, true), Expressions.isNull("leaf")),
        // three levels
        Arguments.of(Arrays.asList(true, true, true, true), Expressions.alwaysFalse()),
        Arguments.of(Arrays.asList(true, true, true, false), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(false, true, true, true), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(true, false, true, true), Expressions.isNull("leaf")),
        // four levels
        Arguments.of(Arrays.asList(true, true, true, true, true), Expressions.alwaysFalse()),
        Arguments.of(Arrays.asList(true, true, true, true, false), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(false, true, true, true, true), Expressions.isNull("leaf")),
        Arguments.of(Arrays.asList(true, false, false, false, true), Expressions.isNull("leaf")));
  }

  private Schema buildNestedSchema(List<Boolean> requiredFields, String leafName) {
    // Build a schema with a single nested struct with requiredFields.size() levels with the
    // following structure:
    // s1: struct(s2: struct(s3: struct(..., sn: struct(leaf: int))))
    // where each s{i} is a required struct if requiredFields.get(i) is true and an optional struct
    // if false
    Preconditions.checkArgument(
        requiredFields != null && !requiredFields.isEmpty(),
        "Invalid required fields: null or empty");
    Types.NestedField leaf =
        requiredFields.get(requiredFields.size() - 1)
            ? required(requiredFields.size(), leafName, Types.IntegerType.get())
            : optional(requiredFields.size(), leafName, Types.IntegerType.get());

    Types.StructType current = Types.StructType.of(leaf);

    for (int i = requiredFields.size() - 2; i >= 0; i--) {
      int id = i + 1;
      String name = "s" + (i + 1);
      current =
          Types.StructType.of(
              requiredFields.get(i) ? required(id, name, current) : optional(id, name, current));
    }

    return new Schema(current.fields());
  }

  @ParameterizedTest
  @MethodSource("nullCasesWithNestedStructs")
  public void testIsNullWithNestedStructs(List<Boolean> requiredFields, Expression expression) {
    String leafName = "leaf";
    Schema schema = buildNestedSchema(requiredFields, leafName);
    int leafId = requiredFields.size();
    int level = 1;
    StringBuilder pathBuilder = new StringBuilder();
    while (level < leafId) {
      pathBuilder.append("s").append(level).append(".");
      level++;
    }

    String path = pathBuilder.append(leafName).toString();
    Expression bound = Binder.bind(schema.asStruct(), isNull(path));
    TestHelpers.assertAllReferencesBound("IsNull", bound);
    assertThat(bound.op()).isEqualTo(expression.op());

    bound = Binder.bind(schema.asStruct(), Expressions.notNull(path));
    TestHelpers.assertAllReferencesBound("NotNull", bound);
    assertThat(bound.op()).isEqualTo(expression.negate().op());
  }
}
