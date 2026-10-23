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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Objects;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.spark.functions.TruncateFunction;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.UserDefinedScalarFunc;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSparkV2DeleteFilters {
  private static final Types.StructType STRUCT =
      Types.StructType.of(
          Types.NestedField.optional(1, "a", Types.IntegerType.get()),
          Types.NestedField.optional(2, "b", Types.IntegerType.get()),
          Types.NestedField.optional(3, "s", Types.StringType.get()));
  private static final Integer[] INTEGER_VALUES = {null, 0, 1, 2};

  @ParameterizedTest
  @ValueSource(strings = {"=", "<>", "<", "<=", ">", ">="})
  void comparisonsPreserveNullSemantics(String operator) {
    Predicate comparison = new Predicate(operator, expressions(reference("a"), integer(1)));
    Predicate reversed = new Predicate(operator, expressions(integer(1), reference("a")));
    for (Integer value : INTEGER_VALUES) {
      Boolean expected = value == null ? null : compare(operator, value, 1);
      assertSelects(comparison, expected, value, null, null);
      assertSelects(new Not(comparison), sqlNot(expected), value, null, null);

      Boolean expectedReversed = value == null ? null : compare(operator, 1, value);
      assertSelects(reversed, expectedReversed, value, null, null);
      assertSelects(new Not(reversed), sqlNot(expectedReversed), value, null, null);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"=", "<>", "<", "<=", ">", ">="})
  void comparisonsWithNullLiteralDoNotSelectRows(String operator) {
    Predicate comparison = new Predicate(operator, expressions(reference("a"), integer(null)));
    Predicate reversed = new Predicate(operator, expressions(integer(null), reference("a")));
    for (Integer value : INTEGER_VALUES) {
      assertSelects(comparison, null, value, null, null);
      assertSelects(new Not(comparison), null, value, null, null);
      assertSelects(reversed, null, value, null, null);
      assertSelects(new Not(reversed), null, value, null, null);
    }
  }

  @Test
  void nullSafeEqualityRemainsTwoValued() {
    for (Integer literal : new Integer[] {null, 1}) {
      Predicate comparison = new Predicate("<=>", expressions(reference("a"), integer(literal)));
      Predicate reversed = new Predicate("<=>", expressions(integer(literal), reference("a")));
      for (Integer value : INTEGER_VALUES) {
        boolean expected = Objects.equals(value, literal);
        assertSelects(comparison, expected, value, null, null);
        assertSelects(new Not(comparison), !expected, value, null, null);
        assertSelects(reversed, expected, value, null, null);
        assertSelects(new Not(reversed), !expected, value, null, null);
      }
    }
  }

  @Test
  void negatedNullChecksCanSelectNull() {
    Predicate isNull = new Predicate("IS_NULL", expressions(reference("a")));
    Predicate notNull = new Predicate("IS_NOT_NULL", expressions(reference("a")));
    for (Integer value : INTEGER_VALUES) {
      assertSelects(isNull, value == null, value, null, null);
      assertSelects(notNull, value != null, value, null, null);
      assertSelects(new Not(isNull), value != null, value, null, null);
      assertSelects(new Not(notNull), value == null, value, null, null);
    }
  }

  @Test
  void negatedPrefixDoesNotSelectNull() {
    Predicate startsWith = new Predicate("STARTS_WITH", expressions(reference("s"), string("a")));
    for (String value : new String[] {null, "", "a1", "b"}) {
      Boolean expected = value == null ? null : value.startsWith("a");
      assertSelects(startsWith, expected, null, null, value);
      assertSelects(new Not(startsWith), sqlNot(expected), null, null, value);
      assertSelects(new Not(new Not(startsWith)), expected, null, null, value);
    }
  }

  @Test
  void inPredicatesPreserveNullSemantics() {
    for (Integer[] literals : new Integer[][] {{1}, {1, 2}, {1, null}, {null}, {1, 1}}) {
      Predicate in = in(literals);
      for (Integer value : INTEGER_VALUES) {
        Boolean expected = sqlIn(value, literals);
        assertSelects(in, expected, value, null, null);
        assertSelects(new Not(in), sqlNot(expected), value, null, null);
        assertSelects(new Not(new Not(in)), expected, value, null, null);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void booleanCompositionPreservesUnknown(boolean conjunction) {
    Predicate left = new Predicate("=", expressions(reference("a"), integer(1)));
    Predicate right = new Predicate("=", expressions(reference("b"), integer(1)));
    Predicate compound = conjunction ? new And(left, right) : new Or(left, right);
    for (Integer a : INTEGER_VALUES) {
      for (Integer b : INTEGER_VALUES) {
        Boolean leftValue = sqlEquals(a, 1);
        Boolean rightValue = sqlEquals(b, 1);
        Boolean expected =
            conjunction ? sqlAnd(leftValue, rightValue) : sqlOr(leftValue, rightValue);
        assertSelects(compound, expected, a, b, null);
        assertSelects(new Not(compound), sqlNot(expected), a, b, null);
        assertSelects(new Not(new Not(compound)), expected, a, b, null);
        assertSelects(new Not(new Not(new Not(compound))), sqlNot(expected), a, b, null);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void inContainingNullComposesThroughNegation(boolean conjunction) {
    Integer[] literals = {1, null};
    Predicate left = in(literals);
    Predicate right = new Predicate("=", expressions(reference("b"), integer(1)));
    Predicate compound = conjunction ? new And(left, right) : new Or(left, right);
    for (Integer a : INTEGER_VALUES) {
      for (Integer b : INTEGER_VALUES) {
        Boolean leftValue = sqlIn(a, literals);
        Boolean rightValue = sqlEquals(b, 1);
        Boolean expected =
            conjunction ? sqlAnd(leftValue, rightValue) : sqlOr(leftValue, rightValue);
        assertSelects(compound, expected, a, b, null);
        assertSelects(new Not(compound), sqlNot(expected), a, b, null);
      }
    }
  }

  @Test
  void transformedComparisonDoesNotSelectNull() {
    ScalarFunction<UTF8String> truncate = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc term =
        new UserDefinedScalarFunc(
            truncate.name(), truncate.canonicalName(), expressions(integer(1), reference("s")));
    Predicate comparison = new Predicate("<>", expressions(term, string("a")));
    for (String value : new String[] {null, "", "a1", "b"}) {
      String truncated = value == null ? null : value.substring(0, Math.min(1, value.length()));
      Boolean expected = truncated == null ? null : !truncated.equals("a");
      assertSelects(comparison, expected, null, null, value);
      assertSelects(new Not(comparison), sqlNot(expected), null, null, value);
    }
  }

  @Test
  void predicateArraysSelectOnlyRowsMatchingEveryPredicate() {
    Predicate left = new Predicate("=", expressions(reference("a"), integer(1)));
    Predicate right = new Predicate("=", expressions(reference("b"), integer(1)));
    Evaluator evaluator =
        new Evaluator(STRUCT, SparkV2Filters.convertForDelete(new Predicate[] {left, right}));
    for (Integer a : INTEGER_VALUES) {
      for (Integer b : INTEGER_VALUES) {
        assertThat(evaluator.eval(Row.of(a, b, null)))
            .as("a=%s, b=%s", a, b)
            .isEqualTo(Boolean.TRUE.equals(sqlAnd(sqlEquals(a, 1), sqlEquals(b, 1))));
      }
    }
  }

  @Test
  void emptyPredicateArrayMatchesEveryRow() {
    Evaluator evaluator =
        new Evaluator(STRUCT, SparkV2Filters.convertForDelete(new Predicate[] {}));
    assertThat(evaluator.eval(Row.of(null, null, null))).isTrue();
    assertThat(evaluator.eval(Row.of(1, 2, "a"))).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void constantPredicatesCanBeNegated(boolean value) {
    Predicate predicate = new Predicate(value ? "ALWAYS_TRUE" : "ALWAYS_FALSE", expressions());
    assertSelects(predicate, value, null, null, null);
    assertSelects(new Not(predicate), !value, null, null, null);
  }

  @Test
  void emptyInRequiresRowLevelEvaluation() {
    Predicate emptyIn = in(new Integer[] {});
    assertThat(SparkV2Filters.convertForDelete(emptyIn)).isNull();
    assertThat(SparkV2Filters.convertForDelete(new Not(emptyIn))).isNull();
  }

  @Test
  void unsupportedPredicatePreventsMetadataConversion() {
    Predicate unsupported = new Predicate("UNSUPPORTED", expressions(reference("a")));
    Predicate comparison = new Predicate("=", expressions(reference("b"), integer(1)));
    assertThat(SparkV2Filters.convertForDelete(unsupported)).isNull();
    assertThat(SparkV2Filters.convertForDelete(new Not(unsupported))).isNull();
    assertThat(SparkV2Filters.convertForDelete(new And(unsupported, comparison))).isNull();
    assertThat(SparkV2Filters.convertForDelete(new Or(comparison, unsupported))).isNull();
    assertThat(SparkV2Filters.convertForDelete(new Predicate[] {comparison, unsupported})).isNull();
    assertThat(SparkV2Filters.convertForDelete(new Predicate[] {unsupported, comparison})).isNull();
  }

  private static void assertSelects(
      Predicate predicate,
      Boolean expected,
      Integer leftValue,
      Integer rightValue,
      String stringValue) {
    Evaluator evaluator =
        new Evaluator(
            STRUCT,
            Objects.requireNonNull(
                SparkV2Filters.convertForDelete(predicate),
                () -> "Expected DELETE conversion for " + predicate));
    assertThat(evaluator.eval(Row.of(leftValue, rightValue, stringValue)))
        .as("%s with a=%s, b=%s, s=%s", predicate, leftValue, rightValue, stringValue)
        .isEqualTo(Boolean.TRUE.equals(expected));
  }

  private static boolean compare(String operator, int left, int right) {
    return switch (operator) {
      case "=" -> left == right;
      case "<>" -> left != right;
      case "<" -> left < right;
      case "<=" -> left <= right;
      case ">" -> left > right;
      case ">=" -> left >= right;
      default -> throw new IllegalArgumentException("Unknown comparison: " + operator);
    };
  }

  private static Boolean sqlEquals(Integer left, Integer right) {
    return left == null || right == null ? null : left.equals(right);
  }

  private static Boolean sqlIn(Integer value, Integer[] literals) {
    if (value == null) {
      return null;
    } else if (Arrays.asList(literals).contains(value)) {
      return true;
    } else {
      return Arrays.asList(literals).contains(null) ? null : false;
    }
  }

  private static Boolean sqlNot(Boolean value) {
    return value == null ? null : !value;
  }

  private static Boolean sqlAnd(Boolean left, Boolean right) {
    if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
      return false;
    } else {
      return left == null || right == null ? null : true;
    }
  }

  private static Boolean sqlOr(Boolean left, Boolean right) {
    if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) {
      return true;
    } else {
      return left == null || right == null ? null : false;
    }
  }

  private static Predicate in(Integer[] literals) {
    Expression[] children = new Expression[literals.length + 1];
    children[0] = reference("a");
    for (int index = 0; index < literals.length; index += 1) {
      children[index + 1] = integer(literals[index]);
    }
    return new Predicate("IN", children);
  }

  private static NamedReference reference(String name) {
    return FieldReference.apply(name);
  }

  private static LiteralValue<Integer> integer(Integer value) {
    return new LiteralValue<>(value, DataTypes.IntegerType);
  }

  private static LiteralValue<UTF8String> string(String value) {
    return new LiteralValue<>(UTF8String.fromString(value), DataTypes.StringType);
  }

  private static Expression[] expressions(Expression... expressions) {
    return expressions;
  }
}
