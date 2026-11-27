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
package org.apache.iceberg.transforms;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTruncatesProjection {

  @SuppressWarnings("unchecked")
  public void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    assertThat(predicate.op())
        .isEqualTo(expectedOp)
        .as("Strict projection never runs for IN")
        .isNotEqualTo(Expression.Operation.IN);

    Transform<Object, Object> transform =
        (Transform<Object, Object>) spec.getFieldsBySourceId(1).get(0).transform();
    Type type = spec.partitionType().field(spec.getFieldsBySourceId(1).get(0).fieldId()).type();
    if (predicate.op() == Expression.Operation.NOT_IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(v -> transform.toHumanString(type, v))
              .collect(Collectors.toList())
              .toString();
      assertThat(actual).isEqualTo(expectedLiteral);
    } else {
      Literal<?> literal = predicate.literal();
      String output = transform.toHumanString(type, literal.value());
      assertThat(output).isEqualTo(expectedLiteral);
    }
  }

  public void assertProjectionStrictValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {

    Expression projection = Projections.strict(spec).project(filter);
    assertThat(expectedOp).isEqualTo(projection.op());
  }

  public void assertProjectionInclusiveValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {

    Expression projection = Projections.inclusive(spec).project(filter);
    assertThat(expectedOp).isEqualTo(projection.op());
  }

  @SuppressWarnings("unchecked")
  public void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    assertThat(predicate.op())
        .as("Operation should match")
        .isEqualTo(expectedOp)
        .as("Inclusive projection never runs for NOT_IN")
        .isNotEqualTo(Expression.Operation.NOT_IN);

    Transform<Object, Object> transform =
        (Transform<Object, Object>) spec.getFieldsBySourceId(1).get(0).transform();
    Type type = spec.partitionType().field(spec.getFieldsBySourceId(1).get(0).fieldId()).type();
    if (predicate.op() == Expression.Operation.IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(v -> transform.toHumanString(type, v))
              .collect(Collectors.toList())
              .toString();
      assertThat(actual).isEqualTo(expectedLiteral);
    } else {
      Literal<?> literal = predicate.literal();
      String output = transform.toHumanString(type, literal.value());
      assertThat(output).isEqualTo(expectedLiteral);
    }
  }

  @Test
  public void testIntegerStrictLowerBound() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), true, 90, true);
    assertProjectionEvaluation(spec, lessThan("value", value), true, 100, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 90, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 100, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 100, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 110, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 90, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 100, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 100, false);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 110, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, null, true);
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec,
        notIn("value", value - 1, value, value + 1),
        Expression.Operation.NOT_IN,
        "[90, 100, 100]");
    assertProjectionStrictValue(spec, in("value", value, value + 1), Expression.Operation.FALSE);
  }

  @Test
  public void testIntegerStrictUpperBound() {
    Integer value = 99;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), true, 80, true);
    assertProjectionEvaluation(spec, lessThan("value", value), true, 90, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 90, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 100, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 90, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 100, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 90, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 100, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 90, false);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 100, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, null, true);
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec,
        notIn("value", value - 1, value, value + 1),
        Expression.Operation.NOT_IN,
        "[90, 90, 100]");
    assertProjectionStrictValue(spec, in("value", value, value - 1), Expression.Operation.FALSE);
  }

  @Test
  public void testIntegerInclusiveLowerBound() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), false, 90, true);
    assertProjectionEvaluation(spec, lessThan("value", value), false, 100, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 100, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 110, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 90, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 100, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 90, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 100, true);
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "100");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value - 1, value, value + 1), Expression.Operation.IN, "[90, 100, 100]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value + 1), Expression.Operation.TRUE);
  }

  @Test
  public void testIntegerInclusiveUpperBound() {
    Integer value = 99;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), false, 90, true);
    assertProjectionEvaluation(spec, lessThan("value", value), false, 100, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 90, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 100, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 90, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 100, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 80, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 90, true);
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "90");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value - 1, value, value + 1), Expression.Operation.IN, "[90, 90, 100]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value - 1), Expression.Operation.TRUE);
  }

  @Test
  public void testLongStrictLowerBound() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), true, 90L, true);
    assertProjectionEvaluation(spec, lessThan("value", value), true, 100L, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 90L, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 100L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 100L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 110L, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 90L, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 100L, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 100L, false);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 110L, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, null, true);
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec,
        notIn("value", value - 1, value, value + 1),
        Expression.Operation.NOT_IN,
        "[90, 100, 100]");
    assertProjectionStrictValue(spec, in("value", value, value + 1), Expression.Operation.FALSE);
  }

  @Test
  public void testLongStrictUpperBound() {
    Long value = 99L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), true, 80L, true);
    assertProjectionEvaluation(spec, lessThan("value", value), true, 90L, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 90L, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), true, 100L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 90L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), true, 100L, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 90L, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), true, 100L, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 90L, false);
    assertProjectionEvaluation(spec, notEqual("value", value), true, 100L, true);
    assertProjectionEvaluation(spec, notEqual("value", value), true, null, true);
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec,
        notIn("value", value - 1, value, value + 1),
        Expression.Operation.NOT_IN,
        "[90, 90, 100]");
    assertProjectionStrictValue(spec, in("value", value, value - 1), Expression.Operation.FALSE);
  }

  @Test
  public void testLongInclusiveLowerBound() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), false, 90L, true);
    assertProjectionEvaluation(spec, lessThan("value", value), false, 100L, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 100L, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 110L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 90L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 100L, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 90L, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 100L, true);
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "100");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value - 1, value, value + 1), Expression.Operation.IN, "[90, 100, 100]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value + 1), Expression.Operation.TRUE);
  }

  @Test
  public void testLongInclusiveUpperBound() {
    Long value = 99L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionEvaluation(spec, lessThan("value", value), false, 90L, true);
    assertProjectionEvaluation(spec, lessThan("value", value), false, 100L, false);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 90L, true);
    assertProjectionEvaluation(spec, lessThanOrEqual("value", value), false, 100L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 90L, false);
    assertProjectionEvaluation(spec, greaterThan("value", value), false, 100L, true);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 80L, false);
    assertProjectionEvaluation(spec, greaterThanOrEqual("value", value), false, 90L, true);
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "90");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value - 1, value, value + 1), Expression.Operation.IN, "[90, 90, 100]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value - 1), Expression.Operation.TRUE);
  }

  private static Stream<Arguments> integerTypes() {
    return Stream.of(
        Arguments.of(Types.IntegerType.get(), (long) Integer.MIN_VALUE),
        Arguments.of(Types.LongType.get(), Long.MIN_VALUE));
  }

  private static Object sourceValue(Type type, long value) {
    if (type.typeId() == Type.TypeID.INTEGER) {
      return (int) value;
    }

    return value;
  }

  @SuppressWarnings("unchecked")
  private static Object transform(PartitionSpec spec, Type type, long value) {
    Transform<Object, Object> transform =
        (Transform<Object, Object>) spec.fields().get(0).transform();
    return transform.bind(type).apply(sourceValue(type, value));
  }

  private static long firstNormalPartition(long minimum, int width) {
    return minimum + (width - Math.floorMod(minimum, width));
  }

  private static boolean evalProjection(
      PartitionSpec spec, UnboundPredicate<?> filter, boolean strict, Object partitionValue) {
    Expression projection =
        strict
            ? Projections.strict(spec).project(filter)
            : Projections.inclusive(spec).project(filter);
    return new Evaluator(spec.partitionType(), projection, true)
        .eval(TestHelpers.Row.of(partitionValue));
  }

  private static void assertProjectionEvaluation(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      boolean strict,
      Object partitionValue,
      boolean expected) {
    assertThat(evalProjection(spec, filter, strict, partitionValue)).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void inclusiveLessThanIncludesWrappedPartition(Type type, long minimum) {
    int width = 1000;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object wrapped = transform(spec, type, minimum);

    assertThat(evalProjection(spec, lessThan("value", 0), false, wrapped)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void inclusiveLessThanOrEqualToIncludesWrappedPartition(Type type, long minimum) {
    int width = 1000;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object wrapped = transform(spec, type, minimum);

    assertThat(evalProjection(spec, lessThanOrEqual("value", 0), false, wrapped)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void inclusiveGreaterThanIncludesFirstNormalPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object firstNormal = transform(spec, type, firstNormalPartition(minimum, width));

    assertThat(
            evalProjection(
                spec, greaterThan("value", sourceValue(type, minimum)), false, firstNormal))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void inclusiveGreaterThanOrEqualToIncludesFirstNormalPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object firstNormal = transform(spec, type, firstNormalPartition(minimum, width));

    assertThat(
            evalProjection(
                spec, greaterThanOrEqual("value", sourceValue(type, minimum)), false, firstNormal))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void strictGreaterThanExcludesWrappedPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object wrapped = transform(spec, type, minimum);

    assertThat(evalProjection(spec, greaterThan("value", 99), true, wrapped)).isFalse();
    assertThat(evalProjection(spec, greaterThanOrEqual("value", 99), true, wrapped)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void strictLessThanExcludesFirstNormalPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object firstNormal = transform(spec, type, firstNormalPartition(minimum, width));

    assertThat(
            evalProjection(spec, lessThan("value", sourceValue(type, minimum)), true, firstNormal))
        .isFalse();
    assertThat(
            evalProjection(
                spec, lessThanOrEqual("value", sourceValue(type, minimum)), true, firstNormal))
        .isFalse();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void strictGreaterThanOrEqualToIncludesFirstNormalPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object firstNormal = transform(spec, type, firstNormalPartition(minimum, width));

    assertThat(
            evalProjection(
                spec,
                greaterThanOrEqual(
                    "value", sourceValue(type, firstNormalPartition(minimum, width))),
                true,
                firstNormal))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void inclusiveGreaterThanExcludesWrappedPartition(Type type, long minimum) {
    int width = 1000;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object wrapped = transform(spec, type, minimum);

    assertThat(evalProjection(spec, greaterThan("value", 0), false, wrapped)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void inclusiveGreaterThanOrEqualToExcludesWrappedPartition(Type type, long minimum) {
    int width = 1000;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();
    Object wrapped = transform(spec, type, minimum);

    assertThat(evalProjection(spec, greaterThanOrEqual("value", 0), false, wrapped)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void strictGreaterThanExcludesNullPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();

    assertThat(evalProjection(spec, greaterThan("value", sourceValue(type, minimum)), true, null))
        .isFalse();
  }

  @ParameterizedTest
  @MethodSource("integerTypes")
  void strictGreaterThanOrEqualToExcludesNullPartition(Type type, long minimum) {
    int width = 10;
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", width).build();

    assertThat(
            evalProjection(
                spec, greaterThanOrEqual("value", sourceValue(type, minimum)), true, null))
        .isFalse();
  }

  @Test
  public void testDecimalStrictLowerBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "100.00");
    assertProjectionStrict(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100.00");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "100.00");
    assertProjectionStrict(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "99.90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "100.00");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    BigDecimal delta = new BigDecimal(1);
    assertProjectionStrict(
        spec,
        notIn("value", value.add(delta), value, value.subtract(delta)),
        Expression.Operation.NOT_IN,
        "[99.00, 100.00, 101.00]");
    assertProjectionStrictValue(
        spec, in("value", value, value.add(delta)), Expression.Operation.FALSE);
  }

  @Test
  public void testDecimalStrictUpperBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("99.99").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "99.90");
    assertProjectionStrict(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100.00");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "99.90");
    assertProjectionStrict(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "99.90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "99.90");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    BigDecimal delta = new BigDecimal(1);
    assertProjectionStrict(
        spec,
        notIn("value", value.add(delta), value, value.subtract(delta)),
        Expression.Operation.NOT_IN,
        "[98.90, 99.90, 100.90]");
    assertProjectionStrictValue(
        spec, in("value", value, value.subtract(delta)), Expression.Operation.FALSE);
  }

  @Test
  public void testDecimalInclusiveLowerBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "99.90");
    assertProjectionInclusive(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "100.00");
    assertProjectionInclusive(
        spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100.00");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "100.00");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "100.00");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    BigDecimal delta = new BigDecimal(1);
    assertProjectionInclusive(
        spec,
        in("value", value.add(delta), value, value.subtract(delta)),
        Expression.Operation.IN,
        "[99.00, 100.00, 101.00]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value.add(delta)), Expression.Operation.TRUE);
  }

  @Test
  public void testDecimalInclusiveUpperBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("99.99").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "99.90");
    assertProjectionInclusive(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "99.90");
    assertProjectionInclusive(
        spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100.00");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "99.90");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "99.90");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    BigDecimal delta = new BigDecimal(1);
    assertProjectionInclusive(
        spec,
        in("value", value.add(delta), value, value.subtract(delta)),
        Expression.Operation.IN,
        "[98.90, 99.90, 100.90]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value.subtract(delta)), Expression.Operation.TRUE);
  }

  @Test
  public void testStringStrict() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "abcde");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "abcde");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "abcde");
    assertProjectionStrict(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "abcde");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "abcde");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec, notIn("value", value, value + "abc"), Expression.Operation.NOT_IN, "[abcde, abcde]");
    assertProjectionStrictValue(
        spec, in("value", value, value + "abc"), Expression.Operation.FALSE);
  }

  @Test
  public void testStringInclusive() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "abcde");
    assertProjectionInclusive(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "abcde");
    assertProjectionInclusive(
        spec, greaterThan("value", value), Expression.Operation.GT_EQ, "abcde");
    assertProjectionInclusive(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "abcde");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "abcde");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value, value + "abc"), Expression.Operation.IN, "[abcde, abcde]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value + "abc"), Expression.Operation.TRUE);
  }

  @Test
  public void testBinaryStrict() throws Exception {
    ByteBuffer value = ByteBuffer.wrap("abcdefg".getBytes(StandardCharsets.UTF_8));
    Schema schema = new Schema(optional(1, "value", Types.BinaryType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();
    String expectedValue =
        TransformUtil.base64encode(ByteBuffer.wrap("abcde".getBytes(StandardCharsets.UTF_8)));

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, expectedValue);
    assertProjectionStrict(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT, expectedValue);
    assertProjectionStrict(
        spec, greaterThan("value", value), Expression.Operation.GT, expectedValue);
    assertProjectionStrict(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT, expectedValue);
    assertProjectionStrict(
        spec, notEqual("value", value), Expression.Operation.NOT_EQ, expectedValue);
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);

    ByteBuffer anotherValue = ByteBuffer.wrap("abcdehij".getBytes(StandardCharsets.UTF_8));
    assertProjectionStrict(
        spec,
        notIn("value", value, anotherValue),
        Expression.Operation.NOT_IN,
        String.format("[%s, %s]", expectedValue, expectedValue));
    assertProjectionStrictValue(spec, in("value", value, anotherValue), Expression.Operation.FALSE);
  }

  @Test
  public void testBinaryInclusive() throws Exception {
    ByteBuffer value = ByteBuffer.wrap("abcdefg".getBytes(StandardCharsets.UTF_8));
    Schema schema = new Schema(optional(1, "value", Types.BinaryType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();
    String expectedValue =
        TransformUtil.base64encode(ByteBuffer.wrap("abcde".getBytes(StandardCharsets.UTF_8)));

    assertProjectionInclusive(
        spec, lessThan("value", value), Expression.Operation.LT_EQ, expectedValue);
    assertProjectionInclusive(
        spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, expectedValue);
    assertProjectionInclusive(
        spec, greaterThan("value", value), Expression.Operation.GT_EQ, expectedValue);
    assertProjectionInclusive(
        spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, expectedValue);
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, expectedValue);
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);

    ByteBuffer anotherValue = ByteBuffer.wrap("abcdehij".getBytes(StandardCharsets.UTF_8));
    assertProjectionInclusive(
        spec,
        in("value", value, anotherValue),
        Expression.Operation.IN,
        String.format("[%s, %s]", expectedValue, expectedValue));
    assertProjectionInclusiveValue(
        spec, notIn("value", value, anotherValue), Expression.Operation.TRUE);
  }
}
