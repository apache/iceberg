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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * This test class ensures that metrics evaluators could handle NaN as upper/lower bounds correctly.
 */
public class TestMetricsEvaluatorsNaNHandling {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "all_nan", Types.DoubleType.get()),
          required(2, "max_nan", Types.DoubleType.get()),
          optional(3, "min_max_nan", Types.FloatType.get()),
          required(4, "all_nan_null_bounds", Types.DoubleType.get()),
          optional(5, "some_nan_correct_bounds", Types.FloatType.get()));

  private static final DataFile FILE =
      new TestHelpers.TestDataFile(
          "file.avro",
          TestHelpers.Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.<Integer, Long>builder()
              .put(1, 10L)
              .put(2, 10L)
              .put(3, 10L)
              .put(4, 10L)
              .put(5, 10L)
              .buildOrThrow(),
          // null value counts
          ImmutableMap.<Integer, Long>builder()
              .put(1, 0L)
              .put(2, 0L)
              .put(3, 0L)
              .put(4, 0L)
              .put(5, 0L)
              .buildOrThrow(),
          // nan value counts
          ImmutableMap.<Integer, Long>builder().put(1, 10L).put(4, 10L).put(5, 5L).buildOrThrow(),
          // lower bounds
          ImmutableMap.<Integer, ByteBuffer>builder()
              .put(1, toByteBuffer(Types.DoubleType.get(), Double.NaN))
              .put(2, toByteBuffer(Types.DoubleType.get(), 7D))
              .put(3, toByteBuffer(Types.FloatType.get(), Float.NaN))
              .put(5, toByteBuffer(Types.FloatType.get(), 7F))
              .buildOrThrow(),
          // upper bounds
          ImmutableMap.<Integer, ByteBuffer>builder()
              .put(1, toByteBuffer(Types.DoubleType.get(), Double.NaN))
              .put(2, toByteBuffer(Types.DoubleType.get(), Double.NaN))
              .put(3, toByteBuffer(Types.FloatType.get(), Float.NaN))
              .put(5, toByteBuffer(Types.FloatType.get(), 22F))
              .buildOrThrow());

  private static final Set<BiFunction<String, Number, Expression>> LESS_THAN_EXPRESSIONS =
      ImmutableSet.of(Expressions::lessThan, Expressions::lessThanOrEqual);

  private static final Set<BiFunction<String, Number, Expression>> GREATER_THAN_EXPRESSIONS =
      ImmutableSet.of(Expressions::greaterThan, Expressions::greaterThanOrEqual);

  @Test
  public void testInclusiveMetricsEvaluatorLessThanAndLessThanOrEqual() {
    for (BiFunction<String, Number, Expression> func : LESS_THAN_EXPRESSIONS) {
      boolean shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("all_nan", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead = new InclusiveMetricsEvaluator(SCHEMA, func.apply("max_nan", 1D)).eval(FILE);
      assertThat(shouldRead).as("Should not match: 1 is smaller than lower bound").isFalse();

      shouldRead = new InclusiveMetricsEvaluator(SCHEMA, func.apply("max_nan", 10D)).eval(FILE);
      assertThat(shouldRead).as("Should match: 10 is larger than lower bound").isTrue();

      shouldRead = new InclusiveMetricsEvaluator(SCHEMA, func.apply("min_max_nan", 1F)).eval(FILE);
      assertThat(shouldRead).as("Should match: no visibility").isTrue();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("all_nan_null_bounds", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 1F))
              .eval(FILE);
      assertThat(shouldRead).as("Should not match: 1 is smaller than lower bound").isFalse();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 10F))
              .eval(FILE);
      assertThat(shouldRead).as("Should match: 10 larger than lower bound").isTrue();
    }
  }

  @Test
  public void testInclusiveMetricsEvaluatorGreaterThanAndGreaterThanOrEqual() {
    for (BiFunction<String, Number, Expression> func : GREATER_THAN_EXPRESSIONS) {
      boolean shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("all_nan", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead = new InclusiveMetricsEvaluator(SCHEMA, func.apply("max_nan", 1D)).eval(FILE);
      assertThat(shouldRead).as("Should match: upper bound is larger than 1").isTrue();

      shouldRead = new InclusiveMetricsEvaluator(SCHEMA, func.apply("max_nan", 10D)).eval(FILE);
      assertThat(shouldRead).as("Should match: upper bound is larger than 10").isTrue();

      shouldRead = new InclusiveMetricsEvaluator(SCHEMA, func.apply("min_max_nan", 1F)).eval(FILE);
      assertThat(shouldRead).as("Should match: no visibility").isTrue();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("all_nan_null_bounds", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 1F))
              .eval(FILE);
      assertThat(shouldRead).as("Should match: 1 is smaller than upper bound").isTrue();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 10F))
              .eval(FILE);
      assertThat(shouldRead).as("Should match: 10 is smaller than upper bound").isTrue();

      shouldRead =
          new InclusiveMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 30))
              .eval(FILE);
      assertThat(shouldRead).as("Should not match: 30 is greater than upper bound").isFalse();
    }
  }

  @Test
  public void testInclusiveMetricsEvaluatorEquals() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("all_nan", 1D)).eval(FILE);
    assertThat(shouldRead).as("Should not match: all nan column doesn't contain number").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("max_nan", 1D)).eval(FILE);
    assertThat(shouldRead).as("Should not match: 1 is smaller than lower bound").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("max_nan", 10D)).eval(FILE);
    assertThat(shouldRead).as("Should match: 10 is within bounds").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("min_max_nan", 1F)).eval(FILE);
    assertThat(shouldRead).as("Should match: no visibility").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("all_nan_null_bounds", 1D))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: all nan column doesn't contain number").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 1F))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: 1 is smaller than lower bound").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 10F))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: 10 is within bounds").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 30))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: 30 is greater than upper bound").isFalse();
  }

  @Test
  public void testInclusiveMetricsEvaluatorNotEquals() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.notEqual("all_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notEqual("max_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notEqual("max_nan", 10D))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notEqual("min_max_nan", 1F))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notEqual("all_nan_null_bounds", 1D))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(
                    SCHEMA, Expressions.notEqual("some_nan_correct_bounds", 1F))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(
                    SCHEMA, Expressions.notEqual("some_nan_correct_bounds", 10F))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(
                    SCHEMA, Expressions.notEqual("some_nan_correct_bounds", 30))
                .eval(FILE);
    assertThat(shouldRead).as("Should match: no visibility").isTrue();
  }

  @Test
  public void testInclusiveMetricsEvaluatorIn() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.in("all_nan", 1D, 10D, 30D)).eval(FILE);
    assertThat(shouldRead).as("Should not match: all nan column doesn't contain number").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.in("max_nan", 1D, 10D, 30D)).eval(FILE);
    assertThat(shouldRead).as("Should match: 10 and 30 are greater than lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.in("min_max_nan", 1F, 10F, 30F))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: no visibility").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.in("all_nan_null_bounds", 1D, 10D, 30D))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: all nan column doesn't contain number").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(
                SCHEMA, Expressions.in("some_nan_correct_bounds", 1F, 10F, 30F))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: 10 within bounds").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.in("some_nan_correct_bounds", 1F, 30F))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: 1 not within bounds").isFalse();
  }

  @Test
  public void testInclusiveMetricsEvaluatorNotIn() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, Expressions.notIn("all_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notIn("max_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notIn("max_nan", 10D)).eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notIn("min_max_nan", 1F))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(SCHEMA, Expressions.notIn("all_nan_null_bounds", 1D))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(
                    SCHEMA, Expressions.notIn("some_nan_correct_bounds", 1F))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(
                    SCHEMA, Expressions.notIn("some_nan_correct_bounds", 10F))
                .eval(FILE);
    shouldRead =
        shouldRead
            & new InclusiveMetricsEvaluator(
                    SCHEMA, Expressions.notIn("some_nan_correct_bounds", 30))
                .eval(FILE);
    assertThat(shouldRead).as("Should match: no visibility").isTrue();
  }

  @Test
  public void testStrictMetricsEvaluatorLessThanAndLessThanOrEqual() {
    for (BiFunction<String, Number, Expression> func : LESS_THAN_EXPRESSIONS) {
      boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("all_nan", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("max_nan", 10D)).eval(FILE);
      assertThat(shouldRead).as("Should not match: 10 is less than upper bound").isFalse();

      shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("min_max_nan", 1F)).eval(FILE);
      assertThat(shouldRead).as("Should not match: no visibility").isFalse();

      shouldRead =
          new StrictMetricsEvaluator(SCHEMA, func.apply("all_nan_null_bounds", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead =
          new StrictMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 30F)).eval(FILE);
      assertThat(shouldRead).as("Should not match: nan value exists").isFalse();
    }
  }

  @Test
  public void testStrictMetricsEvaluatorGreaterThanAndGreaterThanOrEqual() {
    for (BiFunction<String, Number, Expression> func : GREATER_THAN_EXPRESSIONS) {
      boolean shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("all_nan", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("max_nan", 1D)).eval(FILE);
      assertThat(shouldRead).as("Should match: 1 is smaller than lower bound").isTrue();

      shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("max_nan", 10D)).eval(FILE);
      assertThat(shouldRead).as("Should not match: 10 is larger than lower bound").isFalse();

      shouldRead = new StrictMetricsEvaluator(SCHEMA, func.apply("min_max_nan", 1F)).eval(FILE);
      assertThat(shouldRead).as("Should not match: no visibility").isFalse();

      shouldRead =
          new StrictMetricsEvaluator(SCHEMA, func.apply("all_nan_null_bounds", 1D)).eval(FILE);
      assertThat(shouldRead)
          .as("Should not match: all nan column doesn't contain number")
          .isFalse();

      shouldRead =
          new StrictMetricsEvaluator(SCHEMA, func.apply("some_nan_correct_bounds", 30)).eval(FILE);
      assertThat(shouldRead).as("Should not match: nan value exists").isFalse();
    }
  }

  @Test
  public void testStrictMetricsEvaluatorNotEquals() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("all_nan", 1D)).eval(FILE);
    assertThat(shouldRead).as("Should match: all nan column doesn't contain number").isTrue();

    shouldRead = new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("max_nan", 1D)).eval(FILE);
    assertThat(shouldRead).as("Should match: 1 is smaller than lower bound").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("max_nan", 10D)).eval(FILE);
    assertThat(shouldRead).as("Should not match: 10 is within bounds").isFalse();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("min_max_nan", 1F)).eval(FILE);
    assertThat(shouldRead).as("Should not match: no visibility").isFalse();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("all_nan_null_bounds", 1D))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: all nan column doesn't contain number").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("some_nan_correct_bounds", 1F))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: 1 is smaller than lower bound").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("some_nan_correct_bounds", 10F))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: 10 is within bounds").isFalse();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notEqual("some_nan_correct_bounds", 30))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: 30 is greater than upper bound").isTrue();
  }

  @Test
  public void testStrictMetricsEvaluatorEquals() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.equal("all_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("max_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("max_nan", 10D)).eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("min_max_nan", 1F)).eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("all_nan_null_bounds", 1D))
                .eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 1F))
                .eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 10F))
                .eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 30))
                .eval(FILE);
    assertThat(shouldRead).as("Should not match: bounds not equal to given value").isFalse();
  }

  @Test
  public void testStrictMetricsEvaluatorNotIn() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("all_nan", 1D, 10D, 30D)).eval(FILE);
    assertThat(shouldRead).as("Should match: all nan column doesn't contain number").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("max_nan", 1D, 10D, 30D)).eval(FILE);
    assertThat(shouldRead).as("Should not match: 10 and 30 are greater than lower bound").isFalse();

    shouldRead = new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("max_nan", 1D)).eval(FILE);
    assertThat(shouldRead).as("Should match: 1 is less than lower bound").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("min_max_nan", 1F, 10F, 30F))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: no visibility").isFalse();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("all_nan_null_bounds", 1D, 10D, 30D))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: all nan column doesn't contain number").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, Expressions.notIn("some_nan_correct_bounds", 1F, 10F, 30F))
            .eval(FILE);
    assertThat(shouldRead).as("Should not match: 10 within bounds").isFalse();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("some_nan_correct_bounds", 1D))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: 1 not within bounds").isTrue();

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.notIn("some_nan_correct_bounds", 30D))
            .eval(FILE);
    assertThat(shouldRead).as("Should match: 30 not within bounds").isTrue();
  }

  @Test
  public void testStrictMetricsEvaluatorIn() {
    boolean shouldRead =
        new StrictMetricsEvaluator(SCHEMA, Expressions.in("all_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead | new StrictMetricsEvaluator(SCHEMA, Expressions.in("max_nan", 1D)).eval(FILE);
    shouldRead =
        shouldRead | new StrictMetricsEvaluator(SCHEMA, Expressions.in("max_nan", 10D)).eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.in("min_max_nan", 1F)).eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.in("all_nan_null_bounds", 1D))
                .eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.in("some_nan_correct_bounds", 1F))
                .eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.in("some_nan_correct_bounds", 10F))
                .eval(FILE);
    shouldRead =
        shouldRead
            | new StrictMetricsEvaluator(SCHEMA, Expressions.equal("some_nan_correct_bounds", 30))
                .eval(FILE);
    assertThat(shouldRead).as("Should not match: bounds not equal to given value").isFalse();
  }
}
