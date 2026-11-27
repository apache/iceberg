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

import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestLegacyOrcNestedNullMetrics {
  private static final Schema SCHEMA =
      new Schema(
          optional(
              1,
              "parent",
              Types.StructType.of(
                  optional(2, "leaf", Types.IntegerType.get()),
                  required(
                      3,
                      "inner",
                      Types.StructType.of(required(4, "leaf", Types.IntegerType.get()))))),
          required(5, "stable", Types.StructType.of(optional(6, "leaf", Types.IntegerType.get()))),
          optional(7, "simple", Types.IntegerType.get()));

  private static DataFile file(String filename, long nestedValueCount) {
    Map<Integer, Long> valueCounts = Map.of(2, nestedValueCount, 4, nestedValueCount, 6, 2L, 7, 2L);
    Map<Integer, Long> nullCounts = Map.of(2, 0L, 4, 0L, 6, 0L, 7, 0L);
    Map<Integer, ByteBuffer> bounds =
        Map.of(
            2, toByteBuffer(Types.IntegerType.get(), 10),
            4, toByteBuffer(Types.IntegerType.get(), 20),
            6, toByteBuffer(Types.IntegerType.get(), 30),
            7, toByteBuffer(Types.IntegerType.get(), 40));
    return new TestDataFile(filename, Row.of(), 2L, valueCounts, nullCounts, null, bounds, bounds);
  }

  private static boolean mayMatch(DataFile file, Expression expression) {
    return new InclusiveMetricsEvaluator(SCHEMA, expression).eval(file);
  }

  @Test
  public void testLegacyOrcCannotPruneNullsFromAbsentParent() {
    // Legacy OrcMetrics used hasNull() == false to write zero nulls even though it
    // counted only the child values beneath the one present parent (one of two rows).
    DataFile legacy = file("legacy.orc", 1L);
    assertThat(mayMatch(legacy, isNull("parent.leaf"))).isTrue();
    assertThat(mayMatch(legacy, isNull("parent.inner.leaf"))).isTrue();
    assertThat(mayMatch(legacy, notEqual("parent.leaf", 10))).isTrue();
    assertThat(mayMatch(legacy, notIn("parent.leaf", 10))).isTrue();
  }

  @Test
  public void testValidOrcAndOtherFormatsStillPrune() {
    // The fix must not globally turn off statistics pruning for nested ORC fields.
    DataFile healthy = file("healthy.orc", 2L);
    assertThat(mayMatch(healthy, isNull("parent.leaf"))).isFalse();
    assertThat(mayMatch(healthy, notEqual("parent.leaf", 10))).isFalse();
    assertThat(mayMatch(healthy, notIn("parent.leaf", 10))).isFalse();

    DataFile legacyParquet = file("file.parquet", 1L);
    assertThat(mayMatch(legacyParquet, isNull("parent.leaf"))).isFalse();

    DataFile legacy = file("legacy.orc", 1L);
    assertThat(mayMatch(legacy, isNull("stable.leaf"))).isFalse();
    assertThat(mayMatch(legacy, isNull("simple"))).isFalse();
  }

  @Test
  public void testMissingChildValueCountsCannotProveNoNulls() {
    DataFile missingCounts =
        new TestDataFile("legacy.orc", Row.of(), 2L, null, Map.of(2, 0L), null, null, null);
    assertThat(mayMatch(missingCounts, isNull("parent.leaf"))).isTrue();
  }

  @Test
  public void testLegacyOrcNullParentStillMatchesNotNaN() {
    Schema floatSchema =
        new Schema(
            optional(
                10, "parent", Types.StructType.of(optional(11, "leaf", Types.FloatType.get()))));
    DataFile legacyAllObservedNaN =
        new TestDataFile(
            "legacy.orc", Row.of(), 2L, Map.of(11, 1L), Map.of(11, 0L), Map.of(11, 1L), null, null);
    DataFile legacyWithNoPresentChildren =
        new TestDataFile(
            "all-null-parents.orc",
            Row.of(),
            2L,
            Map.of(11, 0L),
            Map.of(11, 0L),
            Map.of(11, 0L),
            null,
            null);
    DataFile healthyAllNaN =
        new TestDataFile(
            "healthy.orc",
            Row.of(),
            2L,
            Map.of(11, 2L),
            Map.of(11, 0L),
            Map.of(11, 2L),
            null,
            null);

    // A NULL value matches NOT NaN in Iceberg's evaluator.
    assertThat(
            new InclusiveMetricsEvaluator(floatSchema, notNaN("parent.leaf"))
                .eval(legacyAllObservedNaN))
        .isTrue();
    assertThat(
            new InclusiveMetricsEvaluator(floatSchema, notNaN("parent.leaf"))
                .eval(legacyWithNoPresentChildren))
        .isTrue();
    assertThat(
            new InclusiveMetricsEvaluator(floatSchema, notNaN("parent.leaf")).eval(healthyAllNaN))
        .isFalse();
  }
}
