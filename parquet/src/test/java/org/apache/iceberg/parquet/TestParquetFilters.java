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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;

class TestParquetFilters {

  private static final Map<String, Integer> ALIASES = ImmutableMap.of("id", 1);
  private static final Schema SCHEMA =
      new Schema(ImmutableList.of(required(1, "id", Types.IntegerType.get())), ALIASES);

  @Test
  void alwaysFalseReturnsNoop() {
    FilterCompat.Filter result = ParquetFilters.convert(SCHEMA, Expressions.alwaysFalse(), true);
    assertThat(result)
        .as("AlwaysFalse expression should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void alwaysTrueReturnsNoop() {
    FilterCompat.Filter result = ParquetFilters.convert(SCHEMA, Expressions.alwaysTrue(), true);
    assertThat(result)
        .as("AlwaysTrue expression should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void realPredicateReturnsFilter() {
    FilterCompat.Filter result = ParquetFilters.convert(SCHEMA, Expressions.equal("id", 42), true);
    assertThat(result)
        .as("Real predicate should produce a non-NOOP filter")
        .isNotSameAs(FilterCompat.NOOP);
  }

  @Test
  void notAlwaysFalseReturnsNoop() {
    FilterCompat.Filter result =
        ParquetFilters.convert(SCHEMA, Expressions.not(Expressions.alwaysFalse()), true);
    assertThat(result)
        .as("not(alwaysFalse) should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void andWithAlwaysFalseReturnsNoop() {
    FilterCompat.Filter result =
        ParquetFilters.convert(
            SCHEMA, Expressions.and(Expressions.alwaysFalse(), Expressions.equal("id", 42)), true);
    assertThat(result)
        .as("and(alwaysFalse, pred) should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void predicateOnMissingColumnReturnsNoop() {
    // simulates reading an older file whose schema does not contain the filtered column
    FilterCompat.Filter result =
        ParquetFilters.convert(SCHEMA, Expressions.equal("missing", 42), true);
    assertThat(result)
        .as("Predicate on a column absent from the file schema should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void notPredicateOnMissingColumnReturnsNoop() {
    // not(Ignored) must stay Ignored rather than flipping to a pushable filter
    FilterCompat.Filter result =
        ParquetFilters.convert(SCHEMA, Expressions.not(Expressions.equal("missing", 42)), true);
    assertThat(result)
        .as("not() of a missing-column predicate should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void orWithMissingColumnReturnsNoop() {
    // or(present, Ignored) cannot be pushed down: the missing column could match rows
    FilterCompat.Filter result =
        ParquetFilters.convert(
            SCHEMA,
            Expressions.or(Expressions.equal("id", 42), Expressions.equal("missing", 7)),
            true);
    assertThat(result)
        .as("or(pred, missing-column pred) should produce NOOP filter")
        .isSameAs(FilterCompat.NOOP);
  }

  @Test
  void andWithMissingColumnPushesPresentSide() {
    // and(present, Ignored) can push the resolvable side; dropping the ignored conjunct is safe
    FilterCompat.Filter result =
        ParquetFilters.convert(
            SCHEMA,
            Expressions.and(Expressions.equal("id", 42), Expressions.equal("missing", 7)),
            true);
    assertThat(result)
        .as("and(pred, missing-column pred) should push the resolvable predicate")
        .isNotSameAs(FilterCompat.NOOP);
  }
}
