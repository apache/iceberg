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

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;

class TestParquetFilters {

  private static Schema schemaWithAliases(Types.NestedField field) {
    Map<String, Integer> aliases = ImmutableMap.of(field.name(), field.fieldId());
    return new Schema(Collections.singletonList(field), aliases);
  }

  @Test
  void decimalInt32FilterPushdown() {
    Schema schema = schemaWithAliases(required(1, "dec", Types.DecimalType.of(5, 2)));
    Expression expr = equal("dec", new BigDecimal("123.45"));
    FilterCompat.Filter filter = ParquetFilters.convert(schema, expr, true);
    assertThat(filter).isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void decimalInt64FilterPushdown() {
    Schema schema = schemaWithAliases(required(1, "dec", Types.DecimalType.of(15, 3)));
    Expression expr = equal("dec", new BigDecimal("123456789012.345"));
    FilterCompat.Filter filter = ParquetFilters.convert(schema, expr, true);
    assertThat(filter).isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void decimalBinaryFilterPushdown() {
    Schema schema = schemaWithAliases(required(1, "dec", Types.DecimalType.of(25, 5)));
    Expression expr = equal("dec", new BigDecimal("12345678901234567890.12345"));
    FilterCompat.Filter filter = ParquetFilters.convert(schema, expr, true);
    assertThat(filter).isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void decimalComparisonPredicates() {
    Schema schema = schemaWithAliases(required(1, "dec", Types.DecimalType.of(10, 2)));
    BigDecimal value = new BigDecimal("100.50");

    assertThat(ParquetFilters.convert(schema, greaterThan("dec", value), true))
        .isNotEqualTo(FilterCompat.NOOP);
    assertThat(ParquetFilters.convert(schema, lessThan("dec", value), true))
        .isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void decimalNullPredicates() {
    Schema schema = schemaWithAliases(optional(1, "dec", Types.DecimalType.of(10, 2)));

    assertThat(ParquetFilters.convert(schema, isNull("dec"), true)).isNotEqualTo(FilterCompat.NOOP);
    assertThat(ParquetFilters.convert(schema, notNull("dec"), true))
        .isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void uuidFilterPushdown() {
    Schema schema = schemaWithAliases(required(1, "id", Types.UUIDType.get()));
    Expression expr = equal("id", UUID.fromString("ab124578-1234-5678-abcd-ef1234567890"));
    FilterCompat.Filter filter = ParquetFilters.convert(schema, expr, true);
    assertThat(filter).isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void uuidComparisonPredicates() {
    Schema schema = schemaWithAliases(required(1, "id", Types.UUIDType.get()));
    UUID value = UUID.fromString("ab124578-1234-5678-abcd-ef1234567890");

    assertThat(ParquetFilters.convert(schema, greaterThan("id", value), true))
        .isNotEqualTo(FilterCompat.NOOP);
    assertThat(ParquetFilters.convert(schema, lessThan("id", value), true))
        .isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void uuidNullPredicates() {
    Schema schema = schemaWithAliases(optional(1, "id", Types.UUIDType.get()));

    assertThat(ParquetFilters.convert(schema, isNull("id"), true)).isNotEqualTo(FilterCompat.NOOP);
    assertThat(ParquetFilters.convert(schema, notNull("id"), true)).isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void stringFilterStillWorks() {
    Schema schema = schemaWithAliases(required(1, "name", Types.StringType.get()));
    Expression expr = equal("name", "test");
    FilterCompat.Filter filter = ParquetFilters.convert(schema, expr, true);
    assertThat(filter).isNotEqualTo(FilterCompat.NOOP);
  }

  @Test
  void integerFilterStillWorks() {
    Schema schema = schemaWithAliases(required(1, "val", Types.IntegerType.get()));
    Expression expr = equal("val", 42);
    FilterCompat.Filter filter = ParquetFilters.convert(schema, expr, true);
    assertThat(filter).isNotEqualTo(FilterCompat.NOOP);
  }
}
