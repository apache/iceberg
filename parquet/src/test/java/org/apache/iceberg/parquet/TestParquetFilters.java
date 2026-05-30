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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

class TestParquetFilters {
  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "decimal_int", Types.DecimalType.of(9, 2)),
          optional(2, "decimal_long", Types.DecimalType.of(18, 2)),
          optional(3, "decimal_fixed", Types.DecimalType.of(19, 2)),
          optional(4, "uuid_col", Types.UUIDType.get()));

  private static final MessageType PARQUET_SCHEMA =
      ParquetSchemaUtil.convert(TABLE_SCHEMA, "table");

  private static final MessageType BINARY_DECIMAL_PARQUET_SCHEMA =
      org.apache.parquet.schema.Types.buildMessage()
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.decimalType(2, 9))
          .named("decimal_int")
          .named("table");

  private static final MessageType EXTENDED_FIXED_DECIMAL_PARQUET_SCHEMA =
      org.apache.parquet.schema.Types.buildMessage()
          .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
          .length(16)
          .as(LogicalTypeAnnotation.decimalType(2, 19))
          .named("decimal_fixed")
          .named("table");

  @Test
  void convertsIntDecimalLiteral() {
    BigDecimal decimal = new BigDecimal("12.34");

    Operators.Eq<?> predicate = predicate(equal("decimal_int", decimal), Operators.Eq.class);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Integer.class);
    assertThat(predicate.getValue()).isEqualTo(1234);
  }

  @Test
  void convertsLongDecimalLiteral() {
    BigDecimal decimal = new BigDecimal("1234567890123456.78");

    Operators.Eq<?> predicate = predicate(equal("decimal_long", decimal), Operators.Eq.class);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Long.class);
    assertThat(predicate.getValue()).isEqualTo(123456789012345678L);
  }

  @Test
  void convertsFixedDecimalLiteral() {
    Types.DecimalType decimalType = Types.DecimalType.of(19, 2);
    BigDecimal decimal = new BigDecimal("12345678901234567.89");

    Operators.Eq<?> predicate = predicate(equal("decimal_fixed", decimal), Operators.Eq.class);

    byte[] expected =
        DecimalUtil.toReusedFixLengthBytes(
            decimalType.precision(),
            decimalType.scale(),
            decimal,
            new byte[TypeUtil.decimalRequiredBytes(decimalType.precision())]);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Binary.class);
    assertThat(((Binary) predicate.getValue()).getBytes()).isEqualTo(expected);
  }

  @Test
  void usesParquetPhysicalTypeForBinaryDecimal() {
    BigDecimal decimal = new BigDecimal("12.34");

    Operators.Eq<?> predicate =
        predicate(BINARY_DECIMAL_PARQUET_SCHEMA, equal("decimal_int", decimal), Operators.Eq.class);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Binary.class);
    assertThat(((Binary) predicate.getValue()).getBytes())
        .isEqualTo(decimal.unscaledValue().toByteArray());
  }

  @Test
  void usesParquetTypeLengthForFixedDecimal() {
    Types.DecimalType decimalType = Types.DecimalType.of(19, 2);
    BigDecimal decimal = new BigDecimal("-12345678901234567.89");

    Operators.Eq<?> predicate =
        predicate(
            EXTENDED_FIXED_DECIMAL_PARQUET_SCHEMA,
            equal("decimal_fixed", decimal),
            Operators.Eq.class);

    byte[] expected =
        DecimalUtil.toReusedFixLengthBytes(
            decimalType.precision(), decimalType.scale(), decimal, new byte[16]);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Binary.class);
    assertThat(((Binary) predicate.getValue()).getBytes()).isEqualTo(expected);
  }

  @Test
  void skipsDecimalLiteralWithIncompatibleScale() {
    FilterCompat.Filter filter =
        ParquetFilters.convert(
            PARQUET_SCHEMA, equal("decimal_int", new BigDecimal("12.345")), true);

    assertThat(filter).isSameAs(FilterCompat.NOOP);
  }

  @Test
  void skipsDecimalLiteralThatExceedsPrecision() {
    FilterCompat.Filter filter =
        ParquetFilters.convert(
            PARQUET_SCHEMA, equal("decimal_int", new BigDecimal("123456789.12")), true);

    assertThat(filter).isSameAs(FilterCompat.NOOP);
  }

  @Test
  void convertsDecimalLiteralWithTrailingZeros() {
    Operators.Eq<?> predicate =
        predicate(equal("decimal_int", new BigDecimal("12.340")), Operators.Eq.class);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Integer.class);
    assertThat(predicate.getValue()).isEqualTo(1234);
  }

  @Test
  void convertsUuidLiteral() {
    UUID uuid = UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56");

    Operators.Eq<?> predicate = predicate(equal("uuid_col", uuid), Operators.Eq.class);

    assertThat(predicate.getColumn().getColumnType()).isEqualTo(Binary.class);
    assertThat(((Binary) predicate.getValue()).getBytes()).isEqualTo(UUIDUtil.convert(uuid));
  }

  private static <P extends FilterPredicate> P predicate(Expression expression, Class<P> type) {
    return predicate(PARQUET_SCHEMA, expression, type);
  }

  private static <P extends FilterPredicate> P predicate(
      MessageType parquetSchema, Expression expression, Class<P> type) {
    FilterCompat.Filter filter = ParquetFilters.convert(parquetSchema, expression, true);
    assertThat(filter).isInstanceOf(FilterPredicateCompat.class);

    FilterPredicate predicate = ((FilterPredicateCompat) filter).getFilterPredicate();
    assertThat(predicate).isInstanceOf(type);
    return type.cast(predicate);
  }
}
