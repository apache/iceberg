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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.vortex.api.Expression;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for filter conversion. The Vortex {@link Expression} API in 0.71+ wraps an opaque
 * native handle so we cannot compare expressions structurally; tests instead check that supported
 * shapes return a non-sentinel expression and unsupported shapes fall back to ALWAYS_TRUE.
 */
public class TestConvertFilterToVortex {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "salary", Types.LongType.get()),
          optional(4, "binary", Types.BinaryType.get()),
          optional(5, "fixed", Types.FixedType.ofLength(4)),
          optional(6, "decimal", Types.DecimalType.of(20, 4)),
          optional(7, "date", Types.DateType.get()),
          optional(8, "timestamp", Types.TimestampType.withoutZone()),
          optional(9, "timestamptz", Types.TimestampType.withZone()),
          optional(10, "timestamp_nano", Types.TimestampNanoType.withoutZone()),
          optional(11, "timestamptz_nano", Types.TimestampNanoType.withZone()),
          optional(12, "uuid", Types.UUIDType.get()),
          optional(13, "time", Types.TimeType.get()),
          optional(
              14, "person", Types.StructType.of(optional(15, "age", Types.IntegerType.get()))));

  @Test
  public void testIn() {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.in("id", 1L, 2L, 3L));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testNotIn() {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.notIn("id", 1L, 2L, 3L));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testInSingleValue() {
    // A single-value IN is optimized by Iceberg to EQ during binding
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.in("id", 42L));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testInStrings() {
    Expression result =
        ConvertFilterToVortex.convert(SCHEMA, Expressions.in("name", "Alice", "Bob"));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testCaseInsensitiveBinding() {
    Expression result =
        ConvertFilterToVortex.convert(SCHEMA, Expressions.lessThan("ID", 10L), false);

    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);

    assertThatThrownBy(
            () -> ConvertFilterToVortex.convert(SCHEMA, Expressions.lessThan("ID", 10L), true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");
  }

  @Test
  public void testNewLiteralTypes() {
    assertConverted(Expressions.equal("binary", ByteBuffer.wrap(new byte[] {1, 2, 3})));
    assertConverted(Expressions.equal("fixed", ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));
    assertConverted(Expressions.equal("decimal", new BigDecimal("1234567890123456.1234")));
    assertConverted(Expressions.equal("date", "2026-07-12"));
    assertConverted(Expressions.equal("timestamp", "2026-07-12T12:34:56.123456"));
    assertConverted(Expressions.equal("timestamptz", "2026-07-12T12:34:56.123456+00:00"));
    assertConverted(Expressions.equal("timestamp_nano", "2026-07-12T12:34:56.123456789"));
    assertConverted(Expressions.equal("timestamptz_nano", "2026-07-12T12:34:56.123456789+00:00"));
    assertConverted(
        Expressions.equal("uuid", UUID.fromString("123e4567-e89b-12d3-a456-426614174000")));
  }

  @Test
  public void testTimeLiteralRemainsUnconverted() {
    Expression result =
        ConvertFilterToVortex.convert(SCHEMA, Expressions.equal("time", "12:34:56.123456"));

    assertThat(result).isSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
  }

  @Test
  public void testNestedField() {
    assertConverted(Expressions.equal("person.age", 34));
  }

  @Test
  public void testNullLiterals() {
    assertNullConverted(Types.BooleanType.get());
    assertNullConverted(Types.IntegerType.get());
    assertNullConverted(Types.LongType.get());
    assertNullConverted(Types.FloatType.get());
    assertNullConverted(Types.DoubleType.get());
    assertNullConverted(Types.StringType.get());
    assertNullConverted(Types.BinaryType.get());
    assertNullConverted(Types.FixedType.ofLength(4));
    assertNullConverted(Types.DecimalType.of(20, 4));
    assertNullConverted(Types.DateType.get());
    assertNullConverted(Types.TimestampType.withoutZone());
    assertNullConverted(Types.TimestampType.withZone());
    assertNullConverted(Types.TimestampNanoType.withoutZone());
    assertNullConverted(Types.TimestampNanoType.withZone());
    assertNullConverted(Types.UUIDType.get());
    assertThat(ConvertFilterToVortex.toVortexLiteral(null, Types.TimeType.get()))
        .isSameAs(ConvertFilterToVortex.UNCONVERTIBLE);
  }

  private static void assertConverted(org.apache.iceberg.expressions.Expression expression) {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, expression);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  private static void assertNullConverted(Type type) {
    assertThat(ConvertFilterToVortex.toVortexLiteral(null, type))
        .isNotSameAs(ConvertFilterToVortex.UNCONVERTIBLE);
  }
}
