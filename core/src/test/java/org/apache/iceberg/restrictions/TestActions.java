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
package org.apache.iceberg.restrictions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

public class TestActions {

  @Test
  public void maskAlphanumSpecExample() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskAlphanum(1), Types.StringType.get());
    assertThat(fn.apply("prashant010696@gmail.com")).isEqualTo("xxxxxxxxnnnnnn@xxxxx.xxx");
  }

  @Test
  public void maskAlphanumPreservedPunctuation() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskAlphanum(1), Types.StringType.get());
    assertThat(fn.apply("(555) 123-4567")).isEqualTo("(nnn)xnnn-nnnn");
    assertThat(fn.apply("a.b,c")).isEqualTo("x.x,x");
  }

  @Test
  public void maskAlphanumNullInNullOut() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskAlphanum(1), Types.StringType.get());
    assertThat(fn.apply(null)).isNull();
  }

  @Test
  public void maskAlphanumEmptyString() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskAlphanum(1), Types.StringType.get());
    assertThat(fn.apply("")).isEqualTo("");
  }

  @Test
  public void showFirst4SpecExample() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.ShowFirst4(1), Types.StringType.get());
    assertThat(fn.apply("prashant010696@gmail.com")).isEqualTo("prasxxxxnnnnnn@xxxxx.xxx");
  }

  @Test
  public void showFirst4FourOrFewerReturnedUnchanged() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.ShowFirst4(1), Types.StringType.get());
    assertThat(fn.apply("abcd")).isEqualTo("abcd");
    assertThat(fn.apply("ab")).isEqualTo("ab");
    assertThat(fn.apply("")).isEqualTo("");
  }

  @Test
  public void showLast4SpecExample() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.ShowLast4(1), Types.StringType.get());
    assertThat(fn.apply("4111-1111-1111-4444")).isEqualTo("nnnn-nnnn-nnnn-4444");
  }

  @Test
  public void showLast4FourOrFewerReturnedUnchanged() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.ShowLast4(1), Types.StringType.get());
    assertThat(fn.apply("abcd")).isEqualTo("abcd");
    assertThat(fn.apply("ab")).isEqualTo("ab");
  }

  @Test
  public void replaceWithNullAlwaysReturnsNull() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.ReplaceWithNull(1), Types.IntegerType.get());
    assertThat(fn.apply(42)).isNull();
    assertThat(fn.apply(null)).isNull();

    SerializableFunction<Object, Object> strFn =
        Actions.bind(new Action.ReplaceWithNull(1), Types.StringType.get());
    assertThat(strFn.apply("hello")).isNull();
  }

  @Test
  public void maskToDefaultString() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.StringType.get());
    assertThat(fn.apply("anything")).isEqualTo("XXXXXXXX");
  }

  @Test
  public void maskToDefaultInt() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.IntegerType.get());
    assertThat(fn.apply(42)).isEqualTo(999999999);
  }

  @Test
  public void maskToDefaultLong() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.LongType.get());
    assertThat(fn.apply(42L)).isEqualTo(999999999L);
  }

  @Test
  public void maskToDefaultDouble() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.DoubleType.get());
    assertThat(fn.apply(3.14)).isEqualTo(0.0d);
  }

  @Test
  public void maskToDefaultBoolean() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.BooleanType.get());
    assertThat(fn.apply(true)).isEqualTo(false);
  }

  @Test
  public void maskToDefaultDate() {
    int input = (int) LocalDate.of(2024, 7, 15).toEpochDay();
    int expected = (int) LocalDate.of(9999, 12, 31).toEpochDay();
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.DateType.get());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void maskToDefaultTimestamp() {
    long input =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long expected = LocalDateTime.of(9999, 12, 31, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.TimestampType.withZone());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void maskToDefaultBinary() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.BinaryType.get());
    ByteBuffer result = (ByteBuffer) fn.apply(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(result.remaining()).isEqualTo(0);
  }

  @Test
  public void maskToDefaultDecimal() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.DecimalType.of(10, 2));
    BigDecimal result = (BigDecimal) fn.apply(new BigDecimal("12.34"));
    assertThat(result.compareTo(BigDecimal.ZERO)).isEqualTo(0);
    assertThat(result.scale()).isEqualTo(2);
  }

  @Test
  public void maskToDefaultNullReturnsNull() {
    // Spec: "For all actions, if the input column value is NULL, the output MUST be NULL."
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.MaskToDefault(1), Types.IntegerType.get());
    assertThat(fn.apply(null)).isNull();
  }

  @Test
  public void truncateToYearDate() {
    int input = (int) LocalDate.of(2024, 7, 15).toEpochDay();
    int expected = (int) LocalDate.of(2024, 1, 1).toEpochDay();
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.TruncateToYear(1), Types.DateType.get());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void truncateToMonthDate() {
    int input = (int) LocalDate.of(2024, 7, 15).toEpochDay();
    int expected = (int) LocalDate.of(2024, 7, 1).toEpochDay();
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.TruncateToMonth(1), Types.DateType.get());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void truncateToYearTimestamp() {
    long inputMicros =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long expectedMicros =
        LocalDateTime.of(2024, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.TruncateToYear(1), Types.TimestampType.withZone());
    assertThat(fn.apply(inputMicros)).isEqualTo(expectedMicros);
  }

  @Test
  public void truncateToMonthTimestamp() {
    long inputMicros =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long expectedMicros =
        LocalDateTime.of(2024, 7, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.TruncateToMonth(1), Types.TimestampType.withZone());
    assertThat(fn.apply(inputMicros)).isEqualTo(expectedMicros);
  }

  @Test
  public void sha256GlobalStringIsDeterministic() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.Sha256Global(1), Types.StringType.get());
    String first = (String) fn.apply("hello");
    String second = (String) fn.apply("hello");
    assertThat(first).isEqualTo(second);
    assertThat(first).isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
  }

  @Test
  public void sha256GlobalBinaryReturns32Bytes() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.Sha256Global(1), Types.BinaryType.get());
    ByteBuffer result = (ByteBuffer) fn.apply(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(result.remaining()).isEqualTo(32);
  }

  @Test
  public void sha256GlobalIntegerDeterministic() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.Sha256Global(1), Types.IntegerType.get());
    Object first = fn.apply(42);
    Object second = fn.apply(42);
    assertThat(first).isEqualTo(second);
    assertThat(first).isInstanceOf(Integer.class);
  }

  @Test
  public void sha256GlobalLongDeterministic() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.Sha256Global(1), Types.LongType.get());
    Object first = fn.apply(42L);
    Object second = fn.apply(42L);
    assertThat(first).isEqualTo(second);
    assertThat(first).isInstanceOf(Long.class);
  }

  @Test
  public void sha256QueryLocalDiffersWithDifferentSalt() {
    byte[] saltA = new byte[16];
    byte[] saltB = new byte[16];
    Arrays.fill(saltA, (byte) 1);
    Arrays.fill(saltB, (byte) 2);
    SerializableFunction<Object, Object> fnA =
        Actions.bind(new Action.Sha256QueryLocal(1), Types.StringType.get(), saltA);
    SerializableFunction<Object, Object> fnB =
        Actions.bind(new Action.Sha256QueryLocal(1), Types.StringType.get(), saltB);
    assertThat(fnA.apply("hello")).isNotEqualTo(fnB.apply("hello"));
  }

  @Test
  public void sha256QueryLocalSaltMustBeAtLeast16Bytes() {
    assertThatThrownBy(
            () ->
                Actions.bind(new Action.Sha256QueryLocal(1), Types.StringType.get(), new byte[15]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("16 bytes");
  }

  @Test
  public void applyExpressionFailsOnApply() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(
            new Action.ApplyExpression(1, Expressions.alwaysTrue()), Types.StringType.get());
    assertThatThrownBy(() -> fn.apply("any"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("apply-expression");
  }

  @Test
  public void bindRejectsMaskAlphanumOnNonString() {
    assertThatThrownBy(() -> Actions.bind(new Action.MaskAlphanum(1), Types.IntegerType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("STRING");
  }

  @Test
  public void bindRejectsTruncateOnUnsupportedType() {
    assertThatThrownBy(() -> Actions.bind(new Action.TruncateToYear(1), Types.StringType.get()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not supported for type");
  }

  @Test
  public void sha256NullInNullOut() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.Sha256Global(1), Types.StringType.get());
    assertThat(fn.apply(null)).isNull();
  }

  @Test
  public void truncateNullInNullOut() {
    SerializableFunction<Object, Object> fn =
        Actions.bind(new Action.TruncateToYear(1), Types.DateType.get());
    assertThat(fn.apply(null)).isNull();
  }
}
