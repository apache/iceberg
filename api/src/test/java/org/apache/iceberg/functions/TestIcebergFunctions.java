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
package org.apache.iceberg.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

public class TestIcebergFunctions {

  @Test
  public void maskAlphanumSpecExample() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.maskAlphanum().bind(Types.StringType.get());
    assertThat(fn.apply("prashant010696@gmail.com")).isEqualTo("xxxxxxxxnnnnnn@xxxxx.xxx");
  }

  @Test
  public void maskAlphanumPreservedPunctuation() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.maskAlphanum().bind(Types.StringType.get());
    assertThat(fn.apply("(555) 123-4567")).isEqualTo("(nnn)xnnn-nnnn");
    assertThat(fn.apply("a.b,c")).isEqualTo("x.x,x");
  }

  @Test
  public void maskAlphanumNullInNullOut() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.maskAlphanum().bind(Types.StringType.get());
    assertThat(fn.apply(null)).isNull();
  }

  @Test
  public void maskAlphanumEmptyString() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.maskAlphanum().bind(Types.StringType.get());
    assertThat(fn.apply("")).isEqualTo("");
  }

  @Test
  public void stringFunctionsAcceptAnyCharSequence() {
    CharSequence email = new StringBuilder("iceberg16@apache.org");
    assertThat(IcebergFunctions.maskAlphanum().bind(Types.StringType.get()).apply(email))
        .hasToString("xxxxxxxnn@xxxxxx.xxx");
    assertThat(IcebergFunctions.showFirst4().bind(Types.StringType.get()).apply(email))
        .hasToString("icebxxxnn@xxxxxx.xxx");
    assertThat(IcebergFunctions.showLast4().bind(Types.StringType.get()).apply(email))
        .hasToString("xxxxxxxnn@xxxxxx.org");
    assertThat(IcebergFunctions.sha256Global().bind(Types.StringType.get()).apply(email))
        .isEqualTo(
            IcebergFunctions.sha256Global()
                .bind(Types.StringType.get())
                .apply("iceberg16@apache.org"));
  }

  @Test
  public void showFirst4SpecExample() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.showFirst4().bind(Types.StringType.get());
    assertThat(fn.apply("prashant010696@gmail.com")).isEqualTo("prasxxxxnnnnnn@xxxxx.xxx");
  }

  @Test
  public void showFirst4FourOrFewerReturnedUnchanged() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.showFirst4().bind(Types.StringType.get());
    assertThat(fn.apply("abcd")).isEqualTo("abcd");
    assertThat(fn.apply("ab")).isEqualTo("ab");
    assertThat(fn.apply("")).isEqualTo("");
  }

  @Test
  public void showLast4SpecExample() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.showLast4().bind(Types.StringType.get());
    assertThat(fn.apply("4111-1111-1111-4444")).isEqualTo("nnnn-nnnn-nnnn-4444");
  }

  @Test
  public void showLast4FourOrFewerReturnedUnchanged() {
    SerializableFunction<CharSequence, CharSequence> fn =
        IcebergFunctions.showLast4().bind(Types.StringType.get());
    assertThat(fn.apply("abcd")).isEqualTo("abcd");
    assertThat(fn.apply("ab")).isEqualTo("ab");
  }

  @Test
  public void replaceWithNullAlwaysReturnsNull() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.replaceWithNull().bind(Types.IntegerType.get());
    assertThat(fn.apply(42)).isNull();
    assertThat(fn.apply(null)).isNull();

    SerializableFunction<Object, Object> strFn =
        IcebergFunctions.replaceWithNull().bind(Types.StringType.get());
    assertThat(strFn.apply("hello")).isNull();
  }

  @Test
  public void maskToFixedValueString() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.StringType.get());
    assertThat(fn.apply("anything")).isEqualTo("XXXXXXXX");
  }

  @Test
  public void maskToFixedValueInt() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.IntegerType.get());
    assertThat(fn.apply(42)).isEqualTo(0);
  }

  @Test
  public void maskToFixedValueLong() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.LongType.get());
    assertThat(fn.apply(42L)).isEqualTo(0L);
  }

  @Test
  public void maskToFixedValueDouble() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.DoubleType.get());
    assertThat(fn.apply(3.14)).isEqualTo(0.0d);
  }

  @Test
  public void maskToFixedValueBoolean() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.BooleanType.get());
    assertThat(fn.apply(true)).isEqualTo(false);
  }

  @Test
  public void maskToFixedValueDate() {
    int input = DateTimeUtil.daysFromDate(LocalDate.of(2024, 7, 15));
    int expected = DateTimeUtil.daysFromDate(LocalDate.of(1970, 1, 1));
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.DateType.get());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void maskToFixedValueTimestamp() {
    long input =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long expected = LocalDateTime.of(1970, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.TimestampType.withZone());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void maskToFixedValueBinary() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.BinaryType.get());
    ByteBuffer result = (ByteBuffer) fn.apply(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(result.remaining()).isEqualTo(0);
  }

  @Test
  public void maskToFixedValueStructWithBinaryField() {
    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.BinaryType.get()));
    SerializableFunction<Object, Object> fn = IcebergFunctions.maskToFixedValue().bind(struct);
    StructLike result = (StructLike) fn.apply(null);
    assertThat(result.get(0, Integer.class)).isEqualTo(0);
    assertThat(result.get(1, ByteBuffer.class).remaining()).isEqualTo(0);
  }

  @Test
  public void maskToFixedValueDecimal() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.DecimalType.of(10, 2));
    BigDecimal result = (BigDecimal) fn.apply(new BigDecimal("12.34"));
    assertThat(result.compareTo(BigDecimal.ZERO)).isEqualTo(0);
    assertThat(result.scale()).isEqualTo(2);
  }

  @Test
  public void maskToFixedValueCanBindStructOfSupportedTypes() {
    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(3, "city", Types.StringType.get()))));
    assertThat(IcebergFunctions.maskToFixedValue().canBind(struct)).isTrue();
  }

  @Test
  public void maskToFixedValueCannotBindStructWithUnsupportedNestedField() {
    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(3, "location", Types.GeometryType.crs84()))));
    assertThat(IcebergFunctions.maskToFixedValue().canBind(struct)).isFalse();
    assertThatThrownBy(() -> IcebergFunctions.maskToFixedValue().bind(struct))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("mask_to_fixed_value is not supported for type: " + struct);
  }

  @Test
  public void maskToFixedValueNullReturnsFixedValue() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.maskToFixedValue().bind(Types.IntegerType.get());
    assertThat(fn.apply(null)).isEqualTo(0);
  }

  @Test
  public void truncateToYearDate() {
    int input = (int) LocalDate.of(2024, 7, 15).toEpochDay();
    int expected = (int) LocalDate.of(2024, 1, 1).toEpochDay();
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToYear().bind(Types.DateType.get());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void truncateToMonthDate() {
    int input = (int) LocalDate.of(2024, 7, 15).toEpochDay();
    int expected = (int) LocalDate.of(2024, 7, 1).toEpochDay();
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToMonth().bind(Types.DateType.get());
    assertThat(fn.apply(input)).isEqualTo(expected);
  }

  @Test
  public void truncateToYearTimestamp() {
    long inputMicros =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long expectedMicros =
        LocalDateTime.of(2024, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToYear().bind(Types.TimestampType.withZone());
    assertThat(fn.apply(inputMicros)).isEqualTo(expectedMicros);
  }

  @Test
  public void truncateToMonthTimestamp() {
    long inputMicros =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    long expectedMicros =
        LocalDateTime.of(2024, 7, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToMonth().bind(Types.TimestampType.withZone());
    assertThat(fn.apply(inputMicros)).isEqualTo(expectedMicros);
  }

  @Test
  public void truncateToYearTimestampNanoTz() {
    long inputNanos =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
    long expectedNanos =
        LocalDateTime.of(2024, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToYear().bind(Types.TimestampNanoType.withZone());
    assertThat(fn.apply(inputNanos)).isEqualTo(expectedNanos);
  }

  @Test
  public void truncateToMonthTimestampNanoTz() {
    long inputNanos =
        LocalDateTime.of(2024, 7, 15, 13, 45, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
    long expectedNanos =
        LocalDateTime.of(2024, 7, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToMonth().bind(Types.TimestampNanoType.withZone());
    assertThat(fn.apply(inputNanos)).isEqualTo(expectedNanos);
  }

  @Test
  public void sha256GlobalStringIsDeterministic() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.sha256Global().bind(Types.StringType.get());
    String first = (String) fn.apply("hello");
    String second = (String) fn.apply("hello");
    assertThat(first).isEqualTo(second);
    assertThat(first).isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
  }

  @Test
  public void sha256GlobalBinaryReturns32Bytes() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.sha256Global().bind(Types.BinaryType.get());
    ByteBuffer result = (ByteBuffer) fn.apply(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(result.remaining()).isEqualTo(32);
  }

  @Test
  public void sha256GlobalIntegerDeterministic() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.sha256Global().bind(Types.IntegerType.get());
    Object first = fn.apply(42);
    Object second = fn.apply(42);
    assertThat(first).isEqualTo(second);
    assertThat(first).isInstanceOf(Integer.class);
  }

  @Test
  public void sha256GlobalLongDeterministic() {
    IcebergFunction<Long, Long> sha256 = IcebergFunctions.sha256Global();
    SerializableFunction<Long, Long> fn = sha256.bind(Types.LongType.get());
    Long first = fn.apply(42L);
    Long second = fn.apply(42L);
    assertThat(first).isEqualTo(second);
  }

  @Test
  public void sha256QueryLocalDiffersWithDifferentSalt() {
    byte[] saltA = new byte[16];
    byte[] saltB = new byte[16];
    Arrays.fill(saltA, (byte) 1);
    Arrays.fill(saltB, (byte) 2);
    SerializableFunction<Object, Object> fnA =
        IcebergFunctions.sha256QueryLocal().bind(Types.StringType.get(), saltA);
    SerializableFunction<Object, Object> fnB =
        IcebergFunctions.sha256QueryLocal().bind(Types.StringType.get(), saltB);
    assertThat(fnA.apply("hello")).isNotEqualTo(fnB.apply("hello"));
  }

  @Test
  public void sha256QueryLocalSaltMustBeAtLeast16Bytes() {
    assertThatThrownBy(
            () -> IcebergFunctions.sha256QueryLocal().bind(Types.StringType.get(), new byte[15]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("16 bytes");
  }

  @Test
  public void bindRejectsMaskAlphanumOnNonString() {
    assertThatThrownBy(() -> IcebergFunctions.maskAlphanum().bind(Types.IntegerType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("STRING");
  }

  @Test
  public void bindRejectsTruncateOnUnsupportedType() {
    assertThatThrownBy(() -> IcebergFunctions.truncateToYear().bind(Types.StringType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not supported for type");
  }

  @Test
  public void bindFailsClosedOnUnknownFunction() {
    assertThatThrownBy(() -> new UnknownFunction("future-mask-v2").bind(Types.StringType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("future-mask-v2");
  }

  @Test
  public void sha256NullInNullOut() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.sha256Global().bind(Types.StringType.get());
    assertThat(fn.apply(null)).isNull();
  }

  @Test
  public void truncateNullInNullOut() {
    SerializableFunction<Object, Object> fn =
        IcebergFunctions.truncateToYear().bind(Types.DateType.get());
    assertThat(fn.apply(null)).isNull();
  }

  @Test
  public void toStringReturnsFunctionName() {
    assertThat(IcebergFunctions.maskAlphanum().toString()).isEqualTo("mask_alphanum");
    assertThat(IcebergFunctions.maskToFixedValue().toString()).isEqualTo("mask_to_fixed_value");
    assertThat(IcebergFunctions.replaceWithNull().toString()).isEqualTo("replace_with_null");
    assertThat(IcebergFunctions.showFirst4().toString()).isEqualTo("show_first_4");
    assertThat(IcebergFunctions.showLast4().toString()).isEqualTo("show_last_4");
    assertThat(IcebergFunctions.truncateToYear().toString()).isEqualTo("truncate_to_year");
    assertThat(IcebergFunctions.truncateToMonth().toString()).isEqualTo("truncate_to_month");
    assertThat(IcebergFunctions.sha256Global().toString()).isEqualTo("sha_256_global");
    assertThat(IcebergFunctions.sha256QueryLocal().toString()).isEqualTo("sha_256_query_local");
  }

  @Test
  public void fromStringRoundTripsEveryFunction() {
    IcebergFunction<?, ?>[] functions =
        new IcebergFunction<?, ?>[] {
          IcebergFunctions.maskAlphanum(),
          IcebergFunctions.maskToFixedValue(),
          IcebergFunctions.replaceWithNull(),
          IcebergFunctions.showFirst4(),
          IcebergFunctions.showLast4(),
          IcebergFunctions.truncateToYear(),
          IcebergFunctions.truncateToMonth(),
          IcebergFunctions.sha256Global(),
          IcebergFunctions.sha256QueryLocal()
        };

    for (IcebergFunction<?, ?> expected : functions) {
      assertThat(IcebergFunctions.fromString(expected.toString())).isSameAs(expected);
    }
  }

  @Test
  public void fromStringPreservesUnknownFunction() {
    IcebergFunction<?, ?> function = IcebergFunctions.fromString("future-mask-v2");
    assertThat(function).isInstanceOf(UnknownFunction.class);
    assertThat(function.toString()).isEqualTo("future-mask-v2");
    assertThat(function.canBind(Types.StringType.get())).isFalse();
  }

  @Test
  public void fromStringReturnsSaltedFunctionForQueryLocalSha256() {
    assertThat(IcebergFunctions.fromString("sha_256_query_local"))
        .isInstanceOf(SaltedFunction.class);
    assertThat(IcebergFunctions.fromString("sha_256_global")).isNotInstanceOf(SaltedFunction.class);
  }

  @Test
  public void factoryMethodsReturnSingletons() {
    assertThat(IcebergFunctions.maskAlphanum()).isSameAs(IcebergFunctions.maskAlphanum());
  }

  @Test
  public void notEqualIfFunctionDiffers() {
    assertThat(IcebergFunctions.maskAlphanum()).isNotEqualTo(IcebergFunctions.showLast4());
  }

  @Test
  public void equalsIsSymmetricAcrossFunctionTypes() {
    // An unknown function reporting a known name must not compare equal to that known function in
    // either direction: equality is by class, not by the reported name.
    IcebergFunction<?, ?> known = IcebergFunctions.maskAlphanum();
    IcebergFunction<?, ?> spoofed = new UnknownFunction(known.toString());
    assertThat(spoofed.toString()).isEqualTo(known.toString());
    assertThat(spoofed).isNotEqualTo(known);
    assertThat(known).isNotEqualTo(spoofed);
  }

  @Test
  public void unknownFunctionsWithTheSameNameAreEqual() {
    assertThat(IcebergFunctions.fromString("future-a"))
        .isEqualTo(IcebergFunctions.fromString("future-a"))
        .hasSameHashCodeAs(IcebergFunctions.fromString("future-a"));
  }

  @Test
  public void unknownFunctionsDifferByName() {
    assertThat(IcebergFunctions.fromString("future-a"))
        .isNotEqualTo(IcebergFunctions.fromString("future-b"));
  }
}
