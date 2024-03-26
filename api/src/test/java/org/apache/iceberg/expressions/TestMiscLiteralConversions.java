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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMiscLiteralConversions {
  @Test
  public void testIdentityConversions() {
    List<Pair<Literal<?>, Type>> pairs =
        Arrays.asList(
            Pair.of(Literal.of(true), Types.BooleanType.get()),
            Pair.of(Literal.of(34), Types.IntegerType.get()),
            Pair.of(Literal.of(34L), Types.LongType.get()),
            Pair.of(Literal.of(34.11F), Types.FloatType.get()),
            Pair.of(Literal.of(34.55D), Types.DoubleType.get()),
            Pair.of(Literal.of("34.55"), Types.DecimalType.of(9, 2)),
            Pair.of(Literal.of("2017-08-18"), Types.DateType.get()),
            Pair.of(Literal.of("14:21:01.919"), Types.TimeType.get()),
            Pair.of(Literal.of("2017-08-18T14:21:01.919432"), Types.TimestampType.withoutZone()),
            Pair.of(
                Literal.of("2017-08-18T14:21:01.919432755"), Types.TimestampNanoType.withoutZone()),
            Pair.of(Literal.of("abc"), Types.StringType.get()),
            Pair.of(Literal.of(UUID.randomUUID()), Types.UUIDType.get()),
            Pair.of(Literal.of(new byte[] {0, 1, 2}), Types.FixedType.ofLength(3)),
            Pair.of(Literal.of(ByteBuffer.wrap(new byte[] {0, 1, 2})), Types.BinaryType.get()));

    for (Pair<Literal<?>, Type> pair : pairs) {
      Literal<?> lit = pair.first();
      Type type = pair.second();

      // first, convert the literal to the target type (date/times start as strings)
      Literal<?> expected = lit.to(type);

      // then check that converting again to the same type results in an identical literal
      assertThat(expected.to(type))
          .as("Converting twice should produce identical values")
          .isSameAs(expected);
    }
  }

  @Test
  public void testTimestampWithMicrosecondsToDate() {
    final Literal<Long> micros =
        Literal.of("2017-08-18T14:21:01.919432755").to(Types.TimestampType.withoutZone());
    final Literal<Integer> dateOfNanos = micros.to(Types.DateType.get());
    assertThat(dateOfNanos).isEqualTo(Literal.of("2017-08-18").to(Types.DateType.get()));
  }

  @Test
  public void testTimestampWithNanoosecondsToDate() {
    final Literal<Long> nanos =
        Literal.of("2017-08-18T14:21:01.919432755").to(Types.TimestampNanoType.withoutZone());
    final Literal<Integer> dateOfNanos = nanos.to(Types.DateType.get());
    assertThat(dateOfNanos).isEqualTo(Literal.of("2017-08-18").to(Types.DateType.get()));
  }

  @Test
  public void testBinaryToFixed() {
    Literal<ByteBuffer> lit = Literal.of(ByteBuffer.wrap(new byte[] {0, 1, 2}));
    Literal<ByteBuffer> fixedLit = lit.to(Types.FixedType.ofLength(3));
    assertThat(fixedLit).as("Should allow conversion to correct fixed length").isNotNull();
    assertThat(fixedLit.value().duplicate())
        .as("Conversion should not change value")
        .isEqualTo(lit.value().duplicate());

    assertThat(lit.to(Types.FixedType.ofLength(4)))
        .as("Should not allow conversion to different fixed length")
        .isNull();
    assertThat(lit.to(Types.FixedType.ofLength(2)))
        .as("Should not allow conversion to different fixed length")
        .isNull();
  }

  @Test
  public void testFixedToBinary() {
    Literal<ByteBuffer> lit = Literal.of(new byte[] {0, 1, 2});
    Literal<ByteBuffer> binaryLit = lit.to(Types.BinaryType.get());
    assertThat(binaryLit).as("Should allow conversion to binary").isNotNull();
    assertThat(binaryLit.value().duplicate())
        .as("Conversion should not change value")
        .isEqualTo(lit.value().duplicate());
  }

  @Test
  public void testInvalidBooleanConversions() {
    testInvalidConversions(
        Literal.of(true),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidIntegerConversions() {
    testInvalidConversions(
        Literal.of(34),
        Types.BooleanType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidLongConversions() {
    testInvalidConversions(
        Literal.of(34L),
        Types.BooleanType.get(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidFloatConversions() {
    testInvalidConversions(
        Literal.of(34.11F),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidDoubleConversions() {
    testInvalidConversions(
        Literal.of(34.11D),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidDateConversions() {
    testInvalidConversions(
        Literal.of("2017-08-18").to(Types.DateType.get()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidTimeConversions() {
    testInvalidConversions(
        Literal.of("14:21:01.919").to(Types.TimeType.get()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidTimestampMicrosConversions() {
    testInvalidConversions(
        Literal.of("2017-08-18T14:21:01.919123").to(Types.TimestampType.withoutZone()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.TimeType.get(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidTimestampNanosConversions() {
    testInvalidConversions(
        Literal.of("2017-08-18T14:21:01.919123456").to(Types.TimestampNanoType.withoutZone()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.TimeType.get(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidDecimalConversions() {
    testInvalidConversions(
        Literal.of(new BigDecimal("34.11")),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidStringConversions() {
    // Strings can be used for types that are difficult to construct, like decimal or timestamp,
    // but are not intended to support parsing strings to any type
    testInvalidConversions(
        Literal.of("abc"),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidUUIDConversions() {
    testInvalidConversions(
        Literal.of(UUID.randomUUID()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get());
  }

  @Test
  public void testInvalidFixedConversions() {
    testInvalidConversions(
        Literal.of(new byte[] {0, 1, 2}),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1));
  }

  @Test
  public void testInvalidBinaryConversions() {
    testInvalidConversions(
        Literal.of(ByteBuffer.wrap(new byte[] {0, 1, 2})),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withoutZone(),
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.TimestampNanoType.withZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1));
  }

  private void testInvalidConversions(Literal<?> lit, Type... invalidTypes) {
    for (Type type : invalidTypes) {
      assertThat(lit.to(type))
          .as(lit.value().getClass().getName() + " literal to " + type + " is not allowed")
          .isNull();
    }
  }

  private static class Pair<X, Y> {
    public static <X, Y> Pair<X, Y> of(X first, Y second) {
      return new Pair<>(first, second);
    }

    private final X first;
    private final Y second;

    private Pair(X first, Y second) {
      this.first = first;
      this.second = second;
    }

    public X first() {
      return first;
    }

    public Y second() {
      return second;
    }
  }
}
