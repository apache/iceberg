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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.TestHelpers.RoundTripSerializer;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestLiteralType {
  private static final String TIMESTAMP = "2024-12-17T23:59:59.123456";
  private static final String TIMESTAMPTZ = "2024-12-17T23:59:59.123456+00:00";

  private static Stream<Arguments> literalTypes() {
    return Stream.of(
        Arguments.of(Literal.of(true), Types.BooleanType.get()),
        Arguments.of(Literal.of(34), Types.IntegerType.get()),
        Arguments.of(Literal.of(34L), Types.LongType.get()),
        Arguments.of(Literal.of(3.4F), Types.FloatType.get()),
        Arguments.of(Literal.of(3.4D), Types.DoubleType.get()),
        Arguments.of(Literal.of("abc"), Types.StringType.get()),
        Arguments.of(Literal.of(UUID.randomUUID()), Types.UUIDType.get()),
        Arguments.of(Literal.of(new byte[] {1, 2, 3}), Types.FixedType.ofLength(3)),
        Arguments.of(Literal.of(ByteBuffer.wrap(new byte[] {1, 2})), Types.BinaryType.get()));
  }

  @ParameterizedTest
  @MethodSource("literalTypes")
  void literalType(Literal<?> literal, Type expected) {
    assertThat(literal.type()).isEqualTo(expected);
  }

  private static Stream<Arguments> conversions() {
    return Stream.of(
        Arguments.of(Literal.of(34), Types.LongType.get()),
        Arguments.of(Literal.of(34), Types.DateType.get()),
        Arguments.of(Literal.of(34L), Types.TimeType.get()),
        Arguments.of(Literal.of(34L), Types.TimestampType.withZone()),
        Arguments.of(Literal.of(34L), Types.TimestampNanoType.withZone()),
        Arguments.of(Literal.of(TIMESTAMP), Types.TimestampType.withoutZone()),
        Arguments.of(Literal.of(TIMESTAMPTZ), Types.TimestampType.withZone()),
        Arguments.of(Literal.of(TIMESTAMP), Types.TimestampNanoType.withoutZone()),
        Arguments.of(Literal.of(TIMESTAMPTZ), Types.TimestampNanoType.withZone()),
        Arguments.of(Expressions.micros(34L), Types.TimestampNanoType.withZone()),
        Arguments.of(Expressions.nanos(34_000L), Types.TimestampType.withZone()),
        Arguments.of(Literal.of(UUID.randomUUID().toString()), Types.UUIDType.get()),
        Arguments.of(Literal.of(ByteBuffer.wrap(new byte[] {1, 2})), Types.FixedType.ofLength(2)));
  }

  @ParameterizedTest
  @MethodSource("conversions")
  void conversionReportsTargetType(Literal<?> literal, Type targetType) {
    assertThat(literal.to(targetType).type()).isEqualTo(targetType);
  }

  @Test
  void untypedTimestampsHaveNoZone() {
    assertThat(Expressions.micros(34L).type()).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(Expressions.millis(34L).type()).isEqualTo(Types.TimestampType.withoutZone());
    assertThat(Expressions.nanos(34L).type()).isEqualTo(Types.TimestampNanoType.withoutZone());
  }

  private static Stream<Arguments> zoneChanges() {
    return Stream.of(
        Arguments.of(Expressions.micros(34L), Types.TimestampType.withZone()),
        Arguments.of(
            Expressions.micros(34L).to(Types.TimestampType.withZone()),
            Types.TimestampType.withoutZone()),
        Arguments.of(Expressions.nanos(34L), Types.TimestampNanoType.withZone()),
        Arguments.of(
            Expressions.nanos(34L).to(Types.TimestampNanoType.withZone()),
            Types.TimestampNanoType.withoutZone()));
  }

  @ParameterizedTest
  @MethodSource("zoneChanges")
  void zoneChangeKeepsValue(Literal<Long> literal, Type targetType) {
    Literal<Long> converted = literal.to(targetType);

    assertThat(converted.type()).isEqualTo(targetType);
    assertThat(converted.value()).isEqualTo(literal.value());
  }

  private static Stream<Arguments> decimalTypes() {
    return Stream.of(
        Arguments.of("123.45", Types.DecimalType.of(5, 2)),
        Arguments.of("-1.5", Types.DecimalType.of(2, 1)),
        Arguments.of("0.01", Types.DecimalType.of(2, 2)),
        Arguments.of("1E+3", Types.DecimalType.of(4, 0)));
  }

  @ParameterizedTest
  @MethodSource("decimalTypes")
  void decimalType(String value, Type expected) {
    assertThat(Literal.of(new BigDecimal(value)).type()).isEqualTo(expected);
  }

  @Test
  void decimalTypeRejectsPrecisionAbove38() {
    assertThatThrownBy(() -> Literal.of(new BigDecimal("1E+38")).type())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Decimals with precision larger than 38 are not supported: 39");
  }

  @Test
  void wideDecimalSerializesToBytes() {
    BigDecimal wide = new BigDecimal("1E+38");

    assertThat(Literal.of(wide).toByteBuffer())
        .isEqualTo(ByteBuffer.wrap(wide.unscaledValue().toByteArray()));
  }

  @Test
  void sentinelsHaveNoType() {
    assertThatThrownBy(() -> Literals.aboveMax().type())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("AboveMax does not have a type");
    assertThatThrownBy(() -> Literals.belowMin().type())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("BelowMin does not have a type");
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void timestampZoneSurvivesSerialization(RoundTripSerializer<Literal<Long>> serializer)
      throws Exception {
    Literal<Long> literal = Literal.of(TIMESTAMPTZ).to(Types.TimestampType.withZone());

    assertThat(serializer.apply(literal).type()).isEqualTo(Types.TimestampType.withZone());
  }
}
