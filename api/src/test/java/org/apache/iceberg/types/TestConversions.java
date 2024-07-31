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
package org.apache.iceberg.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampNanoType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.junit.jupiter.api.Test;

public class TestConversions {

  @Test
  public void testByteBufferConversions() {
    // booleans are stored as 0x00 for 'false' and a non-zero byte for 'true'
    assertConversion(false, BooleanType.get(), new byte[] {0x00});
    assertConversion(true, BooleanType.get(), new byte[] {0x01});
    assertThat(Literal.of(false).toByteBuffer().array()).isEqualTo(new byte[] {0x00});
    assertThat(Literal.of(true).toByteBuffer().array()).isEqualTo(new byte[] {0x01});

    // integers are stored as 4 bytes in little-endian order
    // 84202 is 0...01|01001000|11101010 in binary
    // 11101010 -> -22, 01001000 -> 72, 00000001 -> 1, 00000000 -> 0
    assertConversion(84202, IntegerType.get(), new byte[] {-22, 72, 1, 0});
    assertThat(Literal.of(84202).toByteBuffer().array()).isEqualTo(new byte[] {-22, 72, 1, 0});

    // longs are stored as 8 bytes in little-endian order
    // 200L is 0...0|11001000 in binary
    // 11001000 -> -56, 00000000 -> 0, ... , 00000000 -> 0
    assertConversion(200L, LongType.get(), new byte[] {-56, 0, 0, 0, 0, 0, 0, 0});
    assertThat(Literal.of(200L).toByteBuffer().array())
        .isEqualTo(new byte[] {-56, 0, 0, 0, 0, 0, 0, 0});

    // floats are stored as 4 bytes in little-endian order
    // floating point numbers are represented as sign * 2ˆexponent * mantissa
    // -4.5F is -1 * 2ˆ2 * 1.125 and encoded as 11000000|10010000|0...0 in binary
    // 00000000 -> 0, 00000000 -> 0, 10010000 -> -112, 11000000 -> -64,
    assertConversion(-4.5F, FloatType.get(), new byte[] {0, 0, -112, -64});
    assertThat(Literal.of(-4.5F).toByteBuffer().array()).isEqualTo(new byte[] {0, 0, -112, -64});

    // doubles are stored as 8 bytes in little-endian order
    // floating point numbers are represented as sign * 2ˆexponent * mantissa
    // 6.0 is 1 * 2ˆ4 * 1.5 and encoded as 01000000|00011000|0...0
    // 00000000 -> 0, ... , 00011000 -> 24, 01000000 -> 64
    assertConversion(6.0, DoubleType.get(), new byte[] {0, 0, 0, 0, 0, 0, 24, 64});
    assertThat(Literal.of(6.0).toByteBuffer().array())
        .isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 24, 64});

    // dates are stored as days from 1970-01-01 in a 4-byte little-endian int
    // 1000 is 0...0|00000011|11101000 in binary
    // 11101000 -> -24, 00000011 -> 3, ... , 00000000 -> 0
    assertConversion(1000, DateType.get(), new byte[] {-24, 3, 0, 0});
    assertThat(Literal.of(1000).to(DateType.get()).toByteBuffer().array())
        .isEqualTo(new byte[] {-24, 3, 0, 0});

    // time is stored as microseconds from midnight in an 8-byte little-endian long
    // 10000L is 0...0|00100111|00010000 in binary
    // 00010000 -> 16, 00100111 -> 39, ... , 00000000 -> 0
    assertConversion(10000L, TimeType.get(), new byte[] {16, 39, 0, 0, 0, 0, 0, 0});
    assertThat(Literal.of(10000L).to(TimeType.get()).toByteBuffer().array())
        .isEqualTo(new byte[] {16, 39, 0, 0, 0, 0, 0, 0});

    // timestamps are stored as micro|nanoseconds from 1970-01-01 00:00:00 in an 8-byte
    // little-endian long
    // 400000L is 0...110|00011010|10000000 in binary
    // 10000000 -> -128, 00011010 -> 26, 00000110 -> 6, ... , 00000000 -> 0
    assertConversion(400000L, TimestampType.withoutZone(), new byte[] {-128, 26, 6, 0, 0, 0, 0, 0});
    assertConversion(400000L, TimestampType.withZone(), new byte[] {-128, 26, 6, 0, 0, 0, 0, 0});
    assertThat(Literal.of(400000L).to(TimestampType.withoutZone()).toByteBuffer().array())
        .isEqualTo(new byte[] {-128, 26, 6, 0, 0, 0, 0, 0});
    assertThat(Literal.of(400000L).to(TimestampType.withZone()).toByteBuffer().array())
        .isEqualTo(new byte[] {-128, 26, 6, 0, 0, 0, 0, 0});
    assertConversion(
        400000L, TimestampNanoType.withoutZone(), new byte[] {-128, 26, 6, 0, 0, 0, 0, 0});
    assertConversion(
        400000L, TimestampNanoType.withZone(), new byte[] {-128, 26, 6, 0, 0, 0, 0, 0});
    assertThat(Literal.of(400000L).to(TimestampNanoType.withoutZone()).toByteBuffer().array())
        .isEqualTo(new byte[] {0, -124, -41, 23, 0, 0, 0, 0});
    assertThat(Literal.of(400000L).to(TimestampNanoType.withZone()).toByteBuffer().array())
        .isEqualTo(new byte[] {0, -124, -41, 23, 0, 0, 0, 0});

    // strings are stored as UTF-8 bytes (without length)
    // 'A' -> 65, 'B' -> 66, 'C' -> 67
    assertConversion(CharBuffer.wrap("ABC"), StringType.get(), new byte[] {65, 66, 67});
    assertThat(Literal.of("ABC").toByteBuffer().array()).isEqualTo(new byte[] {65, 66, 67});

    // uuids are stored as 16-byte big-endian values
    // f79c3e09-677c-4bbd-a479-3f349cb785e7 is encoded as F7 9C 3E 09 67 7C 4B BD A4 79 3F 34 9C B7
    // 85 E7
    // 0xF7 -> 11110111 -> -9, 0x9C -> 10011100 -> -100, 0x3E -> 00111110 -> 62,
    // 0x09 -> 00001001 -> 9, 0x67 -> 01100111 -> 103, 0x7C -> 01111100 -> 124,
    // 0x4B -> 01001011 -> 75, 0xBD -> 10111101 -> -67, 0xA4 -> 10100100 -> -92,
    // 0x79 -> 01111001 -> 121, 0x3F -> 00111111 -> 63, 0x34 -> 00110100 -> 52,
    // 0x9C -> 10011100 -> -100, 0xB7 -> 10110111 -> -73, 0x85 -> 10000101 -> -123,
    // 0xE7 -> 11100111 -> -25
    assertConversion(
        UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        UUIDType.get(),
        new byte[] {-9, -100, 62, 9, 103, 124, 75, -67, -92, 121, 63, 52, -100, -73, -123, -25});
    assertThat(
            Literal.of(UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"))
                .toByteBuffer()
                .array())
        .isEqualTo(
            new byte[] {
              -9, -100, 62, 9, 103, 124, 75, -67, -92, 121, 63, 52, -100, -73, -123, -25
            });

    // fixed values are stored directly
    // 'a' -> 97, 'b' -> 98
    assertConversion(
        ByteBuffer.wrap("ab".getBytes(StandardCharsets.UTF_8)),
        FixedType.ofLength(2),
        new byte[] {97, 98});
    assertThat(Literal.of("ab".getBytes(StandardCharsets.UTF_8)).toByteBuffer().array())
        .isEqualTo(new byte[] {97, 98});

    // binary values are stored directly
    // 'Z' -> 90
    assertConversion(
        ByteBuffer.wrap("Z".getBytes(StandardCharsets.UTF_8)), BinaryType.get(), new byte[] {90});
    assertThat(
            Literal.of(ByteBuffer.wrap("Z".getBytes(StandardCharsets.UTF_8)))
                .toByteBuffer()
                .array())
        .isEqualTo(new byte[] {90});

    // decimals are stored as unscaled values in the form of two's-complement big-endian binary,
    // using the minimum number of bytes for the values
    // 345 is 0...1|01011001 in binary
    // 00000001 -> 1, 01011001 -> 89
    assertConversion(new BigDecimal("3.45"), DecimalType.of(3, 2), new byte[] {1, 89});
    assertThat(Literal.of(new BigDecimal("3.45")).toByteBuffer().array())
        .isEqualTo(new byte[] {1, 89});

    // decimal on 3-bytes to test that we use the minimum number of bytes and not a power of 2
    // 1234567 is 00010010|11010110|10000111 in binary
    // 00010010 -> 18, 11010110 -> -42, 10000111 -> -121
    assertConversion(new BigDecimal("123.4567"), DecimalType.of(7, 4), new byte[] {18, -42, -121});
    assertThat(Literal.of(new BigDecimal("123.4567")).toByteBuffer().array())
        .isEqualTo(new byte[] {18, -42, -121});

    // negative decimal to test two's complement
    // -1234567 is 11101101|00101001|01111001 in binary
    // 11101101 -> -19, 00101001 -> 41, 01111001 -> 121
    assertConversion(new BigDecimal("-123.4567"), DecimalType.of(7, 4), new byte[] {-19, 41, 121});
    assertThat(Literal.of(new BigDecimal("-123.4567")).toByteBuffer().array())
        .isEqualTo(new byte[] {-19, 41, 121});

    // test empty byte in decimal
    // 11 is 00001011 in binary
    // 00001011 -> 11
    assertConversion(new BigDecimal("0.011"), DecimalType.of(10, 3), new byte[] {11});
    assertThat(Literal.of(new BigDecimal("0.011")).toByteBuffer().array())
        .isEqualTo(new byte[] {11});
  }

  private <T> void assertConversion(T value, Type type, byte[] expectedBinary) {
    ByteBuffer byteBuffer = Conversions.toByteBuffer(type, value);
    assertThat(byteBuffer.array()).isEqualTo(expectedBinary);
    assertThat(value).isEqualTo(Conversions.fromByteBuffer(type, byteBuffer));
  }
}
