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
package org.apache.iceberg.variants;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestPrimitiveWrapper {
  private static final VariantPrimitive<?>[] PRIMITIVES =
      new VariantPrimitive[] {
        Variants.ofNull(),
        Variants.of(true),
        Variants.of(false),
        Variants.of((byte) 34),
        Variants.of((byte) -34),
        Variants.of((short) 1234),
        Variants.of((short) -1234),
        Variants.of(12345),
        Variants.of(-12345),
        Variants.of(9876543210L),
        Variants.of(-9876543210L),
        Variants.of(10.11F),
        Variants.of(-10.11F),
        Variants.of(14.3D),
        Variants.of(-14.3D),
        Variants.ofIsoDate("2024-11-07"),
        Variants.ofIsoDate("1957-11-07"),
        Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00"),
        Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00"),
        Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456"),
        Variants.ofIsoTimestampntz("1957-11-07T12:33:54.123456"),
        Variants.of(new BigDecimal("12345.6789")), // decimal4
        Variants.of(new BigDecimal("-12345.6789")), // decimal4
        Variants.of(new BigDecimal("123456789.987654321")), // decimal8
        Variants.of(new BigDecimal("-123456789.987654321")), // decimal8
        Variants.of(new BigDecimal("9876543210.123456789")), // decimal16
        Variants.of(new BigDecimal("-9876543210.123456789")), // decimal16
        Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
        Variants.of(
            "icebergicebergicebergicebergicebergicebergicebergicebergiceberg"), // short string of
        // 63 (9*7) chars
        Variants.of(RandomUtil.generateString(64, new Random(1))), // string of 64 chars
      };

  @ParameterizedTest
  @FieldSource("PRIMITIVES")
  public void testPrimitiveValueSerialization(VariantPrimitive<?> primitive) {
    // write the value to the middle of a large buffer
    int size = primitive.sizeInBytes();
    ByteBuffer buffer = ByteBuffer.allocate(size + 1000).order(ByteOrder.LITTLE_ENDIAN);
    primitive.writeTo(buffer, 300);

    // create a copy that is limited to the value range
    ByteBuffer readBuffer = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    readBuffer.position(300);
    readBuffer.limit(300 + size);

    // read and validate the serialized bytes
    VariantValue actual = Variants.value(SerializedMetadata.EMPTY_V1_METADATA, readBuffer);
    assertThat(actual.type()).isEqualTo(primitive.type());
    assertThat(actual).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.asPrimitive().get()).isEqualTo(primitive.get());
  }
}
