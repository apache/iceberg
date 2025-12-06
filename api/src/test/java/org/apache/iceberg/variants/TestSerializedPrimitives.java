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
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

public class TestSerializedPrimitives {
  @Test
  public void testNull() {
    VariantPrimitive<?> value = SerializedPrimitive.from(new byte[] {primitiveHeader(0)});

    assertThat(value.type()).isEqualTo(PhysicalType.NULL);
    assertThat(value.get()).isEqualTo(null);
  }

  @Test
  public void testTrue() {
    VariantPrimitive<?> value = SerializedPrimitive.from(new byte[] {primitiveHeader(1)});

    assertThat(value.type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    assertThat(value.get()).isEqualTo(true);
  }

  @Test
  public void testFalse() {
    VariantPrimitive<?> value = SerializedPrimitive.from(new byte[] {primitiveHeader(2)});

    assertThat(value.type()).isEqualTo(PhysicalType.BOOLEAN_FALSE);
    assertThat(value.get()).isEqualTo(false);
  }

  @Test
  public void testInt8() {
    VariantPrimitive<?> value = SerializedPrimitive.from(new byte[] {primitiveHeader(3), 34});

    assertThat(value.type()).isEqualTo(PhysicalType.INT8);
    assertThat(value.get()).isEqualTo((byte) 34);
  }

  @Test
  public void testNegativeInt8() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(new byte[] {primitiveHeader(3), (byte) 0xFF});

    assertThat(value.type()).isEqualTo(PhysicalType.INT8);
    assertThat(value.get()).isEqualTo((byte) -1);
  }

  @Test
  public void testInt16() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(new byte[] {primitiveHeader(4), (byte) 0xD2, 0x04});

    assertThat(value.type()).isEqualTo(PhysicalType.INT16);
    assertThat(value.get()).isEqualTo((short) 1234);
  }

  @Test
  public void testNegativeInt16() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(new byte[] {primitiveHeader(4), (byte) 0xFF, (byte) 0xFF});

    assertThat(value.type()).isEqualTo(PhysicalType.INT16);
    assertThat(value.get()).isEqualTo((short) -1);
  }

  @Test
  public void testInt32() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49});

    assertThat(value.type()).isEqualTo(PhysicalType.INT32);
    assertThat(value.get()).isEqualTo(1234567890);
  }

  @Test
  public void testNegativeInt32() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(5), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    assertThat(value.type()).isEqualTo(PhysicalType.INT32);
    assertThat(value.get()).isEqualTo(-1);
  }

  @Test
  public void testInt64() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(6),
              (byte) 0xB1,
              0x1C,
              0x6C,
              (byte) 0xB1,
              (byte) 0xF4,
              0x10,
              0x22,
              0x11
            });

    assertThat(value.type()).isEqualTo(PhysicalType.INT64);
    assertThat(value.get()).isEqualTo(1234567890987654321L);
  }

  @Test
  public void testNegativeInt64() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(6),
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.INT64);
    assertThat(value.get()).isEqualTo(-1L);
  }

  @Test
  public void testDouble() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(7),
              (byte) 0xB1,
              0x1C,
              0x6C,
              (byte) 0xB1,
              (byte) 0xF4,
              0x10,
              0x22,
              0x11
            });

    assertThat(value.type()).isEqualTo(PhysicalType.DOUBLE);
    assertThat(value.get()).isEqualTo(Double.longBitsToDouble(1234567890987654321L));
  }

  @Test
  public void testNegativeDouble() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(7), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0x80});

    assertThat(value.type()).isEqualTo(PhysicalType.DOUBLE);
    assertThat(value.get()).isEqualTo(-0.0D);
  }

  @Test
  public void testDecimal4() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(8), 0x04, (byte) 0x15, (byte) 0xCD, (byte) 0x5B, 0x07});

    assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL4);
    assertThat(value.get()).isEqualTo(new BigDecimal("12345.6789"));
  }

  @Test
  public void testNegativeDecimal4() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(8), 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL4);
    assertThat(value.get()).isEqualTo(new BigDecimal("-0.0001"));
  }

  @Test
  public void testDecimal8() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(9),
              0x09, // scale=9
              (byte) 0xB1,
              (byte) 0xFA,
              0x52,
              (byte) 0xE0,
              (byte) 0x4B,
              (byte) 0x9B,
              (byte) 0xB6,
              0x01
            });

    assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL8);
    assertThat(value.get()).isEqualTo(new BigDecimal("123456789.987654321"));
  }

  @Test
  public void testNegativeDecimal8() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(9),
              0x09, // scale=9
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL8);
    assertThat(value.get()).isEqualTo(new BigDecimal("-0.000000001"));
  }

  @Test
  public void testDecimal16() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(10),
              0x09, // scale=9
              0x15,
              0x71,
              0x34,
              (byte) 0xB0,
              (byte) 0xB8,
              (byte) 0x87,
              0x10,
              (byte) 0x89,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00
            });

    assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL16);
    assertThat(value.get()).isEqualTo(new BigDecimal("9876543210.123456789"));
  }

  @Test
  public void testNegativeDecimal16() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(10),
              0x09, // scale=9
              (byte) 0xEB,
              (byte) 0x8E,
              (byte) 0xCB,
              0x4F,
              0x47,
              0x78,
              (byte) 0xEF,
              0x76,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
            });

    assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL16);
    assertThat(value.get()).isEqualTo(new BigDecimal("-9876543210.123456789"));
  }

  @Test
  public void testDate() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(new byte[] {primitiveHeader(11), (byte) 0xF4, 0x43, 0x00, 0x00});

    assertThat(value.type()).isEqualTo(PhysicalType.DATE);
    assertThat(DateTimeUtil.daysToIsoDate((int) value.get())).isEqualTo("2017-08-18");
  }

  @Test
  public void testNegativeDate() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(11), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    assertThat(value.type()).isEqualTo(PhysicalType.DATE);
    assertThat(DateTimeUtil.daysToIsoDate((int) value.get())).isEqualTo("1969-12-31");
  }

  @Test
  public void testTimestamptz() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(12),
              0x18,
              (byte) 0xD3,
              (byte) 0xB1,
              (byte) 0xD6,
              0x07,
              0x57,
              0x05,
              0x00
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPTZ);
    assertThat(DateTimeUtil.microsToIsoTimestamptz((long) value.get()))
        .isEqualTo("2017-08-18T14:21:01.919+00:00");
  }

  @Test
  public void testNegativeTimestamptz() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(12),
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPTZ);
    assertThat(DateTimeUtil.microsToIsoTimestamptz((long) value.get()))
        .isEqualTo("1969-12-31T23:59:59.999999+00:00");
  }

  @Test
  public void testTimestampntz() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(13),
              0x18,
              (byte) 0xD3,
              (byte) 0xB1,
              (byte) 0xD6,
              0x07,
              0x57,
              0x05,
              0x00
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPNTZ);
    assertThat(DateTimeUtil.microsToIsoTimestamp((long) value.get()))
        .isEqualTo("2017-08-18T14:21:01.919");
  }

  @Test
  public void testNegativeTimestampntz() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(13),
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPNTZ);
    assertThat(DateTimeUtil.microsToIsoTimestamp((long) value.get()))
        .isEqualTo("1969-12-31T23:59:59.999999");
  }

  @Test
  public void testFloat() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(14), (byte) 0xD2, 0x02, (byte) 0x96, 0x49});

    assertThat(value.type()).isEqualTo(PhysicalType.FLOAT);
    assertThat(value.get()).isEqualTo(Float.intBitsToFloat(1234567890));
  }

  @Test
  public void testNegativeFloat() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(new byte[] {primitiveHeader(14), 0x00, 0x00, 0x00, (byte) 0x80});

    assertThat(value.type()).isEqualTo(PhysicalType.FLOAT);
    assertThat(value.get()).isEqualTo(-0.0F);
  }

  @Test
  public void testBinary() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {primitiveHeader(15), 0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'});

    assertThat(value.type()).isEqualTo(PhysicalType.BINARY);
    assertThat(value.get()).isEqualTo(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd', 'e'}));
  }

  @Test
  public void testString() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'i', 'c', 'e', 'b', 'e', 'r', 'g'
            });

    assertThat(value.type()).isEqualTo(PhysicalType.STRING);
    assertThat(value.get()).isEqualTo("iceberg");
    assertThat(value.sizeInBytes()).isEqualTo(12);
  }

  @Test
  public void testShortString() {
    VariantPrimitive<?> value =
        SerializedShortString.from(new byte[] {0b11101, 'i', 'c', 'e', 'b', 'e', 'r', 'g'});

    VariantTestUtil.assertVariantString(value, "iceberg");
  }

  @Test
  public void testTimeNano() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(17),
              (byte) 0x80,
              (byte) 0xa8,
              (byte) 0x4b,
              (byte) 0xb7,
              (byte) 0x02,
              (byte) 0x00,
              (byte) 0x00,
              0x00
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIME);
    assertThat(DateTimeUtil.timeFromMicros((long) value.get())).isEqualTo("03:14:25.123456");
  }

  @Test
  public void testNegativeTimeNano() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(17),
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff,
              (byte) 0xff
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIME);
    assertThatException()
        .as("Invalid value for NanoOfDay (valid values 0 - 86399999999999): -1000")
        .isThrownBy(() -> DateTimeUtil.timeFromMicros((long) value.get()));
  }

  @Test
  public void testTimestamptzNano() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(18),
              0x15,
              (byte) 0x8f,
              (byte) 0x35,
              (byte) 0x77,
              (byte) 0x9e,
              (byte) 0xf6,
              (byte) 0xdb,
              0x14
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPTZ_NANOS);
    assertThat(DateTimeUtil.nanosToIsoTimestamptz((long) value.get()))
        .isEqualTo("2017-08-18T14:21:01.123456789+00:00");
  }

  @Test
  public void testNegativeTimestamptzNano() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(18),
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPTZ_NANOS);
    assertThat(DateTimeUtil.nanosToIsoTimestamptz((long) value.get()))
        .isEqualTo("1969-12-31T23:59:59.999999999+00:00");
  }

  @Test
  public void testTimestampntzNano() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(19),
              0x15,
              (byte) 0x8f,
              (byte) 0x35,
              (byte) 0x77,
              (byte) 0x9e,
              (byte) 0xf6,
              (byte) 0xdb,
              0x14
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPNTZ_NANOS);
    assertThat(DateTimeUtil.nanosToIsoTimestamp((long) value.get()))
        .isEqualTo("2017-08-18T14:21:01.123456789");
  }

  @Test
  public void testNegativeTimestampntzNano() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(19),
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            });

    assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPNTZ_NANOS);
    assertThat(DateTimeUtil.nanosToIsoTimestamp((long) value.get()))
        .isEqualTo("1969-12-31T23:59:59.999999999");
  }

  @Test
  public void testUUID() {
    VariantPrimitive<?> value =
        SerializedPrimitive.from(
            new byte[] {
              primitiveHeader(20),
              (byte) 0xf2,
              0x4f,
              (byte) 0x9b,
              0x64,
              (byte) 0x81,
              (byte) 0xfa,
              0x49,
              (byte) 0xd1,
              (byte) 0xb7,
              0x4e,
              (byte) 0x8c,
              0x09,
              (byte) 0xa6,
              (byte) 0xe3,
              0x1c,
              0x56
            });

    assertThat(value.type()).isEqualTo(PhysicalType.UUID);
    assertThat(value.get()).isEqualTo(UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"));
  }

  @Test
  public void testUnsupportedType() {
    assertThatThrownBy(() -> SerializedPrimitive.from(new byte[] {primitiveHeader(21)}))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unknown primitive physical type: 21");
  }

  private static byte primitiveHeader(int primitiveType) {
    return (byte) (primitiveType << 2);
  }
}
