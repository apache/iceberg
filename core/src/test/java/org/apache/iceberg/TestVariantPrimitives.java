/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.Variants.PhysicalType;
import org.apache.iceberg.Variants.Primitive;
import org.apache.iceberg.util.DateTimeUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestVariantPrimitives {
  @Test
  public void testNull() {
    Primitive<?> value = VariantPrimitive.from(new byte[] {primitiveHeader(0)});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.NULL);
    Assertions.assertThat(value.get()).isEqualTo(null);
  }

  @Test
  public void testTrue() {
    Primitive<?> value = VariantPrimitive.from(new byte[] {primitiveHeader(1)});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    Assertions.assertThat(value.get()).isEqualTo(true);
  }

  @Test
  public void testFalse() {
    Primitive<?> value = VariantPrimitive.from(new byte[] {primitiveHeader(2)});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.BOOLEAN_FALSE);
    Assertions.assertThat(value.get()).isEqualTo(false);
  }

  @Test
  public void testInt8() {
    Primitive<?> value = VariantPrimitive.from(new byte[] {primitiveHeader(3), 34});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(value.get()).isEqualTo(34);
  }

  @Test
  public void testNegativeInt8() {
    Primitive<?> value = VariantPrimitive.from(new byte[] {primitiveHeader(3), (byte) 0xFF});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(value.get()).isEqualTo(-1);
  }

  @Test
  public void testInt16() {
    Primitive<?> value =
        VariantPrimitive.from(new byte[] {primitiveHeader(4), (byte) 0xD2, 0x04});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT16);
    Assertions.assertThat(value.get()).isEqualTo(1234);
  }

  @Test
  public void testNegativeInt16() {
    Primitive<?> value =
        VariantPrimitive.from(new byte[] {primitiveHeader(4), (byte) 0xFF, (byte) 0xFF});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT16);
    Assertions.assertThat(value.get()).isEqualTo(-1);
  }

  @Test
  public void testInt32() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(5), (byte) 0xD2, 0x02, (byte) 0x96, 0x49});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT32);
    Assertions.assertThat(value.get()).isEqualTo(1234567890);
  }

  @Test
  public void testNegativeInt32() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(5), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT32);
    Assertions.assertThat(value.get()).isEqualTo(-1);
  }

  @Test
  public void testInt64() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT64);
    Assertions.assertThat(value.get()).isEqualTo(1234567890987654321L);
  }

  @Test
  public void testNegativeInt64() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.INT64);
    Assertions.assertThat(value.get()).isEqualTo(-1L);
  }

  @Test
  public void testDouble() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DOUBLE);
    Assertions.assertThat(value.get()).isEqualTo(Double.longBitsToDouble(1234567890987654321L));
  }

  @Test
  public void testNegativeDouble() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(7), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0x80});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DOUBLE);
    Assertions.assertThat(value.get()).isEqualTo(-0.0D);
  }

  @Test
  public void testDecimal4() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(8), 0x04, (byte) 0xD2, 0x02, (byte) 0x96, 0x49});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL4);
    Assertions.assertThat(value.get()).isEqualTo(new BigDecimal("123456.7890"));
  }

  @Test
  public void testNegativeDecimal4() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {
              primitiveHeader(8), 0x04, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            });

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL4);
    Assertions.assertThat(value.get()).isEqualTo(new BigDecimal("-0.0001"));
  }

  @Test
  public void testDecimal8() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {
              primitiveHeader(9),
              0x09, // scale=9
              (byte) 0xB1,
              0x1C,
              0x6C,
              (byte) 0xB1,
              (byte) 0xF4,
              0x10,
              0x22,
              0x11
            });

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL8);
    Assertions.assertThat(value.get()).isEqualTo(new BigDecimal("1234567890.987654321"));
  }

  @Test
  public void testNegativeDecimal8() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL8);
    Assertions.assertThat(value.get()).isEqualTo(new BigDecimal("-0.000000001"));
  }

  @Test
  public void testDecimal16() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {
              primitiveHeader(10), 0x09, // scale=9
            });

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DECIMAL16);
    Assertions.assertThatThrownBy(value::get).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testDate() {
    Primitive<?> value =
        VariantPrimitive.from(new byte[] {primitiveHeader(11), (byte) 0xF4, 0x43, 0x00, 0x00});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DATE);
    Assertions.assertThat(DateTimeUtil.daysToIsoDate((int) value.get())).isEqualTo("2017-08-18");
  }

  @Test
  public void testNegativeDate() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(11), (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.DATE);
    Assertions.assertThat(DateTimeUtil.daysToIsoDate((int) value.get())).isEqualTo("1969-12-31");
  }

  @Test
  public void testTimestamptz() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPTZ);
    Assertions.assertThat(DateTimeUtil.microsToIsoTimestamptz((long) value.get()))
        .isEqualTo("2017-08-18T14:21:01.919+00:00");
  }

  @Test
  public void testNegativeTimestamptz() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPTZ);
    Assertions.assertThat(DateTimeUtil.microsToIsoTimestamptz((long) value.get()))
        .isEqualTo("1969-12-31T23:59:59.999999+00:00");
  }

  @Test
  public void testTimestampntz() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPNTZ);
    Assertions.assertThat(DateTimeUtil.microsToIsoTimestamp((long) value.get()))
        .isEqualTo("2017-08-18T14:21:01.919");
  }

  @Test
  public void testNegativeTimestampntz() {
    Primitive<?> value =
        VariantPrimitive.from(
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

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.TIMESTAMPNTZ);
    Assertions.assertThat(DateTimeUtil.microsToIsoTimestamp((long) value.get()))
        .isEqualTo("1969-12-31T23:59:59.999999");
  }

  @Test
  public void testFloat() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(14), (byte) 0xD2, 0x02, (byte) 0x96, 0x49});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.FLOAT);
    Assertions.assertThat(value.get()).isEqualTo(Float.intBitsToFloat(1234567890));
  }

  @Test
  public void testNegativeFloat() {
    Primitive<?> value =
        VariantPrimitive.from(new byte[] {primitiveHeader(14), 0x00, 0x00, 0x00, (byte) 0x80});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.FLOAT);
    Assertions.assertThat(value.get()).isEqualTo(-0.0F);
  }

  @Test
  public void testBinary() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {primitiveHeader(15), 0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.BINARY);
    Assertions.assertThat(value.get())
        .isEqualTo(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd', 'e'}));
  }

  @Test
  public void testString() {
    Primitive<?> value =
        VariantPrimitive.from(
            new byte[] {
              primitiveHeader(16), 0x07, 0x00, 0x00, 0x00, 'i', 'c', 'e', 'b', 'e', 'r', 'g'
            });

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(value.get()).isEqualTo("iceberg");
  }

  @Test
  public void testShortString() {
    Primitive<?> value =
        VariantShortString.from(new byte[] {0b11101, 'i', 'c', 'e', 'b', 'e', 'r', 'g'});

    Assertions.assertThat(value.type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(value.get()).isEqualTo("iceberg");
  }

  @Test
  public void testUnsupportedType() {
    Assertions.assertThatThrownBy(() -> VariantPrimitive.from(new byte[] {primitiveHeader(17)}))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unknown primitive physical type: 17");
  }

  private static byte primitiveHeader(int primitiveType) {
    return (byte) (primitiveType << 2);
  }
}
