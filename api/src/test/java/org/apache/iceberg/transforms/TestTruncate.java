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
package org.apache.iceberg.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTruncate {
  @Test
  public void testDeprecatedTruncateInteger() {
    Truncate<Object> trunc = Truncate.get(Types.IntegerType.get(), 10);
    assertThat((int) trunc.apply(0)).isZero();
    assertThat((int) trunc.apply(1)).isZero();
    assertThat((int) trunc.apply(5)).isZero();
    assertThat((int) trunc.apply(9)).isZero();
    assertThat((int) trunc.apply(10)).isEqualTo(10);
    assertThat((int) trunc.apply(11)).isEqualTo(10);
    assertThat((int) trunc.apply(-1)).isEqualTo(-10);
    assertThat((int) trunc.apply(-5)).isEqualTo(-10);
    assertThat((int) trunc.apply(-10)).isEqualTo(-10);
    assertThat((int) trunc.apply(-11)).isEqualTo(-20);
  }

  @Test
  public void testTruncateInteger() {
    Function<Object, Object> trunc = Truncate.get(10).bind(Types.IntegerType.get());
    assertThat((int) trunc.apply(0)).isZero();
    assertThat((int) trunc.apply(1)).isZero();
    assertThat((int) trunc.apply(5)).isZero();
    assertThat((int) trunc.apply(9)).isZero();
    assertThat((int) trunc.apply(10)).isEqualTo(10);
    assertThat((int) trunc.apply(11)).isEqualTo(10);
    assertThat((int) trunc.apply(-1)).isEqualTo(-10);
    assertThat((int) trunc.apply(-5)).isEqualTo(-10);
    assertThat((int) trunc.apply(-10)).isEqualTo(-10);
    assertThat((int) trunc.apply(-11)).isEqualTo(-20);
  }

  @Test
  public void testTruncateLong() {
    Function<Object, Object> trunc = Truncate.get(10).bind(Types.LongType.get());
    assertThat((long) trunc.apply(0L)).isZero();
    assertThat((long) trunc.apply(1L)).isZero();
    assertThat((long) trunc.apply(5L)).isZero();
    assertThat((long) trunc.apply(9L)).isZero();
    assertThat((long) trunc.apply(10L)).isEqualTo(10L);
    assertThat((long) trunc.apply(11L)).isEqualTo(10L);
    assertThat((long) trunc.apply(-1L)).isEqualTo(-10L);
    assertThat((long) trunc.apply(-5L)).isEqualTo(-10L);
    assertThat((long) trunc.apply(-10L)).isEqualTo(-10L);
    assertThat((long) trunc.apply(-11L)).isEqualTo(-20L);
  }

  @Test
  public void testTruncateDecimal() {
    // decimal truncation works by applying the decimal scale to the width: 10 scale 2 = 0.10
    Function<Object, Object> trunc = Truncate.get(10).bind(Types.DecimalType.of(9, 2));
    assertThat(trunc.apply(new BigDecimal("12.34"))).isEqualTo(new BigDecimal("12.30"));
    assertThat(trunc.apply(new BigDecimal("12.30"))).isEqualTo(new BigDecimal("12.30"));
    assertThat(trunc.apply(new BigDecimal("12.29"))).isEqualTo(new BigDecimal("12.20"));
    assertThat(trunc.apply(new BigDecimal("0.05"))).isEqualTo(new BigDecimal("0.00"));
    assertThat(trunc.apply(new BigDecimal("-0.05"))).isEqualTo(new BigDecimal("-0.10"));
  }

  @Test
  public void testTruncateString() {
    Function<Object, Object> trunc = Truncate.get(5).bind(Types.StringType.get());
    assertThat(trunc.apply("abcdefg"))
        .as("Should truncate strings longer than length")
        .isEqualTo("abcde");
    assertThat(trunc.apply("abc"))
        .as("Should not pad strings shorter than length")
        .isEqualTo("abc");
    assertThat(trunc.apply("abcde"))
        .as("Should not alter strings equal to length")
        .isEqualTo("abcde");
  }

  @Test
  public void testTruncateByteBuffer() {
    Function<Object, Object> trunc = Truncate.get(4).bind(Types.BinaryType.get());
    assertThat(trunc.apply(ByteBuffer.wrap("abcdefg".getBytes(StandardCharsets.UTF_8))))
        .as("Should truncate binary longer than length")
        .isEqualTo(ByteBuffer.wrap("abcd".getBytes(StandardCharsets.UTF_8)));
    assertThat(trunc.apply(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8))))
        .as("Should not pad binary shorter than length")
        .isEqualTo(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testVerifiedIllegalWidth() {
    Assertions.assertThatThrownBy(() -> Truncate.get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid truncate width: 0 (must be > 0)");
  }
}
