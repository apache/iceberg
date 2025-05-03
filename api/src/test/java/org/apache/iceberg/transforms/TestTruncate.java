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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
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
  public void testTruncateDecimalResultType() {
    Types.DecimalType sourceType = Types.DecimalType.of(5, 3);

    // Truncate unit = 0.001, possible truncated outputs -99.999, -99.998, ..., 99.999
    // No widening needed
    SerializableFunction<Object, Object> trunc1 = Truncate.get(1).bind(sourceType);
    Type resultType = ((Transform<?, ?>) trunc1).getResultType(sourceType);
    assertThat(resultType).isEqualTo(sourceType);

    // Truncate unit = 0.009, possible truncated outputs -99.999, -99.990, ..., 99.999
    // No widening needed
    SerializableFunction<Object, Object> trunc9 = Truncate.get(9).bind(sourceType);
    resultType = ((Transform<?, ?>) trunc9).getResultType(sourceType);
    assertThat(resultType).isEqualTo(sourceType);

    // Truncate unit = 1.000, possible truncated output -100.000, -99.000, ..., 99.000
    // Needs 1 extra precision widening
    SerializableFunction<Object, Object> trunc1000 = Truncate.get(1000).bind(sourceType);
    resultType = ((Transform<?, ?>) trunc1000).getResultType(sourceType);
    assertThat(resultType).isEqualTo(Types.DecimalType.of(6,3));

    // Truncate unit = 99.999, possible truncated outputs -99.999, 0.000, 99.999
    // No widening needed
    SerializableFunction<Object, Object> trunc99999 = Truncate.get(99999).bind(sourceType);
    resultType = ((Transform<?, ?>) trunc99999).getResultType(sourceType);
    assertThat(resultType).isEqualTo(sourceType);

    sourceType = Types.DecimalType.of(4, 2);

    // Truncate unit = 10000.00, possible truncated outputs -10000.00, 0
    // Needs 3 extra precision widening
    SerializableFunction<Object, Object> trunc1000000 = Truncate.get(1000000).bind(sourceType);
    resultType = ((Transform<?, ?>) trunc1000000).getResultType(sourceType);
    assertThat(resultType).isEqualTo(Types.DecimalType.of(7, 2));
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
    assertThatThrownBy(() -> Truncate.get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid truncate width: 0 (must be > 0)");
  }

  @Test
  public void testUnknownUnsupported() {
    assertThatThrownBy(() -> Transforms.truncate(Types.UnknownType.get(), 22))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot truncate type: unknown");

    Transform<Object, Object> truncate = Transforms.truncate(22);
    assertThatThrownBy(() -> truncate.bind(Types.UnknownType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot bind to unsupported type: unknown");

    assertThat(truncate.canTransform(Types.UnknownType.get())).isFalse();
  }
}
