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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestTruncate {
  @Test
  public void testDeprecatedTruncateInteger() {
    Truncate<Object> trunc = Truncate.get(Types.IntegerType.get(), 10);
    Assert.assertEquals(0, (int) trunc.apply(0));
    Assert.assertEquals(0, (int) trunc.apply(1));
    Assert.assertEquals(0, (int) trunc.apply(5));
    Assert.assertEquals(0, (int) trunc.apply(9));
    Assert.assertEquals(10, (int) trunc.apply(10));
    Assert.assertEquals(10, (int) trunc.apply(11));
    Assert.assertEquals(-10, (int) trunc.apply(-1));
    Assert.assertEquals(-10, (int) trunc.apply(-5));
    Assert.assertEquals(-10, (int) trunc.apply(-10));
    Assert.assertEquals(-20, (int) trunc.apply(-11));
  }

  @Test
  public void testTruncateInteger() {
    Function<Object, Object> trunc = Truncate.get(10).bind(Types.IntegerType.get());
    Assert.assertEquals(0, (int) trunc.apply(0));
    Assert.assertEquals(0, (int) trunc.apply(1));
    Assert.assertEquals(0, (int) trunc.apply(5));
    Assert.assertEquals(0, (int) trunc.apply(9));
    Assert.assertEquals(10, (int) trunc.apply(10));
    Assert.assertEquals(10, (int) trunc.apply(11));
    Assert.assertEquals(-10, (int) trunc.apply(-1));
    Assert.assertEquals(-10, (int) trunc.apply(-5));
    Assert.assertEquals(-10, (int) trunc.apply(-10));
    Assert.assertEquals(-20, (int) trunc.apply(-11));
  }

  @Test
  public void testTruncateLong() {
    Function<Object, Object> trunc = Truncate.get(10).bind(Types.LongType.get());
    Assert.assertEquals(0L, (long) trunc.apply(0L));
    Assert.assertEquals(0L, (long) trunc.apply(1L));
    Assert.assertEquals(0L, (long) trunc.apply(5L));
    Assert.assertEquals(0L, (long) trunc.apply(9L));
    Assert.assertEquals(10L, (long) trunc.apply(10L));
    Assert.assertEquals(10L, (long) trunc.apply(11L));
    Assert.assertEquals(-10L, (long) trunc.apply(-1L));
    Assert.assertEquals(-10L, (long) trunc.apply(-5L));
    Assert.assertEquals(-10L, (long) trunc.apply(-10L));
    Assert.assertEquals(-20L, (long) trunc.apply(-11L));
  }

  @Test
  public void testTruncateDecimal() {
    // decimal truncation works by applying the decimal scale to the width: 10 scale 2 = 0.10
    Function<Object, Object> trunc = Truncate.get(10).bind(Types.DecimalType.of(9, 2));
    Assert.assertEquals(new BigDecimal("12.30"), trunc.apply(new BigDecimal("12.34")));
    Assert.assertEquals(new BigDecimal("12.30"), trunc.apply(new BigDecimal("12.30")));
    Assert.assertEquals(new BigDecimal("12.20"), trunc.apply(new BigDecimal("12.29")));
    Assert.assertEquals(new BigDecimal("0.00"), trunc.apply(new BigDecimal("0.05")));
    Assert.assertEquals(new BigDecimal("-0.10"), trunc.apply(new BigDecimal("-0.05")));
  }

  @Test
  public void testTruncateString() {
    Function<Object, Object> trunc = Truncate.get(5).bind(Types.StringType.get());
    Assert.assertEquals(
        "Should truncate strings longer than length", "abcde", trunc.apply("abcdefg"));
    Assert.assertEquals("Should not pad strings shorter than length", "abc", trunc.apply("abc"));
    Assert.assertEquals("Should not alter strings equal to length", "abcde", trunc.apply("abcde"));
  }

  @Test
  public void testTruncateByteBuffer() {
    Function<Object, Object> trunc = Truncate.get(4).bind(Types.BinaryType.get());
    Assert.assertEquals(
        "Should truncate binary longer than length",
        ByteBuffer.wrap("abcd".getBytes(StandardCharsets.UTF_8)),
        trunc.apply(ByteBuffer.wrap("abcdefg".getBytes(StandardCharsets.UTF_8))));
    Assert.assertEquals(
        "Should not pad binary shorter than length",
        ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8)),
        trunc.apply(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testVerifiedIllegalWidth() {
    Assertions.assertThatThrownBy(() -> Truncate.get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid truncate width: 0 (must be > 0)");
  }
}
