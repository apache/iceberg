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
package org.apache.iceberg.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Contains the logic for various {@code truncate} transformations for various types.
 *
 * <p>This utility class allows for the logic to be reused in different scenarios where input
 * validation is done at different times either in org.apache.iceberg.transforms.Truncate and within
 * defined SQL functions for different compute engines for usage in SQL.
 *
 * <p>In general, the inputs to the functions should have already been validated by the calling
 * code, as different classes use truncate with different preprocessing. This generally means that
 * the truncation width is positive and the value to truncate is non-null.
 *
 * <p>Thus, <b>none</b> of these utility functions validate their input. <i>It is the responsibility
 * of the calling code to validate input.</i>
 *
 * <p>See also {@linkplain UnicodeUtil#truncateString(CharSequence, int)} and {@link
 * BinaryUtil#truncateBinaryUnsafe(ByteBuffer, int)} for similar methods for Strings and
 * ByteBuffers.
 */
public class TruncateUtil {

  private TruncateUtil() {}

  /**
   * Truncates a value to a multiple of the width, rounding toward negative infinity.
   *
   * @param width the truncation width, must be positive
   * @param value the value to truncate
   * @return the truncated value
   * @throws IllegalArgumentException if the truncated value does not fit in a byte
   */
  public static byte truncateByte(int width, byte value) {
    int truncated = value - Math.floorMod(value, width);
    Preconditions.checkArgument(
        truncated >= Byte.MIN_VALUE && truncated <= Byte.MAX_VALUE,
        "Truncated value %s does not fit in a byte: truncate(%s, %s)",
        truncated,
        width,
        value);
    return (byte) truncated;
  }

  /**
   * Truncates a value to a multiple of the width, rounding toward negative infinity.
   *
   * @param width the truncation width, must be positive
   * @param value the value to truncate
   * @return the truncated value
   * @throws IllegalArgumentException if the truncated value does not fit in a short
   */
  public static short truncateShort(int width, short value) {
    int truncated = value - Math.floorMod(value, width);
    Preconditions.checkArgument(
        truncated >= Short.MIN_VALUE && truncated <= Short.MAX_VALUE,
        "Truncated value %s does not fit in a short: truncate(%s, %s)",
        truncated,
        width,
        value);
    return (short) truncated;
  }

  /**
   * Truncates a value to a multiple of the width, rounding toward negative infinity.
   *
   * @param width the truncation width, must be positive
   * @param value the value to truncate
   * @return the truncated value
   * @throws IllegalArgumentException if the truncated value does not fit in an int
   */
  public static int truncateInt(int width, int value) {
    long truncated = (long) value - Math.floorMod(value, width);
    Preconditions.checkArgument(
        truncated >= Integer.MIN_VALUE && truncated <= Integer.MAX_VALUE,
        "Truncated value %s does not fit in an int: truncate(%s, %s)",
        truncated,
        width,
        value);
    return (int) truncated;
  }

  /**
   * Truncates a value to a multiple of the width, rounding toward negative infinity.
   *
   * @param width the truncation width, must be positive
   * @param value the value to truncate
   * @return the truncated value
   * @throws IllegalArgumentException if the truncated value does not fit in a long
   */
  public static long truncateLong(int width, long value) {
    long remainder = Math.floorMod(value, (long) width);
    long truncated;
    try {
      truncated = Math.subtractExact(value, remainder);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Truncated value does not fit in a long: truncate(" + width + ", " + value + ")", e);
    }
    return truncated;
  }

  public static BigDecimal truncateDecimal(BigInteger unscaledWidth, BigDecimal value) {
    BigDecimal remainder =
        new BigDecimal(
            value
                .unscaledValue()
                .remainder(unscaledWidth)
                .add(unscaledWidth)
                .remainder(unscaledWidth),
            value.scale());

    return value.subtract(remainder);
  }
}
