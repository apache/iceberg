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

  public static byte truncateByte(int width, byte value) {
    return (byte) (value - (((value % width) + width) % width));
  }

  public static short truncateShort(int width, short value) {
    return (short) (value - (((value % width) + width) % width));
  }

  public static int truncateInt(int width, int value) {
    return value - (((value % width) + width) % width);
  }

  public static long truncateLong(int width, long value) {
    return value - (((value % width) + width) % width);
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
