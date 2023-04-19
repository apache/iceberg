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

import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class UnicodeUtil {
  // not meant to be instantiated
  private UnicodeUtil() {}

  /**
   * Determines if the given character value is a unicode high-surrogate code unit. The range of
   * high-surrogates is 0xD800 - 0xDBFF.
   */
  public static boolean isCharHighSurrogate(char ch) {
    return (ch & '\uFC00') == '\uD800'; // 0xDC00 - 0xDFFF shouldn't match
  }

  /**
   * Truncates the input charSequence such that the truncated charSequence is a valid unicode string
   * and the number of unicode characters in the truncated charSequence is lesser than or equal to
   * length
   */
  public static CharSequence truncateString(CharSequence input, int length) {
    Preconditions.checkArgument(length > 0, "Truncate length should be positive");
    StringBuilder sb = new StringBuilder(input);
    // Get the number of unicode characters in the input
    int numUniCodeCharacters = sb.codePointCount(0, sb.length());
    // No need to truncate if the number of unicode characters in the char sequence is <= truncate
    // length
    if (length >= numUniCodeCharacters) {
      return input;
    }
    // Get the offset in the input charSequence where the number of unicode characters = truncate
    // length
    int offsetByCodePoint = sb.offsetByCodePoints(0, length);
    return input.subSequence(0, offsetByCodePoint);
  }

  /**
   * Returns a valid unicode charsequence that is lower than the given input such that the number of
   * unicode characters in the truncated charSequence is lesser than or equal to length
   */
  public static Literal<CharSequence> truncateStringMin(Literal<CharSequence> input, int length) {
    // Truncate the input to the specified truncate length.
    CharSequence truncatedInput = truncateString(input.value(), length);
    return Literal.of(truncatedInput);
  }

  /**
   * Returns a valid unicode charsequence that is greater than the given input such that the number
   * of unicode characters in the truncated charSequence is lesser than or equal to length
   */
  public static Literal<CharSequence> truncateStringMax(Literal<CharSequence> input, int length) {
    CharSequence inputCharSeq = input.value();
    // Truncate the input to the specified truncate length.
    StringBuilder truncatedStringBuilder = new StringBuilder(truncateString(inputCharSeq, length));

    // No need to increment if the input length is under the truncate length
    if (inputCharSeq.length() == truncatedStringBuilder.length()) {
      return input;
    }

    // Try incrementing the code points from the end
    for (int i = length - 1; i >= 0; i--) {
      // Get the offset in the truncated string buffer where the number of unicode characters = i
      int offsetByCodePoint = truncatedStringBuilder.offsetByCodePoints(0, i);
      int nextCodePoint = truncatedStringBuilder.codePointAt(offsetByCodePoint) + 1;
      // No overflow
      if (nextCodePoint != 0 && Character.isValidCodePoint(nextCodePoint)) {
        truncatedStringBuilder.setLength(offsetByCodePoint);
        // Append next code point to the truncated substring
        truncatedStringBuilder.appendCodePoint(nextCodePoint);
        return Literal.of(truncatedStringBuilder.toString());
      }
    }
    return null; // Cannot find a valid upper bound
  }
}
