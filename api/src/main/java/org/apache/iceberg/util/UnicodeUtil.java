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
   * Returns a valid String that is lower than the given input such that the number of unicode
   * characters in the truncated String is lesser than or equal to length
   */
  public static String truncateStringMin(String input, int length) {
    return truncateString(input, length).toString();
  }

  /**
   * Returns a valid unicode charsequence that is greater than the given input such that the number
   * of unicode characters in the truncated charSequence is lesser than or equal to length
   */
  public static Literal<CharSequence> truncateStringMax(Literal<CharSequence> input, int length) {
    CharSequence truncated = internalTruncateMax(input.value(), length);
    return truncated != null ? Literal.of(truncated) : null;
  }

  /**
   * Returns a valid String that is greater than the given input such that the number of unicode
   * characters in the truncated String is lesser than or equal to length
   */
  public static String truncateStringMax(String input, int length) {
    CharSequence truncated = internalTruncateMax(input, length);
    return truncated != null ? truncated.toString() : null;
  }

  private static CharSequence internalTruncateMax(CharSequence inputCharSeq, int length) {
    // Truncate the input to the specified truncate length.
    StringBuilder truncatedStringBuilder = new StringBuilder(truncateString(inputCharSeq, length));

    // No need to increment if the input length is under the truncate length
    if (inputCharSeq.length() == truncatedStringBuilder.length()) {
      return inputCharSeq;
    }

    // Try incrementing the code points from the end
    for (int i = length - 1; i >= 0; i--) {
      // Get the offset in the truncated string buffer where the number of unicode characters = i
      int offsetByCodePoint = truncatedStringBuilder.offsetByCodePoints(0, i);
      int nextCodePoint = incrementCodePoint(truncatedStringBuilder.codePointAt(offsetByCodePoint));
      // No overflow
      if (nextCodePoint != 0) {
        truncatedStringBuilder.setLength(offsetByCodePoint);
        // Append next code point to the truncated substring
        truncatedStringBuilder.appendCodePoint(nextCodePoint);
        return truncatedStringBuilder.toString();
      }
    }
    return null; // Cannot find a valid upper bound
  }

  private static int incrementCodePoint(int codePoint) {
    // surrogate code points are not Unicode scalar values,
    // any UTF-8 byte sequence that would otherwise map to code points U+D800..U+DFFF is ill-formed.
    // see https://www.unicode.org/versions/Unicode16.0.0/core-spec/chapter-3/#G27288
    Preconditions.checkArgument(
        codePoint < Character.MIN_SURROGATE || codePoint > Character.MAX_SURROGATE,
        "invalid code point: %s",
        codePoint);

    if (codePoint == Character.MIN_SURROGATE - 1) {
      // increment to the next Unicode scalar value
      return Character.MAX_SURROGATE + 1;
    } else if (codePoint == Character.MAX_CODE_POINT) {
      // overflow
      return 0;
    } else {
      return codePoint + 1;
    }
  }
}
