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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestUnicodeUtil {

  @Test
  void isCharHighSurrogate() {
    // the high-surrogate range is 0xD800 - 0xDBFF
    assertThat(UnicodeUtil.isCharHighSurrogate('\uD800')).isTrue(); // first high surrogate
    assertThat(UnicodeUtil.isCharHighSurrogate('\uDBFF')).isTrue(); // last high surrogate
    // low surrogates and regular characters are not high surrogates
    assertThat(UnicodeUtil.isCharHighSurrogate('\uDC00')).isFalse(); // first low surrogate
    assertThat(UnicodeUtil.isCharHighSurrogate('a')).isFalse();
  }

  @Test
  void truncateStringShorterThanLength() {
    // no truncation is needed when the length is at least the number of characters
    assertThat(UnicodeUtil.truncateString("abc", 5)).isEqualTo("abc");
    assertThat(UnicodeUtil.truncateString("abc", 3)).isEqualTo("abc");
  }

  @Test
  void truncateStringByCodePoints() {
    assertThat(UnicodeUtil.truncateString("abcde", 3)).isEqualTo("abc");
    // a surrogate pair is a single unicode character and is not split
    String withSurrogatePair = "a😀b"; // "a" + emoji + "b"
    assertThat(UnicodeUtil.truncateString(withSurrogatePair, 2)).isEqualTo("a😀");
  }

  @Test
  void truncateStringRejectsNonPositiveLength() {
    assertThatThrownBy(() -> UnicodeUtil.truncateString("abc", 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Truncate length should be positive");
  }

  @Test
  void truncateStringMin() {
    // longer than the length: the minimum is the truncated prefix
    assertThat(UnicodeUtil.truncateStringMin("abcde", 3)).isEqualTo("abc");

    // equal to or shorter than the length: the input is already a valid lower bound
    assertThat(UnicodeUtil.truncateStringMin("abc", 3)).isEqualTo("abc");
    assertThat(UnicodeUtil.truncateStringMin("abc", 5)).isEqualTo("abc");
    assertThat(UnicodeUtil.truncateStringMin("", 3)).isEqualTo("");
  }

  @Test
  void truncateStringMax() {
    // longer than the length: the maximum increments the last retained code point
    assertThat(UnicodeUtil.truncateStringMax("abcde", 3)).isEqualTo("abd");

    // equal to or shorter than the length: the input is already a valid upper bound
    assertThat(UnicodeUtil.truncateStringMax("abc", 3)).isEqualTo("abc");
    assertThat(UnicodeUtil.truncateStringMax("abc", 5)).isEqualTo("abc");
    assertThat(UnicodeUtil.truncateStringMax("", 3)).isEqualTo("");
  }

  @Test
  void truncateStringMaxCarriesOnOverflow() {
    // the last retained code point cannot be incremented, so the carry moves to the previous one
    String input =
        new StringBuilder()
            .append('a')
            .appendCodePoint(Character.MAX_CODE_POINT)
            .append('c')
            .toString();
    assertThat(UnicodeUtil.truncateStringMax(input, 2)).isEqualTo("b");
  }

  @Test
  void truncateStringMaxSkipsSurrogateRange() {
    // the code point below the surrogate block must increment to the one above it, because
    // surrogate code points are not valid unicode scalar values
    String input =
        new StringBuilder().appendCodePoint(Character.MIN_SURROGATE - 1).append("extra").toString();
    String expected = new StringBuilder().appendCodePoint(Character.MAX_SURROGATE + 1).toString();
    assertThat(UnicodeUtil.truncateStringMax(input, 1)).isEqualTo(expected);
  }

  @Test
  void truncateStringMaxReturnsNullOnOverflow() {
    // when every retained code point is the maximum, no greater bound exists
    String maxCodePoints =
        new StringBuilder()
            .appendCodePoint(Character.MAX_CODE_POINT)
            .appendCodePoint(Character.MAX_CODE_POINT)
            .toString();
    assertThat(UnicodeUtil.truncateStringMax(maxCodePoints, 1)).isNull();
  }
}
