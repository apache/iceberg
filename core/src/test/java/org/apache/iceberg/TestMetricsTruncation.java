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
package org.apache.iceberg;

import static org.apache.iceberg.util.BinaryUtil.truncateBinary;
import static org.apache.iceberg.util.BinaryUtil.truncateBinaryMax;
import static org.apache.iceberg.util.BinaryUtil.truncateBinaryMin;
import static org.apache.iceberg.util.UnicodeUtil.truncateStringMax;
import static org.apache.iceberg.util.UnicodeUtil.truncateStringMin;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.iceberg.expressions.Literal;
import org.junit.jupiter.api.Test;

@SuppressWarnings("checkstyle:LocalVariableName")
public class TestMetricsTruncation {

  @Test
  public void testTruncateBinary() {
    ByteBuffer original = ByteBuffer.wrap(new byte[] {1, 1, (byte) 0xFF, 2});
    ByteBuffer emptyByteBuffer = ByteBuffer.allocate(0);
    Comparator<ByteBuffer> cmp = Literal.of(original).comparator();

    assertThat(cmp.compare(truncateBinary(original, 0), emptyByteBuffer))
        .as("Truncating to a length of zero should return an empty ByteBuffer")
        .isEqualTo(0);
    assertThat(truncateBinary(original, original.remaining()))
        .as("Truncating to the original buffer's remaining size should return the original buffer")
        .isEqualTo(original);
    assertThat(truncateBinary(original, 16))
        .as(
            "Truncating with a length greater than the input's remaining size should return the input")
        .isEqualTo(original);
    ByteBuffer truncated = truncateBinary(original, 2);
    assertThat(truncated.remaining())
        .as(
            "Truncating with a length less than the input's remaining size should truncate properly")
        .isEqualTo(2);
    assertThat(truncated.position())
        .as(
            "Truncating with a length less than the input's remaining size should truncate properly")
        .isEqualTo(0);
    assertThat(original.remaining())
        .as("Truncating should not modify the input buffer")
        .isEqualTo(4);
    assertThat(original.position())
        .as("Truncating should not modify the input buffer")
        .isEqualTo(0);
    assertThatThrownBy(() -> truncateBinary(original, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Truncate length should be non-negative");
  }

  @Test
  public void testTruncateBinaryMin() {
    ByteBuffer test1 = ByteBuffer.wrap(new byte[] {1, 1, (byte) 0xFF, 2});
    // Output of test1 when truncated to 2 bytes
    ByteBuffer test1_2_expected = ByteBuffer.wrap(new byte[] {1, 1});
    ByteBuffer test2 = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 2});
    ByteBuffer test2_2 = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF});

    Comparator<ByteBuffer> cmp = Literal.of(test1).comparator();
    assertThat(cmp.compare(truncateBinaryMin(Literal.of(test1), 2).value(), test1))
        .as("Truncated lower bound should be lower than or equal to the actual lower bound")
        .isLessThanOrEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMin(Literal.of(test1), 2).value(), test1_2_expected))
        .as("Output must have the first two bytes of the input")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMin(Literal.of(test1), 5).value(), test1))
        .as("No truncation required as truncate length is greater than the input size")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMin(Literal.of(test2), 2).value(), test2))
        .as("Truncated lower bound should be lower than or equal to the actual lower bound")
        .isLessThanOrEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMin(Literal.of(test2), 2).value(), test2_2))
        .as(
            "Output must have the first two bytes of the input. A lower bound exists "
                + "even though the first two bytes are the max value")
        .isEqualTo(0);
  }

  @Test
  public void testTruncateBinaryMax() {
    ByteBuffer test1 = ByteBuffer.wrap(new byte[] {1, 1, 2});
    ByteBuffer test2 = ByteBuffer.wrap(new byte[] {1, 1, (byte) 0xFF, 2});
    ByteBuffer test3 = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 2});
    ByteBuffer test4 = ByteBuffer.wrap(new byte[] {1, 1, 0});
    ByteBuffer expectedOutput = ByteBuffer.wrap(new byte[] {1, 2});

    Comparator<ByteBuffer> cmp = Literal.of(test1).comparator();
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test1), 2).value(), test1))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test1), 2).value(), expectedOutput))
        .as("Output must have two bytes and the second byte of the input must be incremented")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test2), 2).value(), test2))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test2), 3).value(), expectedOutput))
        .as(
            "Since the third byte is already the max value, output must have two bytes "
                + "with the second byte incremented ")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test3), 5).value(), test3))
        .as("No truncation required as truncate length is greater than the input size")
        .isEqualTo(0);
    assertThat(truncateBinaryMax(Literal.of(test3), 2))
        .as("An upper bound doesn't exist since the first two bytes are the max value")
        .isNull();
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test4), 2).value(), test4))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateBinaryMax(Literal.of(test4), 2).value(), expectedOutput))
        .as(
            "Since a shorter sequence is considered smaller, output must have two bytes "
                + "and the second byte of the input must be incremented")
        .isEqualTo(0);
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @Test
  public void testTruncateStringMin() {
    String test1 = "イロハニホヘト";
    // Output of test1 when truncated to 2 unicode characters
    String test1_2_expected = "イロ";
    String test1_3_expected = "イロハ";
    String test2 = "щщаεはчωいにπάほхεろへσκζ";
    String test2_7_expected = "щщаεはчω";
    // U+FFFF is max 3 byte UTF-8 character
    String test3 = "\uFFFF\uFFFF";
    // test4 consists of 2 4 byte UTF-8 characters
    String test4 = "\uD800\uDC00\uD800\uDC00";
    String test4_1_expected = "\uD800\uDC00";
    Comparator<CharSequence> cmp = Literal.of(test1).comparator();
    assertThat(cmp.compare(truncateStringMin(Literal.of(test1), 3).value(), test1))
        .as("Truncated lower bound should be lower than or equal to the actual lower bound")
        .isLessThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test1), 8).value(), test1))
        .as("No truncation required as truncate length is greater than the input size")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test1), 2).value(), test1_2_expected))
        .as("Output must have the first two characters of the input")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test1), 3).value(), test1_3_expected))
        .as("Output must have the first three characters of the input")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test2), 16).value(), test2))
        .as("Truncated lower bound should be lower than or equal to the actual lower bound")
        .isLessThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test2), 7).value(), test2_7_expected))
        .as("Output must have the first seven characters of the input")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test3), 2).value(), test3))
        .as("Truncated lower bound should be lower than or equal to the actual lower bound")
        .isLessThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test3), 2).value(), test3))
        .as("No truncation required as truncate length is equal to the input size")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test4), 1).value(), test4))
        .as("Truncated lower bound should be lower than or equal to the actual lower bound")
        .isLessThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMin(Literal.of(test4), 1).value(), test4_1_expected))
        .as("Output must have the first 4 byte UTF-8 character of the input")
        .isEqualTo(0);
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @Test
  public void testTruncateStringMax() {
    String test1 = "イロハニホヘト";
    // Output of test1 when truncated to 2 unicode characters
    String test1_2_expected = "イヮ";
    String test1_3_expected = "イロバ";
    String test2 = "щщаεはчωいにπάほхεろへσκζ";
    String test2_7_expected = "щщаεはчϊ";
    String test3 = "aनि\uFFFF\uFFFF";
    String test3_3_expected = "aनी";
    // U+FFFF is max 3 byte UTF-8 character
    String test4 = "\uFFFF\uFFFF";
    String test4_1_expected = "\uD800\uDC00";
    // test5 consists of 2 4 byte max UTF-8 characters
    String test5 = "\uDBFF\uDFFF\uDBFF\uDFFF";
    String test6 = "\uD800\uDFFF\uD800\uDFFF";
    // Increment the previous character
    String test6_1_expected = "\uD801\uDC00";
    String test7 = "\uD83D\uDE02\uD83D\uDE02\uD83D\uDE02";
    String test7_2_expected = "\uD83D\uDE02\uD83D\uDE03";
    String test7_1_expected = "\uD83D\uDE03";

    // Increment the max UTF-8 character will overflow
    String test8 = "a\uDBFF\uDFFFc";
    String test8_2_expected = "b";

    // Increment skip invalid Unicode scalar values [Character.MIN_SURROGATE,
    // Character.MAX_SURROGATE]
    String test9 = "a" + (char) (Character.MIN_SURROGATE - 1) + "b";
    String test9_2_expected = "a" + (char) (Character.MAX_SURROGATE + 1);

    Comparator<CharSequence> cmp = Literal.of(test1).comparator();
    assertThat(cmp.compare(truncateStringMax(Literal.of(test1), 4).value(), test1))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test1), 7).value(), test1))
        .as("No truncation required as truncate length is equal to the input size")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test1), 2).value(), test1_2_expected))
        .as(
            "Output must have two characters and the second character of the input must "
                + "be incremented")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test1), 3).value(), test1_3_expected))
        .as(
            "Output must have three characters and the third character of the input must "
                + "be incremented")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test1), 8).value(), test1))
        .as("No truncation required as truncate length is greater than the input size")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test2), 8).value(), test2))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test2), 7).value(), test2_7_expected))
        .as(
            "Output must have seven characters and the seventh character of the input must be incremented")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test3), 3).value(), test3))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test3), 3).value(), test3_3_expected))
        .as(
            "Output must have three characters and the third character of the input must "
                + "be incremented. The second perceivable character in this string is actually a glyph. It consists of "
                + "two unicode characters")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test4), 1).value(), test4))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test4), 1).value(), test4_1_expected))
        .as(
            "Output must have one character. Since the first character is the max 3 byte "
                + "UTF-8 character, it should be incremented to the lowest 4 byte UTF-8 character")
        .isEqualTo(0);
    assertThat(truncateStringMax(Literal.of(test5), 1))
        .as("An upper bound doesn't exist since the first two characters are max UTF-8 characters")
        .isNull();
    assertThat(cmp.compare(truncateStringMax(Literal.of(test6), 1).value(), test6))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test6), 1).value(), test6_1_expected))
        .as(
            "Test 4 byte UTF-8 character increment. Output must have one character with "
                + "the first character incremented")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test7), 2).value(), test7))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test7), 2).value(), test7_2_expected))
        .as(
            "Test input with multiple 4 byte UTF-8 character where the second unicode character should be incremented")
        .isEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test7), 1).value(), test7_1_expected))
        .as(
            "Test input with multiple 4 byte UTF-8 character where the first unicode character should be incremented")
        .isEqualTo(0);

    assertThat(cmp.compare(truncateStringMax(Literal.of(test8), 2).value(), test8))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);
    assertThat(cmp.compare(truncateStringMax(Literal.of(test8), 2).value(), test8_2_expected))
        .as(
            "Test the last character is the 4-byte max UTF-8 character after truncated where the second-to-last "
                + "character should be incremented")
        .isEqualTo(0);

    assertThat(cmp.compare(truncateStringMax(Literal.of(test9), 2).value(), test9))
        .as("Truncated upper bound should be greater than or equal to the actual upper bound")
        .isGreaterThanOrEqualTo(0);

    assertThat(cmp.compare(truncateStringMax(Literal.of(test9), 2).value(), test9_2_expected))
        .as(
            "Test the last character is `Character.MIN_SURROGATE - 1` after truncated, it should be incremented to "
                + "next valid Unicode scalar value `Character.MAX_SURROGATE + 1`");
  }
}
