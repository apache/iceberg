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

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.iceberg.expressions.Literal;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.util.BinaryUtil.truncateBinaryMax;
import static org.apache.iceberg.util.BinaryUtil.truncateBinaryMin;
import static org.apache.iceberg.util.UnicodeUtil.truncateStringMax;
import static org.apache.iceberg.util.UnicodeUtil.truncateStringMin;

public class TestMetricsTruncation {
  @Test
  public void testTruncateBinaryMin() {
    ByteBuffer test1 = ByteBuffer.wrap(new byte[] {1, 1, (byte) 0xFF, 2});
    // Output of test1 when truncated to 2 bytes
    ByteBuffer test12Expected = ByteBuffer.wrap(new byte[] {1, 1});
    ByteBuffer test2 = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 2});
    ByteBuffer test22 = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF});

    Comparator<ByteBuffer> cmp = Literal.of(test1).comparator();
    Assert.assertTrue("Truncated lower bound should be lower than or equal to the actual lower bound",
        cmp.compare(truncateBinaryMin(Literal.of(test1), 2).value(), test1) <= 0);
    Assert.assertTrue("Output must have the first two bytes of the input",
        cmp.compare(truncateBinaryMin(Literal.of(test1), 2).value(), test12Expected) == 0);
    Assert.assertTrue("No truncation required as truncate length is greater than the input size",
        cmp.compare(truncateBinaryMin(Literal.of(test1), 5).value(), test1) == 0);
    Assert.assertTrue("Truncated lower bound should be lower than or equal to the actual lower bound",
        cmp.compare(truncateBinaryMin(Literal.of(test2), 2).value(), test2) <= 0);
    Assert.assertTrue("Output must have the first two bytes of the input. A lower bound exists " +
            "even though the first two bytes are the max value",
        cmp.compare(truncateBinaryMin(Literal.of(test2), 2).value(), test22) == 0);
  }

  @Test
  public void testTruncateBinaryMax() {
    ByteBuffer test1 = ByteBuffer.wrap(new byte[] {1, 1, 2});
    ByteBuffer test2 = ByteBuffer.wrap(new byte[] {1, 1, (byte) 0xFF, 2});
    ByteBuffer test3 = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 2});
    ByteBuffer test4 = ByteBuffer.wrap(new byte[] {1, 1, 0});
    ByteBuffer expectedOutput = ByteBuffer.wrap(new byte[] {1, 2});

    Comparator<ByteBuffer> cmp = Literal.of(test1).comparator();
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper bound",
        cmp.compare(truncateBinaryMax(Literal.of(test1), 2).value(), test1) >= 0);
    Assert.assertTrue("Output must have two bytes and the second byte of the input must be incremented",
        cmp.compare(truncateBinaryMax(Literal.of(test1), 2).value(), expectedOutput) == 0);
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper bound",
        cmp.compare(truncateBinaryMax(Literal.of(test2), 2).value(), test2) >= 0);
    Assert.assertTrue("Since the third byte is already the max value, output must have two bytes " +
        "with the second byte incremented ", cmp.compare(
        truncateBinaryMax(Literal.of(test2), 3).value(), expectedOutput) == 0);
    Assert.assertTrue("No truncation required as truncate length is greater than the input size",
        cmp.compare(truncateBinaryMax(Literal.of(test3), 5).value(), test3) == 0);
    Assert.assertNull("An upper bound doesn't exist since the first two bytes are the max value",
        truncateBinaryMax(Literal.of(test3), 2));
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper bound",
        cmp.compare(truncateBinaryMax(Literal.of(test4), 2).value(), test4) >= 0);
    Assert.assertTrue("Since a shorter sequence is considered smaller, output must have two bytes " +
            "and the second byte of the input must be incremented",
        cmp.compare(truncateBinaryMax(Literal.of(test4), 2).value(), expectedOutput) == 0);
  }

  @Test
  public void testTruncateStringMin() {
    String test1 = "イロハニホヘト";
    // Output of test1 when truncated to 2 unicode characters
    String test12Expected = "イロ";
    String test13Expected = "イロハ";
    String test2 = "щщаεはчωいにπάほхεろへσκζ";
    String test27Expected = "щщаεはчω";
    // CHECKSTYLE:OFF
    // U+FFFF is max 3 byte UTF-8 character
    String test3 = "\uFFFF\uFFFF";
    // test4 consists of 2 4 byte UTF-8 characters
    String test4 = "\uD800\uDC00\uD800\uDC00";
    String test41Expected = "\uD800\uDC00";
    // CHECKSTYLE:ON
    Comparator<CharSequence> cmp = Literal.of(test1).comparator();
    Assert.assertTrue("Truncated lower bound should be lower than or equal to the actual lower bound",
        cmp.compare(truncateStringMin(Literal.of(test1), 3).value(), test1) <= 0);
    Assert.assertTrue("No truncation required as truncate length is greater than the input size",
        cmp.compare(truncateStringMin(Literal.of(test1), 8).value(), test1) == 0);
    Assert.assertTrue("Output must have the first two characters of the input",
        cmp.compare(truncateStringMin(Literal.of(test1), 2).value(), test12Expected) == 0);
    Assert.assertTrue("Output must have the first three characters of the input",
        cmp.compare(truncateStringMin(Literal.of(test1), 3).value(), test13Expected) == 0);
    Assert.assertTrue("Truncated lower bound should be lower than or equal to the actual lower bound",
        cmp.compare(truncateStringMin(Literal.of(test2), 16).value(), test2) <= 0);
    Assert.assertTrue("Output must have the first seven characters of the input",
        cmp.compare(truncateStringMin(Literal.of(test2), 7).value(), test27Expected) == 0);
    Assert.assertTrue("Truncated lower bound should be lower than or equal to the actual lower bound",
        cmp.compare(truncateStringMin(Literal.of(test3), 2).value(), test3) <= 0);
    Assert.assertTrue("No truncation required as truncate length is equal to the input size",
        cmp.compare(truncateStringMin(Literal.of(test3), 2).value(), test3) == 0);
    Assert.assertTrue("Truncated lower bound should be lower than or equal to the actual lower bound",
        cmp.compare(truncateStringMin(Literal.of(test4), 1).value(), test4) <= 0);
    Assert.assertTrue("Output must have the first 4 byte UTF-8 character of the input",
        cmp.compare(truncateStringMin(Literal.of(test4), 1).value(), test41Expected) == 0);
  }

  @Test
  public void testTruncateStringMax() {
    String test1 = "イロハニホヘト";
    // Output of test1 when truncated to 2 unicode characters
    String test12Expected = "イヮ";
    String test13Expected = "イロバ";
    String test2 = "щщаεはчωいにπάほхεろへσκζ";
    String test27Expected = "щщаεはчϊ";
    String test3 = "aनि\uFFFF\uFFFF";
    String test33Expected = "aनी";
    // U+FFFF is max 3 byte UTF-8 character
    // CHECKSTYLE:OFF
    String test4 = "\uFFFF\uFFFF";
    String test41Expected = "\uD800\uDC00";
    // test5 consists of 2 4 byte max UTF-8 characters
    String test5 = "\uDBFF\uDFFF\uDBFF\uDFFF";
    String test6 = "\uD800\uDFFF\uD800\uDFFF";
    // Increment the previous character
    String test62Expected = "\uD801\uDC00";
    // CHECKSTYLE:ON

    Comparator<CharSequence> cmp = Literal.of(test1).comparator();
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper bound",
        cmp.compare(truncateStringMax(Literal.of(test1), 4).value(), test1) >= 0);
    Assert.assertTrue("No truncation required as truncate length is equal to the input size",
        cmp.compare(truncateStringMax(Literal.of(test1), 7).value(), test1) == 0);
    Assert.assertTrue("Output must have two characters and the second character of the input must " +
        "be incremented", cmp.compare(
        truncateStringMax(Literal.of(test1), 2).value(), test12Expected) == 0);
    Assert.assertTrue("Output must have three characters and the third character of the input must " +
        "be incremented", cmp.compare(
        truncateStringMax(Literal.of(test1), 3).value(), test13Expected) == 0);
    Assert.assertTrue("No truncation required as truncate length is greater than the input size",
        cmp.compare(truncateStringMax(Literal.of(test1), 8).value(), test1) == 0);
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper " +
        "bound", cmp.compare(truncateStringMax(Literal.of(test2), 8).value(), test2) >= 0);
    Assert.assertTrue("Output must have seven characters and the seventh character of the input " +
        "must be incremented", cmp.compare(
        truncateStringMax(Literal.of(test2), 7).value(), test27Expected) == 0);
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper " +
        "bound", cmp.compare(truncateStringMax(Literal.of(test3), 3).value(), test3) >= 0);
    Assert.assertTrue("Output must have three characters and the third character of the input must " +
        "be incremented. The second perceivable character in this string is actually a glyph. It consists of " +
        "two unicode characters", cmp.compare(
        truncateStringMax(Literal.of(test3), 3).value(), test33Expected) == 0);
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper bound",
        cmp.compare(truncateStringMax(Literal.of(test4), 1).value(), test4) >= 0);
    Assert.assertTrue("Output must have one character. Since the first character is the max 3 byte " +
            "UTF-8 character, it should be incremented to the lowest 4 byte UTF-8 character",
        cmp.compare(truncateStringMax(Literal.of(test4), 1).value(), test41Expected) == 0);
    Assert.assertNull("An upper bound doesn't exist since the first two characters are max UTF-8 " +
        "characters", truncateStringMax(Literal.of(test5), 1));
    Assert.assertTrue("Truncated upper bound should be greater than or equal to the actual upper bound",
        cmp.compare(truncateStringMax(Literal.of(test6), 2).value(), test6) >= 0);
    Assert.assertTrue("Test 4 byte UTF-8 character increment. Output must have one character with " +
        "the first character incremented", cmp.compare(
        truncateStringMax(Literal.of(test6), 1).value(), test62Expected) == 0);
  }
}
