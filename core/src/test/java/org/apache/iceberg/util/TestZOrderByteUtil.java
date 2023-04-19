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

import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.primitives.UnsignedBytes;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestZOrderByteUtil {
  private static final byte IIIIIIII = (byte) 255;
  private static final byte IOIOIOIO = (byte) 170;
  private static final byte OIOIOIOI = (byte) 85;
  private static final byte OOOOIIII = (byte) 15;
  private static final byte OOOOOOOI = (byte) 1;
  private static final byte OOOOOOOO = (byte) 0;

  private static final int NUM_TESTS = 100000;
  private static final int NUM_INTERLEAVE_TESTS = 1000;

  private final Random random = new Random(42);

  private String bytesToString(byte[] bytes) {
    StringBuilder result = new StringBuilder();
    for (byte b : bytes) {
      result.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
    }
    return result.toString();
  }

  /** Returns a non-0 length byte array */
  private byte[] generateRandomBytes() {
    int length = Math.abs(random.nextInt(100) + 1);
    return generateRandomBytes(length);
  }

  /** Returns a byte array of a specified length */
  private byte[] generateRandomBytes(int length) {
    byte[] result = new byte[length];
    random.nextBytes(result);
    return result;
  }

  /** Test method to ensure correctness of byte interleaving code */
  private String interleaveStrings(String[] strings) {
    StringBuilder result = new StringBuilder();
    int totalLength = Arrays.stream(strings).mapToInt(String::length).sum();
    int substringIndex = 0;
    int characterIndex = 0;
    while (characterIndex < totalLength) {
      for (String str : strings) {
        if (substringIndex < str.length()) {
          result.append(str.charAt(substringIndex));
          characterIndex++;
        }
      }
      substringIndex++;
    }
    return result.toString();
  }

  /**
   * Compares the result of a string based interleaving algorithm implemented above versus the
   * binary bit-shifting algorithm used in ZOrderByteUtils. Either both algorithms are identically
   * wrong or are both identically correct.
   */
  @Test
  public void testInterleaveRandomExamples() {
    for (int test = 0; test < NUM_INTERLEAVE_TESTS; test++) {
      int numByteArrays = Math.abs(random.nextInt(6)) + 1;
      byte[][] testBytes = new byte[numByteArrays][];
      String[] testStrings = new String[numByteArrays];
      for (int byteIndex = 0; byteIndex < numByteArrays; byteIndex++) {
        testBytes[byteIndex] = generateRandomBytes();
        testStrings[byteIndex] = bytesToString(testBytes[byteIndex]);
      }

      int zOrderSize = Arrays.stream(testBytes).mapToInt(column -> column.length).sum();
      byte[] byteResult = ZOrderByteUtils.interleaveBits(testBytes, zOrderSize);
      String byteResultAsString = bytesToString(byteResult);

      String stringResult = interleaveStrings(testStrings);

      Assert.assertEquals(
          "String interleave didn't match byte interleave", stringResult, byteResultAsString);
    }
  }

  @Test
  public void testReuseInterleaveBuffer() {
    int numByteArrays = 2;
    int colLength = 16;
    ByteBuffer interleaveBuffer = ByteBuffer.allocate(numByteArrays * colLength);
    for (int test = 0; test < NUM_INTERLEAVE_TESTS; test++) {
      byte[][] testBytes = new byte[numByteArrays][];
      String[] testStrings = new String[numByteArrays];
      for (int byteIndex = 0; byteIndex < numByteArrays; byteIndex++) {
        testBytes[byteIndex] = generateRandomBytes(colLength);
        testStrings[byteIndex] = bytesToString(testBytes[byteIndex]);
      }

      byte[] byteResult =
          ZOrderByteUtils.interleaveBits(testBytes, numByteArrays * colLength, interleaveBuffer);
      String byteResultAsString = bytesToString(byteResult);

      String stringResult = interleaveStrings(testStrings);

      Assert.assertEquals(
          "String interleave didn't match byte interleave", stringResult, byteResultAsString);
    }
  }

  @Test
  public void testInterleaveEmptyBits() {
    byte[][] test = new byte[4][10];
    byte[] expected = new byte[40];

    Assert.assertArrayEquals(
        "Should combine empty arrays", expected, ZOrderByteUtils.interleaveBits(test, 40));
  }

  @Test
  public void testInterleaveFullBits() {
    byte[][] test = new byte[4][];
    test[0] = new byte[] {IIIIIIII, IIIIIIII};
    test[1] = new byte[] {IIIIIIII};
    test[2] = new byte[0];
    test[3] = new byte[] {IIIIIIII, IIIIIIII, IIIIIIII};
    byte[] expected = new byte[] {IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII, IIIIIIII};

    Assert.assertArrayEquals(
        "Should combine full arrays", expected, ZOrderByteUtils.interleaveBits(test, 6));
  }

  @Test
  public void testInterleaveMixedBits() {
    byte[][] test = new byte[4][];
    test[0] = new byte[] {OOOOOOOI, IIIIIIII, OOOOOOOO, OOOOIIII};
    test[1] = new byte[] {OOOOOOOI, OOOOOOOO, IIIIIIII};
    test[2] = new byte[] {OOOOOOOI};
    test[3] = new byte[] {OOOOOOOI};
    byte[] expected =
        new byte[] {
          OOOOOOOO, OOOOOOOO, OOOOOOOO, OOOOIIII, IOIOIOIO, IOIOIOIO, OIOIOIOI, OIOIOIOI, OOOOIIII
        };
    Assert.assertArrayEquals(
        "Should combine mixed byte arrays", expected, ZOrderByteUtils.interleaveBits(test, 9));
  }

  @Test
  public void testIntOrdering() {
    ByteBuffer aBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    ByteBuffer bBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    for (int i = 0; i < NUM_TESTS; i++) {
      int aInt = random.nextInt();
      int bInt = random.nextInt();
      int intCompare = Integer.signum(Integer.compare(aInt, bInt));
      byte[] aBytes = ZOrderByteUtils.intToOrderedBytes(aInt, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.intToOrderedBytes(bInt, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of ints should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aInt,
              bInt,
              intCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          intCompare,
          byteCompare);
    }
  }

  @Test
  public void testLongOrdering() {
    ByteBuffer aBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    ByteBuffer bBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    for (int i = 0; i < NUM_TESTS; i++) {
      long aLong = random.nextInt();
      long bLong = random.nextInt();
      int longCompare = Integer.signum(Long.compare(aLong, bLong));
      byte[] aBytes = ZOrderByteUtils.longToOrderedBytes(aLong, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.longToOrderedBytes(bLong, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of longs should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aLong,
              bLong,
              longCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          longCompare,
          byteCompare);
    }
  }

  @Test
  public void testShortOrdering() {
    ByteBuffer aBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    ByteBuffer bBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    for (int i = 0; i < NUM_TESTS; i++) {
      short aShort = (short) (random.nextInt() % (Short.MAX_VALUE + 1));
      short bShort = (short) (random.nextInt() % (Short.MAX_VALUE + 1));
      int longCompare = Integer.signum(Long.compare(aShort, bShort));
      byte[] aBytes = ZOrderByteUtils.shortToOrderedBytes(aShort, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.shortToOrderedBytes(bShort, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of longs should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aShort,
              bShort,
              longCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          longCompare,
          byteCompare);
    }
  }

  @Test
  public void testTinyOrdering() {
    ByteBuffer aBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    ByteBuffer bBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    for (int i = 0; i < NUM_TESTS; i++) {
      byte aByte = (byte) (random.nextInt() % (Byte.MAX_VALUE + 1));
      byte bByte = (byte) (random.nextInt() % (Byte.MAX_VALUE + 1));
      int longCompare = Integer.signum(Long.compare(aByte, bByte));
      byte[] aBytes = ZOrderByteUtils.tinyintToOrderedBytes(aByte, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.tinyintToOrderedBytes(bByte, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of longs should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aByte,
              bByte,
              longCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          longCompare,
          byteCompare);
    }
  }

  @Test
  public void testFloatOrdering() {
    ByteBuffer aBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    ByteBuffer bBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    for (int i = 0; i < NUM_TESTS; i++) {
      float aFloat = random.nextFloat();
      float bFloat = random.nextFloat();
      int floatCompare = Integer.signum(Float.compare(aFloat, bFloat));
      byte[] aBytes = ZOrderByteUtils.floatToOrderedBytes(aFloat, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.floatToOrderedBytes(bFloat, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of floats should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aFloat,
              bFloat,
              floatCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          floatCompare,
          byteCompare);
    }
  }

  @Test
  public void testDoubleOrdering() {
    ByteBuffer aBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    ByteBuffer bBuffer = ZOrderByteUtils.allocatePrimitiveBuffer();
    for (int i = 0; i < NUM_TESTS; i++) {
      double aDouble = random.nextDouble();
      double bDouble = random.nextDouble();
      int doubleCompare = Integer.signum(Double.compare(aDouble, bDouble));
      byte[] aBytes = ZOrderByteUtils.doubleToOrderedBytes(aDouble, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.doubleToOrderedBytes(bDouble, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of doubles should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aDouble,
              bDouble,
              doubleCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          doubleCompare,
          byteCompare);
    }
  }

  @Test
  public void testStringOrdering() {
    CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
    ByteBuffer aBuffer = ByteBuffer.allocate(128);
    ByteBuffer bBuffer = ByteBuffer.allocate(128);
    for (int i = 0; i < NUM_TESTS; i++) {
      String aString = (String) RandomUtil.generatePrimitive(Types.StringType.get(), random);
      String bString = (String) RandomUtil.generatePrimitive(Types.StringType.get(), random);
      int stringCompare = Integer.signum(aString.compareTo(bString));
      byte[] aBytes = ZOrderByteUtils.stringToOrderedBytes(aString, 128, aBuffer, encoder).array();
      byte[] bBytes = ZOrderByteUtils.stringToOrderedBytes(bString, 128, bBuffer, encoder).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of strings should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aString,
              bString,
              stringCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          stringCompare,
          byteCompare);
    }
  }

  @Test
  public void testByteTruncateOrFill() {
    ByteBuffer aBuffer = ByteBuffer.allocate(128);
    ByteBuffer bBuffer = ByteBuffer.allocate(128);
    for (int i = 0; i < NUM_TESTS; i++) {
      byte[] aBytesRaw = (byte[]) RandomUtil.generatePrimitive(Types.BinaryType.get(), random);
      byte[] bBytesRaw = (byte[]) RandomUtil.generatePrimitive(Types.BinaryType.get(), random);
      int stringCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytesRaw, bBytesRaw));
      byte[] aBytes = ZOrderByteUtils.byteTruncateOrFill(aBytesRaw, 128, aBuffer).array();
      byte[] bBytes = ZOrderByteUtils.byteTruncateOrFill(bBytesRaw, 128, bBuffer).array();
      int byteCompare =
          Integer.signum(UnsignedBytes.lexicographicalComparator().compare(aBytes, bBytes));

      Assert.assertEquals(
          String.format(
              "Ordering of strings should match ordering of bytes, %s ~ %s -> %s != %s ~ %s -> %s ",
              aBytesRaw,
              bBytesRaw,
              stringCompare,
              Arrays.toString(aBytes),
              Arrays.toString(bBytes),
              byteCompare),
          stringCompare,
          byteCompare);
    }
  }
}
