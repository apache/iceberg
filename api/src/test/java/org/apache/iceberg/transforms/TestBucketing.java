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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BucketUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBucketing {
  private static final HashFunction MURMUR3 = Hashing.murmur3_32_fixed();
  private static Constructor<UUID> uuidBytesConstructor;

  @BeforeAll
  public static void getUUIDConstructor() {
    try {
      uuidBytesConstructor = UUID.class.getDeclaredConstructor(byte[].class);
      uuidBytesConstructor.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private Random testRandom = null;

  @BeforeEach
  public void initRandom() {
    // reinitialize random for each test to avoid dependence on run order
    this.testRandom = new Random(314358);
  }

  @Test
  public void testSpecValues() {
    assertThat(BucketUtil.hash(1))
        .as("Spec example: hash(true) = 1392991556")
        .isEqualTo(1392991556);
    assertThat(BucketUtil.hash(34)).as("Spec example: hash(34) = 2017239379").isEqualTo(2017239379);
    assertThat(BucketUtil.hash(34L))
        .as("Spec example: hash(34L) = 2017239379")
        .isEqualTo(2017239379);

    assertThat(BucketUtil.hash(1.0F))
        .as("Spec example: hash(17.11F) = -142385009")
        .isEqualTo(-142385009);
    assertThat(BucketUtil.hash(1.0D))
        .as("Spec example: hash(17.11D) = -142385009")
        .isEqualTo(-142385009);
    assertThat(BucketUtil.hash(0.0F))
        .as("Spec example: hash(0.0F) = 1669671676")
        .isEqualTo(1669671676);
    assertThat(BucketUtil.hash(-0.0F))
        .as("Spec example: hash(-0.0F) = 1669671676")
        .isEqualTo(1669671676);
    assertThat(BucketUtil.hash(0.0))
        .as("Spec example: hash(0.0) = 1669671676")
        .isEqualTo(1669671676);
    assertThat(BucketUtil.hash(-0.0))
        .as("Spec example: hash(-0.0) = 1669671676")
        .isEqualTo(1669671676);

    assertThat(BucketUtil.hash(new BigDecimal("14.20")))
        .as("Spec example: hash(decimal2(14.20)) = -500754589")
        .isEqualTo(-500754589)
        .as("Spec example: hash(decimal2(14.20)) = -500754589")
        .isEqualTo(-500754589);

    Literal<Integer> date = Literal.of("2017-11-16").to(Types.DateType.get());
    assertThat(BucketUtil.hash(date.value()))
        .as("Spec example: hash(2017-11-16) = -653330422")
        .isEqualTo(-653330422);

    Literal<Long> timeValue = Literal.of("22:31:08").to(Types.TimeType.get());
    assertThat(BucketUtil.hash(timeValue.value()))
        .as("Spec example: hash(22:31:08) = -662762989")
        .isEqualTo(-662762989);

    Literal<Long> timestampVal =
        Literal.of("2017-11-16T22:31:08").to(Types.TimestampType.withoutZone());
    assertThat(BucketUtil.hash(timestampVal.value()))
        .as("Spec example: hash(2017-11-16T22:31:08) = -2047944441")
        .isEqualTo(-2047944441);

    Literal<Long> timestamptzVal =
        Literal.of("2017-11-16T14:31:08-08:00").to(Types.TimestampType.withZone());
    assertThat(BucketUtil.hash(timestamptzVal.value()))
        .as("Spec example: hash(2017-11-16T14:31:08-08:00) = -2047944441")
        .isEqualTo(-2047944441);

    assertThat(BucketUtil.hash("iceberg"))
        .as("Spec example: hash(\"iceberg\") = 1210000089")
        .isEqualTo(1210000089);
    assertThat(BucketUtil.hash(new Utf8("iceberg")))
        .as("Spec example: hash(\"iceberg\") = 1210000089")
        .isEqualTo(1210000089);

    Literal<UUID> uuid =
        Literal.of("f79c3e09-677c-4bbd-a479-3f349cb785e7").to(Types.UUIDType.get());
    assertThat(BucketUtil.hash(uuid.value()))
        .as("Spec example: hash(f79c3e09-677c-4bbd-a479-3f349cb785e7) = 1488055340")
        .isEqualTo(1488055340);

    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
    assertThat(BucketUtil.hash(bytes))
        .as("Spec example: hash([00 01 02 03]) = -188683207")
        .isEqualTo(-188683207);
    // another assertion for confirming that hashing a ByteBuffer again doesnt modify its value.
    assertThat(BucketUtil.hash(bytes))
        .as("Spec example: hash([00 01 02 03]) = -188683207")
        .isEqualTo(-188683207);
  }

  @Test
  public void testInteger() {
    int num = testRandom.nextInt();
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(num);

    assertThat(BucketUtil.hash(num))
        .as("Integer hash should match hash of little-endian bytes")
        .isEqualTo(hashBytes(buffer.array()));
  }

  @Test
  public void testLong() {
    long num = testRandom.nextLong();
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(num);

    assertThat(BucketUtil.hash(num))
        .as("Long hash should match hash of little-endian bytes")
        .isEqualTo(hashBytes(buffer.array()));
  }

  @Test
  public void testIntegerTypePromotion() {
    int randomInt = testRandom.nextInt();

    assertThat(BucketUtil.hash((long) randomInt))
        .as("Integer and Long bucket results should match")
        .isEqualTo(BucketUtil.hash(randomInt));
  }

  @Test
  public void testFloatTypePromotion() {
    float randomFloat = testRandom.nextFloat();

    assertThat(BucketUtil.hash((double) randomFloat))
        .as("Float and Double bucket results should match")
        .isEqualTo(BucketUtil.hash(randomFloat));
  }

  @Test
  public void testFloatNegativeZero() {
    assertThat(BucketUtil.hash(0.0f))
        .as("Positive and negative 0.0f should have the same hash")
        .isEqualTo(BucketUtil.hash(-0.0f));
  }

  @Test
  public void testDoubleNegativeZero() {
    assertThat(BucketUtil.hash(0.0))
        .as("Positive and negative 0.0 should have the same hash")
        .isEqualTo(BucketUtil.hash(-0.0));
  }

  @Test
  public void testFloatNaN() {
    double canonicalNaN = Double.longBitsToDouble(0x7ff8000000000000L);

    // All bit patterns in the range 0x7f800001 to 0x7fffffff
    // and 0xff800001 to 0xffffffff are considered NaNs.
    float[] testNaNs = {
      Float.NaN,
      Float.intBitsToFloat(0x7f800001),
      Float.intBitsToFloat(0x7f9abcde),
      Float.intBitsToFloat(0x7fedcba9),
      Float.intBitsToFloat(0x7fffffff),
      Float.intBitsToFloat(0xff800001),
      Float.intBitsToFloat(0xff9abcde),
      Float.intBitsToFloat(0xffedcba9),
      Float.intBitsToFloat(0xffffffff),
    };

    for (float value : testNaNs) {
      assertThat(Float.isNaN(value)).as("Bit pattern is expected to be NaN.").isTrue();
      assertThat(BucketUtil.hash(canonicalNaN))
          .as("All NaN representations should result in the same hash")
          .isEqualTo(BucketUtil.hash(value));
    }
  }

  @Test
  public void testDoubleNaN() {
    double canonicalNaN = Double.longBitsToDouble(0x7ff8000000000000L);

    // All bit patterns in the range 0x7ff0000000000001L to 0x7fffffffffffffffL
    // and 0xfff0000000000001L to 0xffffffffffffffffL are considered NaNs.
    double[] testNaNs = {
      Double.NaN,
      Double.longBitsToDouble(0x7ff0000000000001L),
      Double.longBitsToDouble(0x7ff123456789abcdL),
      Double.longBitsToDouble(0x7ffdcba987654321L),
      Double.longBitsToDouble(0x7fffffffffffffffL),
      Double.longBitsToDouble(0xfff0000000000001L),
      Double.longBitsToDouble(0xfff123456789abcdL),
      Double.longBitsToDouble(0xfffdcba987654321L),
      Double.longBitsToDouble(0xffffffffffffffffL),
    };

    for (double value : testNaNs) {
      assertThat(Double.isNaN(value)).as("Bit pattern is expected to be NaN.").isTrue();
      assertThat(BucketUtil.hash(canonicalNaN))
          .as("All NaN representations should result in the same hash")
          .isEqualTo(BucketUtil.hash(value));
    }
  }

  @Test
  public void testDecimal() {
    double num = testRandom.nextDouble();
    BigDecimal decimal = BigDecimal.valueOf(num);
    byte[] unscaledBytes = decimal.unscaledValue().toByteArray();

    assertThat(BucketUtil.hash(decimal))
        .as("Decimal hash should match hash of backing bytes")
        .isEqualTo(hashBytes(unscaledBytes));
  }

  @Test
  public void testString() {
    String string = "string to test murmur3 hash";
    byte[] asBytes = string.getBytes(StandardCharsets.UTF_8);

    assertThat(BucketUtil.hash(string))
        .as("String hash should match hash of UTF-8 bytes")
        .isEqualTo(hashBytes(asBytes));
  }

  @Test
  public void testStringWithSurrogatePair() {
    String string = "string with a surrogate pair: 💰";
    assertThat(string.codePoints().count())
        .as("string has no surrogate pairs")
        .isNotEqualTo(string.length());
    byte[] asBytes = string.getBytes(StandardCharsets.UTF_8);

    assertThat(BucketUtil.hash(string))
        .as("String hash should match hash of UTF-8 bytes")
        .isEqualTo(hashBytes(asBytes));
  }

  @Test
  public void testUtf8() {
    Utf8 utf8 = new Utf8("string to test murmur3 hash");
    byte[] asBytes = utf8.toString().getBytes(StandardCharsets.UTF_8);

    assertThat(BucketUtil.hash(utf8))
        .as("String hash should match hash of UTF-8 bytes")
        .isEqualTo(hashBytes(asBytes));
  }

  @Test
  public void testByteBufferOnHeap() {
    byte[] bytes = randomBytes(128);
    ByteBuffer buffer = ByteBuffer.wrap(bytes, 5, 100);

    assertThat(BucketUtil.hash(buffer))
        .as("HeapByteBuffer hash should match hash for correct slice")
        .isEqualTo(hashBytes(bytes, 5, 100));

    // verify that the buffer was not modified
    assertThat(buffer.position()).as("Buffer position should not change").isEqualTo(5);
    assertThat(buffer.limit()).as("Buffer limit should not change").isEqualTo(105);
  }

  @Test
  public void testByteBufferOnHeapArrayOffset() {
    byte[] bytes = randomBytes(128);
    ByteBuffer raw = ByteBuffer.wrap(bytes, 5, 100);
    ByteBuffer buffer = raw.slice();
    assertThat(buffer.arrayOffset()).as("Buffer arrayOffset should be 5").isEqualTo(5);

    assertThat(BucketUtil.hash(buffer))
        .as("HeapByteBuffer hash should match hash for correct slice")
        .isEqualTo(hashBytes(bytes, 5, 100));

    // verify that the buffer was not modified
    assertThat(buffer.position()).as("Buffer position should be 0").isEqualTo(0);
    assertThat(buffer.limit()).as("Buffer limit should not change").isEqualTo(100);
  }

  @Test
  public void testByteBufferOffHeap() {
    byte[] bytes = randomBytes(128);
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);

    // copy to the middle of the off-heap buffer
    buffer.position(5);
    buffer.limit(105);
    buffer.mark();
    buffer.put(bytes, 5, 100);
    buffer.reset();

    assertThat(BucketUtil.hash(buffer))
        .as("DirectByteBuffer hash should match hash for correct slice")
        .isEqualTo(hashBytes(bytes, 5, 100));

    // verify that the buffer was not modified
    assertThat(buffer.position()).as("Buffer position should not change").isEqualTo(5);
    assertThat(buffer.limit()).as("Buffer limit should not change").isEqualTo(105);
  }

  @Test
  public void testUUIDHash() {
    byte[] uuidBytes = randomBytes(16);
    UUID uuid = newUUID(uuidBytes);

    assertThat(BucketUtil.hash(uuid))
        .as("UUID hash should match hash of backing bytes")
        .isEqualTo(hashBytes(uuidBytes));
  }

  @Test
  public void testVerifiedIllegalNumBuckets() {
    assertThatThrownBy(() -> Bucket.get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid number of buckets: 0 (must be > 0)");
  }

  private byte[] randomBytes(int length) {
    byte[] bytes = new byte[length];
    testRandom.nextBytes(bytes);
    return bytes;
  }

  private int hashBytes(byte[] bytes) {
    return hashBytes(bytes, 0, bytes.length);
  }

  private int hashBytes(byte[] bytes, int offset, int length) {
    return MURMUR3.hashBytes(bytes, offset, length).asInt();
  }

  /**
   * This method returns a UUID for the bytes in the array without modification.
   *
   * @param bytes a 16-byte array
   * @return a UUID for the bytes
   */
  private static UUID newUUID(byte[] bytes) {
    try {
      return uuidBytesConstructor.newInstance((Object) bytes);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
