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
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class UUIDUtil {
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final byte[] HEX_DIGITS = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  private UUIDUtil() {}

  public static UUID convert(byte[] buf) {
    Preconditions.checkArgument(buf.length == 16, "UUID require 16 bytes");
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.order(ByteOrder.BIG_ENDIAN);
    return convert(bb);
  }

  public static UUID convert(byte[] buf, int offset) {
    Preconditions.checkArgument(
        offset >= 0 && offset < buf.length,
        "Offset overflow, offset=%s, length=%s",
        offset,
        buf.length);
    Preconditions.checkArgument(
        offset + 16 <= buf.length,
        "UUID require 16 bytes, offset=%s, length=%s",
        offset,
        buf.length);

    ByteBuffer bb = ByteBuffer.wrap(buf, offset, 16);
    bb.order(ByteOrder.BIG_ENDIAN);
    return convert(bb);
  }

  public static UUID convert(ByteBuffer buf) {
    long mostSigBits = buf.getLong();
    long leastSigBits = buf.getLong();

    return new UUID(mostSigBits, leastSigBits);
  }

  public static byte[] convert(UUID value) {
    return convertToByteBuffer(value).array();
  }

  public static ByteBuffer convertToByteBuffer(UUID value) {
    return convertToByteBuffer(value, null);
  }

  public static ByteBuffer convertToByteBuffer(UUID value, ByteBuffer reuse) {
    ByteBuffer buffer;
    if (reuse != null) {
      buffer = reuse;
    } else {
      buffer = ByteBuffer.allocate(16);
    }

    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(0, value.getMostSignificantBits());
    buffer.putLong(8, value.getLeastSignificantBits());
    return buffer;
  }

  /**
   * Parses the ASCII bytes of a canonical UUID string (36 bytes, layout {@code 8-4-4-4-12})
   * directly into the 16-byte big-endian representation, without allocating an intermediate {@link
   * String} or {@link UUID}.
   *
   * <p>This is intended for hot write paths where the UUID is already available as the ASCII bytes
   * of its string form (for example {@code UTF8String#getBytes()}). The output is byte-for-byte
   * identical to {@code convertToByteBuffer(UUID.fromString(s), reuse)}.
   *
   * @param uuidStringBytes ASCII bytes of a canonical UUID string (length 36)
   * @param reuse a 16-byte buffer to reuse, or null to allocate a new one
   * @return a big-endian buffer holding the 16-byte UUID representation
   */
  public static ByteBuffer convertToByteBuffer(byte[] uuidStringBytes, ByteBuffer reuse) {
    Preconditions.checkArgument(
        uuidStringBytes.length == 36,
        "Invalid UUID string: expected 36 ASCII bytes, got %s",
        uuidStringBytes.length);
    checkDash(uuidStringBytes, 8);
    checkDash(uuidStringBytes, 13);
    checkDash(uuidStringBytes, 18);
    checkDash(uuidStringBytes, 23);

    long mostSigBits = hexToLong(uuidStringBytes, 0, 8);
    mostSigBits <<= 16;
    mostSigBits |= hexToLong(uuidStringBytes, 9, 13);
    mostSigBits <<= 16;
    mostSigBits |= hexToLong(uuidStringBytes, 14, 18);

    long leastSigBits = hexToLong(uuidStringBytes, 19, 23);
    leastSigBits <<= 48;
    leastSigBits |= hexToLong(uuidStringBytes, 24, 36);

    ByteBuffer buffer = reuse != null ? reuse : ByteBuffer.allocate(16);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(0, mostSigBits);
    buffer.putLong(8, leastSigBits);
    return buffer;
  }

  /**
   * Renders the 16-byte big-endian UUID representation as the ASCII bytes of its canonical string
   * form (36 bytes, layout {@code 8-4-4-4-12}), without allocating an intermediate {@link UUID} or
   * {@link String}.
   *
   * <p>This is intended for hot read paths: the result can be wrapped with {@code
   * UTF8String#fromBytes} without an intermediate {@link String}. The output is byte-for-byte
   * identical to {@code convert(uuidBytes).toString().getBytes(US_ASCII)}. Two longs are read
   * relatively from the buffer's current position, mirroring {@link #convert(ByteBuffer)}.
   *
   * @param uuidBytes a buffer positioned at the 16 UUID bytes (big-endian)
   * @param reuse a 36-byte array to reuse, or null to allocate a new one
   * @return a 36-byte array holding the ASCII bytes of the canonical UUID string
   */
  public static byte[] convertToStringBytes(ByteBuffer uuidBytes, byte[] reuse) {
    Preconditions.checkArgument(
        reuse == null || reuse.length == 36,
        "Invalid reuse buffer: expected 36 bytes, got %s",
        reuse == null ? 0 : reuse.length);
    long mostSigBits = uuidBytes.getLong();
    long leastSigBits = uuidBytes.getLong();

    byte[] out = reuse != null ? reuse : new byte[36];
    formatHex(out, 0, mostSigBits >>> 32, 8);
    out[8] = '-';
    formatHex(out, 9, mostSigBits >>> 16, 4);
    out[13] = '-';
    formatHex(out, 14, mostSigBits, 4);
    out[18] = '-';
    formatHex(out, 19, leastSigBits >>> 48, 4);
    out[23] = '-';
    formatHex(out, 24, leastSigBits, 12);
    return out;
  }

  private static void checkDash(byte[] bytes, int pos) {
    Preconditions.checkArgument(
        bytes[pos] == '-', "Invalid UUID string: expected '-' at position %s", pos);
  }

  private static long hexToLong(byte[] bytes, int start, int end) {
    long result = 0;
    for (int i = start; i < end; i += 1) {
      int digit = Character.digit((char) (bytes[i] & 0xFF), 16);
      Preconditions.checkArgument(
          digit >= 0, "Invalid UUID string: not a hex digit at position %s", i);
      result = (result << 4) | digit;
    }

    return result;
  }

  private static void formatHex(byte[] out, int offset, long value, int digits) {
    for (int i = digits - 1; i >= 0; i -= 1) {
      out[offset + i] = HEX_DIGITS[(int) (value & 0xF)];
      value >>>= 4;
    }
  }

  /**
   * Generate a RFC 9562 UUIDv7.
   *
   * <p>Layout: - 48-bit Unix epoch milliseconds - 4-bit version (0b0111) - 12-bit random (rand_a) -
   * 2-bit variant (RFC 4122, 0b10) - 62-bit random (rand_b)
   */
  public static UUID generateUuidV7() {
    long epochMs = System.currentTimeMillis();
    Preconditions.checkState(
        (epochMs >>> 48) == 0, "Invalid timestamp: does not fit within 48 bits: %s", epochMs);

    // Draw 10 random bytes once: 2 bytes for rand_a (12 bits) and 8 bytes for rand_b (62 bits)
    byte[] randomBytes = new byte[10];
    SECURE_RANDOM.nextBytes(randomBytes);
    ByteBuffer rb = ByteBuffer.wrap(randomBytes).order(ByteOrder.BIG_ENDIAN);
    long randMSB = ((long) rb.getShort()) & 0x0FFFL; // 12 bits
    long randLSB = rb.getLong() & 0x3FFFFFFFFFFFFFFFL; // 62 bits

    long msb = (epochMs << 16); // place timestamp in the top 48 bits
    msb |= 0x7000L; // version 7 (UUID bits 48..51)
    msb |= randMSB; // low 12 bits of MSB

    long lsb = 0x8000000000000000L; // RFC 4122 variant '10'
    lsb |= randLSB;

    return new UUID(msb, lsb);
  }
}
