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

  /** Length of a UUID in its canonical 8-4-4-4-12 textual form. */
  private static final int UUID_TEXT_LENGTH = 36;

  private static final int[] DASH_POSITIONS = {8, 13, 18, 23};

  /**
   * Writes the UUID given in canonical textual form as 16 big-endian bytes.
   *
   * <p>The text is read directly from ASCII bytes, so this avoids the {@code String} allocation and
   * UTF-8 decode, and the {@link UUID#fromString} parse and {@code UUID} allocation, that a caller
   * holding raw bytes would otherwise pay per value. The result is identical to {@code
   * convertToByteBuffer(UUID.fromString(new String(uuidText, StandardCharsets.UTF_8)), reuse)} for
   * every canonical UUID.
   *
   * <p>Only the canonical form is accepted: 32 hexadecimal digits in 8-4-4-4-12 groups separated by
   * {@code '-'}. Digits may be upper or lower case. Unlike {@link UUID#fromString}, shortened
   * groups are rejected rather than zero-extended.
   *
   * @param uuidText ASCII bytes of a canonical UUID
   * @param offset start of the UUID text within {@code uuidText}
   * @param length length of the UUID text, which must be 36
   * @param reuse buffer to write into, or null to allocate one
   * @return a buffer of 16 bytes positioned at 0
   */
  public static ByteBuffer convertToByteBuffer(
      byte[] uuidText, int offset, int length, ByteBuffer reuse) {
    Preconditions.checkArgument(uuidText != null, "Invalid UUID text: null");
    Preconditions.checkArgument(
        length == UUID_TEXT_LENGTH, "Invalid UUID text: expected 36 characters, got %s", length);
    Preconditions.checkArgument(
        offset >= 0 && offset + length <= uuidText.length,
        "UUID text out of bounds, offset=%s, length=%s, array length=%s",
        offset,
        length,
        uuidText.length);
    for (int dash : DASH_POSITIONS) {
      Preconditions.checkArgument(
          uuidText[offset + dash] == '-', "Invalid UUID text: expected '-' at position %s", dash);
    }

    // 8-4-4 before the third dash forms the most significant bits, 4-12 after it the least
    long mostSigBits = readHex(uuidText, offset, 8);
    mostSigBits = (mostSigBits << 16) | readHex(uuidText, offset + 9, 4);
    mostSigBits = (mostSigBits << 16) | readHex(uuidText, offset + 14, 4);

    long leastSigBits = readHex(uuidText, offset + 19, 4);
    leastSigBits = (leastSigBits << 48) | readHex(uuidText, offset + 24, 12);

    ByteBuffer buffer = reuse != null ? reuse : ByteBuffer.allocate(16);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(0, mostSigBits);
    buffer.putLong(8, leastSigBits);
    return buffer;
  }

  /** See {@link #convertToByteBuffer(byte[], int, int, ByteBuffer)}. */
  public static ByteBuffer convertToByteBuffer(byte[] uuidText, ByteBuffer reuse) {
    Preconditions.checkArgument(uuidText != null, "Invalid UUID text: null");
    return convertToByteBuffer(uuidText, 0, uuidText.length, reuse);
  }

  /** Reads {@code count} hexadecimal digits as an unsigned value. */
  private static long readHex(byte[] text, int offset, int count) {
    long value = 0;
    for (int i = 0; i < count; i += 1) {
      value = (value << 4) | hexDigit(text[offset + i]);
    }
    return value;
  }

  private static int hexDigit(byte character) {
    if (character >= '0' && character <= '9') {
      return character - '0';
    } else if (character >= 'a' && character <= 'f') {
      return character - 'a' + 10;
    } else if (character >= 'A' && character <= 'F') {
      return character - 'A' + 10;
    }

    throw new IllegalArgumentException(
        String.format("Invalid UUID text: '%s' is not a hexadecimal digit", (char) character));
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
