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
