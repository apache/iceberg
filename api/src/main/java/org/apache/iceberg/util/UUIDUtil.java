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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class UUIDUtil {
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

    public static UUID uuid5(String data) throws NoSuchAlgorithmException {
        final MessageDigest md = MessageDigest.getInstance("SHA1");
        // digest the metadata location
        final int sz = data.length();
        for (int i = 0; i < sz; ++i) {
            char c = data.charAt(i);
            md.update((byte) (c & 0xff));
            md.update((byte) (c >> 8));
        }
        // make a UUID v5 from the metadata location SHA1 hash
        byte[] sha1Bytes = md.digest();
        sha1Bytes[6] &= 0x0f; /* clear version        */
        sha1Bytes[6] |= 0x50; /* set to version 5     */
        sha1Bytes[8] &= 0x3f; /* clear variant        */
        sha1Bytes[8] |= (byte) 0x80; /* set to IETF variant  */

        // SHA generates 160 bytes; truncate to 128
        long msb = 0;
        // assert data.length == 16 || data.length == 20;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (sha1Bytes[i] & 0xff);
        }
        long lsb = 0;
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (sha1Bytes[i] & 0xff);
        }
        return new UUID(msb, lsb);
    }
}
