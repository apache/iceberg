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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;

/**
 * Contains the logic for hashing various types for use with the {@code bucket} partition
 * transformations
 */
public class BucketHashUtil {

  private static final HashFunction MURMUR3 = Hashing.murmur3_32_fixed();

  private BucketHashUtil() {}

  public static int forInteger(Integer value) {
    return MURMUR3.hashLong(value.longValue()).asInt();
  }

  public static int forLong(Long value) {
    return MURMUR3.hashLong(value).asInt();
  }

  public static int forFloat(Float value) {
    return MURMUR3.hashLong(Double.doubleToLongBits((double) value)).asInt();
  }

  public static int forDouble(Double value) {
    return MURMUR3.hashLong(Double.doubleToLongBits(value)).asInt();
  }

  public static int forCharSequence(CharSequence value) {
    return MURMUR3.hashString(value, StandardCharsets.UTF_8).asInt();
  }

  public static int forByteBuffer(ByteBuffer value) {
    if (value.hasArray()) {
      return MURMUR3
          .hashBytes(
              value.array(),
              value.arrayOffset() + value.position(),
              value.arrayOffset() + value.remaining())
          .asInt();
    } else {
      int position = value.position();
      byte[] copy = new byte[value.remaining()];
      try {
        value.get(copy);
      } finally {
        // make sure the buffer position is unchanged
        value.position(position);
      }
      return MURMUR3.hashBytes(copy).asInt();
    }
  }

  public static int forUUID(UUID value) {
    return MURMUR3
        .newHasher(16)
        .putLong(Long.reverseBytes(value.getMostSignificantBits()))
        .putLong(Long.reverseBytes(value.getLeastSignificantBits()))
        .hash()
        .asInt();
  }

  public static int forDecimal(BigDecimal value) {
    return MURMUR3.hashBytes(value.unscaledValue().toByteArray()).asInt();
  }
}
