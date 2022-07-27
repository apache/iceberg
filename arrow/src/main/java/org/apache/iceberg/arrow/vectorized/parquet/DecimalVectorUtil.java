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
package org.apache.iceberg.arrow.vectorized.parquet;

import java.util.Arrays;
import org.apache.arrow.vector.DecimalVector;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public class DecimalVectorUtil {

  private DecimalVectorUtil() {}

  public static void setBigEndian(DecimalVector vector, int idx, byte[] value) {
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(value, DecimalVector.TYPE_WIDTH);
    vector.setBigEndian(idx, paddedBytes);
  }

  /**
   * Parquet stores decimal values in big-endian byte order, and Arrow stores them in native byte
   * order. When setting the value in Arrow, we call setBigEndian(), and the byte order is reversed
   * if needed. Also, the byte array is padded to fill 16 bytes in length by calling
   * Unsafe.setMemory(). The padding operation can be slow, so by using this utility method, we can
   * pad before calling setBigEndian() and avoid the call to Unsafe.setMemory().
   *
   * @param bigEndianBytes The big endian bytes
   * @param newLength The length of the byte array to return
   * @return The new byte array
   */
  @VisibleForTesting
  static byte[] padBigEndianBytes(byte[] bigEndianBytes, int newLength) {
    if (bigEndianBytes.length == newLength) {
      return bigEndianBytes;
    } else if (bigEndianBytes.length < newLength) {
      byte[] result = new byte[newLength];
      if (bigEndianBytes.length == 0) {
        return result;
      }

      int start = newLength - bigEndianBytes.length;
      if (bigEndianBytes[0] < 0) {
        Arrays.fill(result, 0, start, (byte) 0xFF);
      }
      System.arraycopy(bigEndianBytes, 0, result, start, bigEndianBytes.length);

      return result;
    }
    throw new IllegalArgumentException(
        String.format(
            "Buffer size of %d is larger than requested size of %d",
            bigEndianBytes.length, newLength));
  }
}
