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

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import org.apache.iceberg.expressions.Literal;

public class BinaryUtil {
  // not meant to be instantiated
  private BinaryUtil() {
  }

  /**
   * Truncates the input byte buffer to the given length
   */
  public static ByteBuffer truncateBinary(ByteBuffer input, int length) {
    Preconditions.checkArgument(length > 0 && length < input.remaining(),
        "Truncate length should be positive and lower than the number of remaining elements");
    byte[] array = new byte[length];
    input.duplicate().get(array);
    return ByteBuffer.wrap(array);
  }

  /**
   * Returns a byte buffer whose length is lesser than or equal to truncateLength and is lower than the given input
   */
  public static Literal<ByteBuffer> truncateBinaryMin(Literal<ByteBuffer> input, int length) {
    ByteBuffer inputBuffer = input.value();
    if (length >= inputBuffer.remaining()) {
      return input;
    }
    return Literal.of(truncateBinary(inputBuffer, length));
  }

  /**
   * Returns a byte buffer whose length is lesser than or equal to truncateLength and is greater than the given input
   */
  public static Literal<ByteBuffer> truncateBinaryMax(Literal<ByteBuffer> input, int length) {
    ByteBuffer inputBuffer = input.value();
    if (length >= inputBuffer.remaining()) {
      return input;
    }

    // Truncate the input to the specified truncate length.
    ByteBuffer truncatedInput = truncateBinary(inputBuffer, length);

    // Try incrementing the bytes from the end. If all bytes overflow after incrementing, then return null
    for (int i = length - 1; i >= 0; --i) {
      byte element = truncatedInput.get(i);
      element = (byte) (element + 1);
      if (element != 0) { // No overflow
        truncatedInput.put(i, element);
        // Return a byte buffer whose position is zero and limit is i + 1
        truncatedInput.position(0);
        truncatedInput.limit(i + 1);
        return Literal.of(truncatedInput);
      }
    }
    return null; // Cannot find a valid upper bound
  }
}
