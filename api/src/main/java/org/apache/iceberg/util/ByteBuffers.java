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
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class ByteBuffers {

  public static byte[] toByteArray(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    if (buffer.hasArray()) {
      byte[] array = buffer.array();
      if (buffer.arrayOffset() == 0 && buffer.position() == 0 &&
          array.length == buffer.remaining()) {
        return array;
      } else {
        int start = buffer.arrayOffset() + buffer.position();
        int end = start + buffer.remaining();
        return Arrays.copyOfRange(array, start, end);
      }
    } else {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.asReadOnlyBuffer().get(bytes);
      return bytes;
    }
  }

  public static ByteBuffer reuse(ByteBuffer reuse, int length) {
    Preconditions.checkArgument(reuse.hasArray() && reuse.arrayOffset() == 0 && reuse.capacity() == length,
        "Cannot reuse buffer: Should be an array %s, should have an offset of 0 %s, should be of size %s was %s",
        reuse.hasArray(), reuse.arrayOffset(), length, reuse.capacity());
    reuse.position(0);
    reuse.limit(length);
    return reuse;
  }

  public static ByteBuffer copy(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    byte[] copyArray = new byte[buffer.remaining()];
    ByteBuffer readerBuffer = buffer.asReadOnlyBuffer();
    readerBuffer.get(copyArray);

    return ByteBuffer.wrap(copyArray);
  }

  private ByteBuffers() {
  }
}
