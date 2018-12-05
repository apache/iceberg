package com.netflix.iceberg.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteBuffers {
  private ByteBuffers() {}

  public static byte[] copy(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      byte[] array = buffer.array();
      if (buffer.arrayOffset() == 0 && buffer.position() == 0 && array.length == buffer.remaining()) {
        return array;
      } else {
        int start = buffer.arrayOffset() + buffer.position();
        int end = start + buffer.remaining();
        return Arrays.copyOfRange(array, start, end);
      }
    } else {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    }
  }
}
