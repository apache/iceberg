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
package org.apache.iceberg.puffin;

import static org.apache.iceberg.puffin.PuffinFormat.readIntegerLittleEndian;
import static org.apache.iceberg.puffin.PuffinFormat.writeIntegerLittleEndian;
import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.jupiter.api.Test;

public class TestPuffinFormat {
  @Test
  public void testWriteIntegerLittleEndian() throws Exception {
    testWriteIntegerLittleEndian(0, bytes(0, 0, 0, 0));
    testWriteIntegerLittleEndian(42, bytes(42, 0, 0, 0));
    testWriteIntegerLittleEndian(Integer.MAX_VALUE - 5, bytes(0xFA, 0xFF, 0xFF, 0x7F));
    testWriteIntegerLittleEndian(-7, bytes(0xF9, 0xFF, 0xFF, 0xFF));
  }

  private void testWriteIntegerLittleEndian(int value, byte[] expected) throws Exception {
    // Sanity check: validate the expectation
    ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(value);
    buffer.flip();
    byte[] written = new byte[4];
    buffer.get(written);
    Preconditions.checkState(Arrays.equals(written, expected), "Invalid expected value");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    writeIntegerLittleEndian(outputStream, value);
    assertThat(outputStream.toByteArray()).isEqualTo(expected);
  }

  @Test
  public void testReadIntegerLittleEndian() {
    testReadIntegerLittleEndian(bytes(0, 0, 0, 0), 0, 0);
    testReadIntegerLittleEndian(bytes(42, 0, 0, 0), 0, 42);
    testReadIntegerLittleEndian(bytes(13, 42, 0, 0, 0, 14), 1, 42);
    testReadIntegerLittleEndian(bytes(13, 0xFa, 0xFF, 0xFF, 0x7F, 14), 1, Integer.MAX_VALUE - 5);
    testReadIntegerLittleEndian(bytes(13, 0xF9, 0xFF, 0xFF, 0xFF, 14), 1, -7);
  }

  private void testReadIntegerLittleEndian(byte[] input, int offset, int expected) {
    // Sanity check: validate the expectation
    Preconditions.checkArgument(
        expected
            == ByteBuffer.wrap(input.clone(), offset, input.length - offset)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt(),
        "Invalid expected value");
    // actual test
    assertThat(readIntegerLittleEndian(input, offset)).isEqualTo(expected);
  }

  private byte[] bytes(int... unsignedBytes) {
    byte[] bytes = new byte[unsignedBytes.length];
    for (int i = 0; i < unsignedBytes.length; i++) {
      int value = unsignedBytes[i];
      checkArgument(0 <= value && value <= 0xFF, "Invalid value: %s", value);
      bytes[i] = (byte) value;
    }
    return bytes;
  }
}
