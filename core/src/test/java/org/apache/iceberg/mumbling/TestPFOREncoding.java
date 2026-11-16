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
package org.apache.iceberg.mumbling;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.api.Test;

public class TestPFOREncoding {

  // Example 1: 256 values, b1=0, m=0, all=0: `00 00 00`
  @Test
  public void testExample1AllZeros() {
    int[] values = new int[256];
    Arrays.fill(values, 0);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(bytes(0x00, 0x00, 0x00));

    int[] decoded = PFOREncoding.decode(encoded, 256);
    assertThat(decoded).isEqualTo(values);
  }

  // Example 2: 51 values, b1=0, m=5, all=5: `00 00 05`
  @Test
  public void testExample2AllFives() {
    int[] values = new int[51];
    Arrays.fill(values, 5);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(bytes(0x00, 0x00, 0x05));

    int[] decoded = PFOREncoding.decode(encoded, 51);
    assertThat(decoded).isEqualTo(values);
  }

  // Example 3: only exception values, b1=0, b2=8: `80 02 00 04 07 FF FE`
  @Test
  public void testExample3SparseExceptions() {
    int[] values = {0, 0, 0, 0, 0xFF, 0, 0, 0xFE};

    byte[] expected =
        bytes(0x80, 0x02, 0x00, /* offsets */ 0x04, 0x07, /* exceptions */ 0xFF, 0xFE);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(expected);

    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);
  }

  // Example 4: [6, 7, 8], no exceptions, b1=2, m=6: `02 00 06 18`
  @Test
  public void testExample4TwoBitsNoExceptions() {
    int[] values = {6, 7, 8};

    byte[] expected = bytes(0x02, 0x00, 0x06, 0x18);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(expected);

    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);
  }

  // Example 5: [6, 34, 8, 7], b1=2, b2=3, m=6, 1 exception: `32 01 06 09 01 E0`
  @Test
  public void testExample5() {
    int[] values = {6, 34, 8, 7};

    // impl prefers larger widths when the storage is the same size
    byte[] expected = bytes(0x05, 0x00, 0x06, 0x07, 0x04, 0x10);
    byte[] fromSpec = bytes(0x32, 0x01, 0x06, 0x09, 0x01, 0xE0);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(expected);

    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);

    int[] decodedFromSpec = PFOREncoding.decode(ByteBuffer.wrap(fromSpec), values.length);
    assertThat(decodedFromSpec).isEqualTo(values);
  }

  // count is the number of values to encode starting at valueOffset, not an end offset
  @Test
  public void testEncodeWithValueOffset() {
    int[] values = new int[400];
    for (int i = 0; i < values.length; i += 1) {
      values[i] = i % 251;
    }

    int valueOffset = 5;
    // span a chunk boundary so a count misread as an end offset would drop values
    int count = 260;

    ByteBuffer out = ByteBuffer.allocate(PFOREncoding.estimateEncodedSize(count));
    int bytesWritten = PFOREncoding.encode(values, valueOffset, out, 0, count);

    int[] decoded = new int[count];
    int bytesRead = PFOREncoding.decode(out, 0, decoded, 0, count);

    assertThat(bytesRead).isEqualTo(bytesWritten);
    assertThat(decoded).isEqualTo(Arrays.copyOfRange(values, valueOffset, valueOffset + count));
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }
}
