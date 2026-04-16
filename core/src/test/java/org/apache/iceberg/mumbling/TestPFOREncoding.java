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

/**
 * Tests for PFOR encoding based on the examples in Appendix A of the Mumbling bitmap spec.
 *
 * <p>Note: examples 3 and 5 from the spec appear to contain typos in the encoded byte sequences.
 * The expected byte arrays used here are derived from the spec algorithm applied to the stated
 * decoded values, and the tests verify both the byte-level encoding and the roundtrip decode.
 *
 * <p>Example 3 spec header is {@code 80 00 00} but {@code e} must be 2 (two exceptions), so the
 * correct header is {@code 80 02 00}.
 *
 * <p>Example 5 spec bytes are {@code 42 01 06 19 01 A0}, but with {@code b1=2} the cost-minimizing
 * {@code b2 = maxWidth - b1 = 5 - 2 = 3} (not 4), producing header {@code 32 01 06}.
 */
public class TestPFOREncoding {

  // ---------------------------------------------------------------------------
  // Spec example 1: 256 values, all = 0
  // Encoding: 0 bits per value, m = 0, no exceptions
  // Expected: 00 00 00
  // ---------------------------------------------------------------------------
  @Test
  public void testExample1AllZeros() {
    int[] values = new int[256];
    Arrays.fill(values, 0);

    ByteBuffer encoded = PFOREncoding.encode(values);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(bytes(0x00, 0x00, 0x00));

    int[] decoded = PFOREncoding.decode(encoded, 256);
    assertThat(decoded).isEqualTo(values);
  }

  // ---------------------------------------------------------------------------
  // Spec example 2: 51 values, all = 5
  // Encoding: 0 bits per value, m = 5, no exceptions
  // Expected: 00 00 05
  // ---------------------------------------------------------------------------
  @Test
  public void testExample2AllFives() {
    int[] values = new int[51];
    Arrays.fill(values, 5);

    ByteBuffer encoded = PFOREncoding.encode(values);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(bytes(0x00, 0x00, 0x05));

    int[] decoded = PFOREncoding.decode(encoded, 51);
    assertThat(decoded).isEqualTo(values);
  }

  // ---------------------------------------------------------------------------
  // Spec example 3: [0, 0, 0, 0, 0xFF, 0, 0, 0xFE]
  // Encoding: b1=0, b2=8, m=0, 2 exceptions at positions 4 and 7
  //
  // Note: the spec shows header bytes "80 00 00" but e=0 is incorrect; there are
  // 2 exceptions so the correct encoding is "80 02 00 04 07 FF FE".
  // ---------------------------------------------------------------------------
  @Test
  public void testExample3SparseExceptions() {
    int[] values = {0, 0, 0, 0, 0xFF, 0, 0, 0xFE};

    // b1=0, b2=8, e=2, m=0  →  header byte0=(8<<4)|0=0x80, byte1=0x02, byte2=0x00
    // primary: empty (b1=0)
    // offsets: 0x04, 0x07
    // exception values: 0xFF, 0xFE (8 bits each)
    byte[] expected =
        bytes(0x80, 0x02, 0x00, /* offsets */ 0x04, 0x07, /* exceptions */ 0xFF, 0xFE);

    ByteBuffer encoded = PFOREncoding.encode(values);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(expected);

    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);
  }

  // ---------------------------------------------------------------------------
  // Spec example 4: [6, 7, 8]
  // Encoding: b1=2, b2=0, m=6, no exceptions
  // Expected: 02 00 06 18
  // ---------------------------------------------------------------------------
  @Test
  public void testExample4TwoBitsNoExceptions() {
    int[] values = {6, 7, 8};

    // b1=2, b2=0, e=0, m=6  →  header byte0=(0<<4)|2=0x02, byte1=0x00, byte2=0x06
    // normalized: [0, 1, 2]
    // primary: 00 01 10 (padded) = 0b00011000 = 0x18
    byte[] expected = bytes(0x02, 0x00, 0x06, 0x18);

    ByteBuffer encoded = PFOREncoding.encode(values);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(expected);

    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);
  }

  // ---------------------------------------------------------------------------
  // Spec example 5: [6, 34, 8, 7]
  // Encoding: b1=2, b2=3, m=6, 1 exception (value 34 → normalized 28)
  //
  // Note: the spec shows "42 01 06 19 01 A0" but that byte sequence is inconsistent
  // with the stated decoded values. With b1=2 and maxWidth=5, the cost-minimizing
  // b2 = maxWidth - b1 = 3 (not 4). The correct encoding is:
  //   header:    32 01 06  (b1=2, b2=3, e=1, m=6)
  //   primary:   09        ([0,0,2,1] low 2 bits packed MSB-first)
  //   offset:    01        (exception at position 1)
  //   exception: E0        (7 = 28>>2, packed as 3 bits: 111_00000)
  // ---------------------------------------------------------------------------
  @Test
  public void testExample5TwoBitsOneException() {
    int[] values = {6, 34, 8, 7};

    // b1=2, b2=3, e=1, m=6  →  header byte0=(3<<4)|2=0x32, byte1=0x01, byte2=0x06
    // normalized: [0, 28, 2, 1]
    // primary:    [00, 00, 10, 01] packed MSB-first = 0b00001001 = 0x09
    // exception offset: 0x01  (position 1)
    // exception value:  28>>2 = 7 = 0b111, 3 bits MSB-first = 0b111_00000 = 0xE0
    byte[] expected = bytes(0x32, 0x01, 0x06, 0x09, 0x01, 0xE0);

    ByteBuffer encoded = PFOREncoding.encode(values);
    assertThat(ByteBuffers.toByteArray(encoded)).isEqualTo(expected);

    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);
  }

  // ---------------------------------------------------------------------------
  // Helper to convert varargs ints to a byte array (treating each int as a byte)
  // ---------------------------------------------------------------------------
  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }
}
