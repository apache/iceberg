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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

class TestVectorizedDeltaByteArrayValuesReader extends VectorizedParquetDecoderTestBase {

  /** Builds a single-value DELTA_BINARY_PACKED page encoding only the given firstValue. */
  private static byte[] singleValueDeltaBinaryPacked(long firstValue) {
    return page(
        out -> {
          BytesUtils.writeUnsignedVarInt(128, out); // blockSize
          BytesUtils.writeUnsignedVarInt(4, out); // miniBlocksPerBlock
          BytesUtils.writeUnsignedVarInt(1, out); // totalValueCount
          BytesUtils.writeZigZagVarLong(firstValue, out);
        });
  }

  /** Builds a single-value DELTA_LENGTH_BYTE_ARRAY page encoding the given suffix. */
  private static byte[] singleValueDeltaLengthByteArray(byte[] suffix) {
    return concat(singleValueDeltaBinaryPacked(suffix.length), suffix);
  }

  @Test
  void rejectsPrefixLengthLargerThanPreviousValue() throws IOException {
    // Row 0: prefixLength = 5, but previous is Binary.EMPTY (length 0) => first guard fails.
    byte[] page =
        concat(
            singleValueDeltaBinaryPacked(5L), // prefixLengths[0] = 5
            singleValueDeltaLengthByteArray(new byte[] {'x', 'y'})); // suffix length = 2

    VectorizedDeltaByteArrayValuesReader reader = new VectorizedDeltaByteArrayValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(() -> reader.readBinary(7))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid prefixLength 5 for previous value of length 0");
  }

  @Test
  void rejectsTotalLengthShorterThanPrefix() throws IOException {
    // Row 0: prefixLength = 0 (passes first guard since 0 <= 0), then call readBinary(-1).
    // The -1 fails the len >= prefixLength guard (-1 < 0) BEFORE suffixReader is touched.
    byte[] page =
        concat(
            singleValueDeltaBinaryPacked(0L),
            singleValueDeltaLengthByteArray(new byte[] {'a', 'b'}));

    VectorizedDeltaByteArrayValuesReader reader = new VectorizedDeltaByteArrayValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(() -> reader.readBinary(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Total length -1 is less than prefixLength 0");
  }

  @Test
  void decodesSingleValueRoundTrip() throws IOException {
    byte[] page =
        concat(
            singleValueDeltaBinaryPacked(0L), // prefixLength = 0
            singleValueDeltaLengthByteArray(new byte[] {'h', 'i'}));

    VectorizedDeltaByteArrayValuesReader reader = new VectorizedDeltaByteArrayValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    Binary value = reader.readBinary(2);
    assertThat(value.getBytes()).containsExactly('h', 'i');
  }
}
