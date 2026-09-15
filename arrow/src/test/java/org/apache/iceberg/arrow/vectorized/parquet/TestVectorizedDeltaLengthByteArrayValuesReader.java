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

class TestVectorizedDeltaLengthByteArrayValuesReader extends VectorizedParquetDecoderTestBase {

  /** Builds a minimal valid DELTA_LENGTH_BYTE_ARRAY page encoding a single binary value. */
  private static byte[] singleValuePage(byte[] suffix) {
    byte[] lengthHeader =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(128, out); // blockSize
              BytesUtils.writeUnsignedVarInt(4, out); // miniBlocksPerBlock
              BytesUtils.writeUnsignedVarInt(1, out); // totalValueCount
              BytesUtils.writeZigZagVarLong(suffix.length, out); // firstValue = length
            });
    return concat(lengthHeader, suffix);
  }

  @Test
  void rejectsNegativeLength() throws IOException {
    byte[] page = singleValuePage(new byte[] {'a', 'b', 'c'});

    VectorizedDeltaLengthByteArrayValuesReader reader =
        new VectorizedDeltaLengthByteArrayValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(() -> reader.readBinary(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Length must be >= 0");
  }

  @Test
  void rejectsLengthExceedingAvailable() throws IOException {
    // Page carries only 3 suffix bytes; an attacker-shaped len asks for 1 MB.
    byte[] page = singleValuePage(new byte[] {'a', 'b', 'c'});

    VectorizedDeltaLengthByteArrayValuesReader reader =
        new VectorizedDeltaLengthByteArrayValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(() -> reader.readBinary(1024 * 1024))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds 3 bytes available");
  }

  @Test
  void surfacesInnerLengthReaderGuardOnMalformedLengthHeader() {
    // Inner DELTA_BINARY_PACKED has blockSizeInValues = 0, which must be rejected by the inner
    // reader's guard — proves the composition propagates hardening, not just the outer reader's.
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(0, out); // blockSize = 0
              BytesUtils.writeUnsignedVarInt(4, out);
              BytesUtils.writeUnsignedVarInt(1, out);
              BytesUtils.writeZigZagVarLong(0L, out);
            });

    VectorizedDeltaLengthByteArrayValuesReader reader =
        new VectorizedDeltaLengthByteArrayValuesReader();
    assertThatThrownBy(
            () -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blockSizeInValues must be in");
  }

  @Test
  void decodesSingleValueRoundTrip() throws IOException {
    byte[] suffix = new byte[] {'a', 'b', 'c'};
    byte[] page = singleValuePage(suffix);

    VectorizedDeltaLengthByteArrayValuesReader reader =
        new VectorizedDeltaLengthByteArrayValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThat(reader.lengthForCurrentRow()).isEqualTo(3);
    Binary value = reader.readBinary(3);
    assertThat(value.getBytes()).containsExactly('a', 'b', 'c');
  }
}
