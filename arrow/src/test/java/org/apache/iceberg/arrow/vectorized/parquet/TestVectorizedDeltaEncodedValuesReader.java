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
import org.junit.jupiter.api.Test;

class TestVectorizedDeltaEncodedValuesReader extends VectorizedParquetDecoderTestBase {

  @Test
  void rejectsZeroBlockSize() {
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(0, out); // blockSizeInValues = 0
              BytesUtils.writeUnsignedVarInt(4, out);
              BytesUtils.writeUnsignedVarInt(1, out);
              BytesUtils.writeZigZagVarLong(0L, out);
            });

    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    assertThatThrownBy(
            () -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blockSizeInValues must be in");
  }

  @Test
  void rejectsZeroMiniBlocksPerBlock() {
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(128, out);
              BytesUtils.writeUnsignedVarInt(0, out); // miniBlocksPerBlock = 0
              BytesUtils.writeUnsignedVarInt(1, out);
              BytesUtils.writeZigZagVarLong(0L, out);
            });

    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    assertThatThrownBy(
            () -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("miniBlocksPerBlock");
  }

  @Test
  void rejectsMiniBlocksPerBlockExceedingBlockSize() {
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(128, out);
              BytesUtils.writeUnsignedVarInt(200, out); // > blockSize
              BytesUtils.writeUnsignedVarInt(1, out);
              BytesUtils.writeZigZagVarLong(0L, out);
            });

    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    assertThatThrownBy(
            () -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("miniBlocksPerBlock");
  }

  @Test
  void rejectsTotalValueCountAboveCallerValueCount() {
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(128, out);
              BytesUtils.writeUnsignedVarInt(4, out);
              BytesUtils.writeUnsignedVarInt(100, out); // totalValueCount = 100
              BytesUtils.writeZigZagVarLong(0L, out);
            });

    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    assertThatThrownBy(
            () ->
                reader.initFromPage(
                    50, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))) // valueCount = 50
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("totalValueCount must be in [0, valueCount=50]");
  }

  @Test
  void rejectsBlockSizeAboveCap() {
    // 1 << 21 = 2 MiB values per block, well above the (1 << 20) cap.
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(1 << 21, out);
              BytesUtils.writeUnsignedVarInt(4, out);
              BytesUtils.writeUnsignedVarInt(1, out);
              BytesUtils.writeZigZagVarLong(0L, out);
            });

    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    assertThatThrownBy(
            () -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blockSizeInValues must be in");
  }

  @Test
  void decodesSingleValuePage() throws IOException {
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(128, out); // blockSize
              BytesUtils.writeUnsignedVarInt(4, out); // miniBlocksPerBlock
              BytesUtils.writeUnsignedVarInt(1, out); // totalValueCount
              BytesUtils.writeZigZagVarLong(42L, out); // firstValue
            });

    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThat(reader.totalValueCount()).isEqualTo(1);
    assertThat(reader.readInteger()).isEqualTo(42);
  }
}
