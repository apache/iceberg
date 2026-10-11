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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.jupiter.api.Test;

class TestVectorizedByteStreamSplitValuesReader extends VectorizedParquetDecoderTestBase {

  @Test
  void rejectsNegativeValueCount() {
    byte[] page = new byte[0];

    VectorizedByteStreamSplitValuesReader reader = new VectorizedByteStreamSplitValuesReader(4);
    assertThatThrownBy(
            () -> reader.initFromPage(-1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("valueCount must be >= 0");
  }

  @Test
  void acceptsPageSmallerThanValueCountTimesElementSize() {
    // valueCount=4 means 4 logical rows, but BYTE_STREAM_SPLIT only encodes non-null values, so
    // a page covering 2 nulls + 2 non-null floats is 8 bytes — legitimate, not a size mismatch.
    byte[] page = new byte[8];

    VectorizedByteStreamSplitValuesReader reader = new VectorizedByteStreamSplitValuesReader(4);
    reader.initFromPage(4, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
  }

  @Test
  void rejectsPageSizeMismatchWithOversizedPage() {
    // valueCount=2, elementSize=4 => bound is 8 bytes, but page has 1024 (an attacker-shaped page)
    byte[] page = new byte[1024];

    VectorizedByteStreamSplitValuesReader reader = new VectorizedByteStreamSplitValuesReader(4);
    assertThatThrownBy(
            () -> reader.initFromPage(2, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds bound");
  }

  @Test
  void rejectsNegativeLengthInReadBinary() {
    // valueCount=1, elementSize=4 => 4 raw bytes.
    byte[] page = new byte[4];

    VectorizedByteStreamSplitValuesReader reader = new VectorizedByteStreamSplitValuesReader(4);
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(() -> reader.readBinary(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Length must be >= 0");
  }

  @Test
  void rejectsLengthExceedingDecodedStream() {
    byte[] page = new byte[4];

    VectorizedByteStreamSplitValuesReader reader = new VectorizedByteStreamSplitValuesReader(4);
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(() -> reader.readBinary(1024 * 1024))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds 4 bytes remaining");
  }

  @Test
  void decodesIntegersRoundTrip() {
    // BYTE_STREAM_SPLIT: for 4-byte ints, bytes are interleaved by position.
    // Values: 0x01020304, 0x05060708 stored little-endian:
    //   stream 0 (LSB): 0x04, 0x08
    //   stream 1:       0x03, 0x07
    //   stream 2:       0x02, 0x06
    //   stream 3 (MSB): 0x01, 0x05
    byte[] page = {0x04, 0x08, 0x03, 0x07, 0x02, 0x06, 0x01, 0x05};

    VectorizedByteStreamSplitValuesReader reader = new VectorizedByteStreamSplitValuesReader(4);
    reader.initFromPage(2, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    int first = reader.readInteger();
    int second = reader.readInteger();
    int firstLittleEndian =
        ByteBuffer.wrap(new byte[] {0x04, 0x03, 0x02, 0x01})
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt();
    int secondLittleEndian =
        ByteBuffer.wrap(new byte[] {0x08, 0x07, 0x06, 0x05})
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt();
    assertThat(first).isEqualTo(firstLittleEndian);
    assertThat(second).isEqualTo(secondLittleEndian);
  }
}
