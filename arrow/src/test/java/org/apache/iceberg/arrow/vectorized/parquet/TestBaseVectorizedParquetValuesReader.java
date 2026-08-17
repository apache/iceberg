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

class TestBaseVectorizedParquetValuesReader extends VectorizedParquetDecoderTestBase {

  /** Constructs a reader with fixed bitWidth, no length prefix, and no validity vector. */
  private static BaseVectorizedParquetValuesReader fixedWidthReader(int bitWidth) {
    return new BaseVectorizedParquetValuesReader(bitWidth, 0, false, false);
  }

  @Test
  void rejectsOverflowingPackedGroupCount() throws IOException {
    int badNumGroups = Integer.MAX_VALUE / 8 + 1;
    int header = (badNumGroups << 1) | 1; // PACKED with overflowing numGroups
    byte[] page = page(out -> BytesUtils.writeUnsignedVarInt(header, out));

    BaseVectorizedParquetValuesReader reader = fixedWidthReader(1);
    reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(reader::readInteger)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid PACKED group count");
  }

  @Test
  void rejectsPackedRunNeedingMoreBytesThanAvailable() throws IOException {
    int numGroups = 100;
    int header = (numGroups << 1) | 1; // PACKED, 100 groups => needs 100 * bitWidth bytes
    byte[] page = page(out -> BytesUtils.writeUnsignedVarInt(header, out));

    BaseVectorizedParquetValuesReader reader = fixedWidthReader(8);
    reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    assertThatThrownBy(reader::readInteger)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("PACKED run needs");
  }

  @Test
  void decodesSinglePackedGroupOfZeros() throws IOException {
    // bitWidth = 1, numGroups = 1 -> 8 packed values in 1 byte. All-zero byte unpacks to 8 zeros.
    int header = (1 << 1) | 1; // = 3
    byte[] page =
        page(
            out -> {
              BytesUtils.writeUnsignedVarInt(header, out);
              out.write(0x00);
            });

    BaseVectorizedParquetValuesReader reader = fixedWidthReader(1);
    reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    for (int i = 0; i < 8; i++) {
      assertThat(reader.readInteger()).as("value %d", i).isEqualTo(0);
    }
  }
}
