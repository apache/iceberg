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
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.Test;

class TestVectorizedDeltaEncodedValuesReader {
  @Test
  void readIntegersNoMoreValues() throws Exception {
    // 10 values: read in two batches of 6 then 4, then assert the overflow guard fires
    int[] values = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    VectorizedDeltaEncodedValuesReader reader = readerFor(values);

    int[] batch1 = reader.readIntegers(6, 0);
    int[] batch2 = reader.readIntegers(4, 0);

    assertThat(batch1).containsExactly(10, 20, 30, 40, 50, 60);
    assertThat(batch2).containsExactly(70, 80, 90, 100);

    assertThat(reader.totalValueCount()).isEqualTo(10);
    assertThatThrownBy(() -> reader.readIntegers(1, 0))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("No more values to read");
  }

  private static VectorizedDeltaEncodedValuesReader readerFor(int... values) throws IOException {
    ByteBuffer encoded = write(values);
    VectorizedDeltaEncodedValuesReader reader = new VectorizedDeltaEncodedValuesReader();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(encoded));
    return reader;
  }

  private static ByteBuffer write(int[] values) {
    try (DeltaBinaryPackingValuesWriterForInteger writer =
        new DeltaBinaryPackingValuesWriterForInteger(
            128, 4, 100, 200, HeapByteBufferAllocator.getInstance())) {
      for (int value : values) {
        writer.writeInteger(value);
      }
      return writer
          .getBytes()
          .toByteBuffer(new ByteBufferReleaser(HeapByteBufferAllocator.getInstance()));
    }
  }
}
