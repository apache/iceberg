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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

public class TestVectorizedDictionaryEncodedParquetValuesReader {

  @Test
  public void varWidthBinaryDictEncodedReaderHandlesNonArrayBackedByteBuffer() {
    byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
    // pad the direct buffer so the payload sits at a non-zero offset, exercising the
    // position/offset handling and not just an empty-buffer edge case.
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(3 + payload.length + 2);
    directBuffer.put(new byte[] {-1, -1, -1});
    directBuffer.put(payload);
    directBuffer.put(new byte[] {-1, -1});
    directBuffer.flip();
    assertThat(directBuffer.hasArray()).as("buffer under test must not be array-backed").isFalse();

    Binary binary = Binary.fromConstantByteBuffer(directBuffer, 3, payload.length);

    Dictionary dictionary =
        new Dictionary(Encoding.PLAIN_DICTIONARY) {
          @Override
          public int getMaxId() {
            return 0;
          }

          @Override
          public Binary decodeToBinary(int id) {
            return binary;
          }
        };

    try (RootAllocator allocator = new RootAllocator();
        VarCharVector vector = new VarCharVector("s", allocator)) {
      vector.allocateNew(payload.length, 1);

      VectorizedDictionaryEncodedParquetValuesReader dictionaryReader =
          new VectorizedDictionaryEncodedParquetValuesReader(1, false);
      dictionaryReader.mode = BaseVectorizedParquetValuesReader.Mode.RLE;
      dictionaryReader.currentCount = 1;
      dictionaryReader.currentValue = 0;

      dictionaryReader
          .varWidthBinaryDictEncodedReader()
          .nextBatch(vector, 0, 1, dictionary, new NullabilityHolder(1), 0);

      vector.setValueCount(1);

      assertThat(new String(vector.get(0), StandardCharsets.UTF_8)).isEqualTo("hello");
    }
  }
}
