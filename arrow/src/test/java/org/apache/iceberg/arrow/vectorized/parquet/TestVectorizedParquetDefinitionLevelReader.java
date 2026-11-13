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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestVectorizedParquetDefinitionLevelReader {
  private static final int UNIX_EPOCH_JULIAN_DAY = 2_440_588;

  @ParameterizedTest
  @CsvSource({"false, RLE", "false, PACKED", "true, RLE", "true, PACKED"})
  void decodesTimestampDictionaryValues(
      boolean nanos, BaseVectorizedParquetValuesReader.Mode mode) {
    long[] expected = {-1L, 1001L};
    try (RootAllocator allocator = new RootAllocator();
        TimeStampVector vector =
            nanos
                ? new TimeStampNanoVector("ts", allocator)
                : new TimeStampMicroVector("ts", allocator)) {
      vector.allocateNew(expected.length);
      VectorizedParquetDefinitionLevelReader definitionReader =
          new VectorizedParquetDefinitionLevelReader(1, 1, false);
      VectorizedDictionaryEncodedParquetValuesReader dictionaryReader =
          new VectorizedDictionaryEncodedParquetValuesReader(1, false);
      Dictionary dictionary =
          new Dictionary(Encoding.PLAIN_DICTIONARY) {
            @Override
            public int getMaxId() {
              return expected.length - 1;
            }

            @Override
            public Binary decodeToBinary(int id) {
              return Int96TestUtil.encode(
                  BigInteger.valueOf(expected[id])
                      .multiply(nanos ? BigInteger.ONE : BigInteger.valueOf(1000)));
            }
          };
      VectorizedParquetDefinitionLevelReader.TimestampInt96Reader timestampReader =
          definitionReader.timestampInt96Reader(vector);
      NullabilityHolder nullability = new NullabilityHolder(expected.length);
      for (int index = 0; index < expected.length; index += 1) {
        vector.set(index, 0L);
        dictionaryReader.mode = mode;
        dictionaryReader.currentCount = 1;
        dictionaryReader.currentValue = index;
        dictionaryReader.packedValuesBuffer[0] = index;
        dictionaryReader.packedValuesBufferIdx = 0;
        timestampReader.nextDictEncodedVal(
            vector, index, dictionaryReader, dictionary, mode, 1, nullability, Long.BYTES);
      }
      vector.setValueCount(expected.length);

      for (int index = 0; index < expected.length; index += 1) {
        assertThat(vector.get(index)).isEqualTo(expected[index]);
      }
    }
  }

  @Test
  public void timestampInt96ReaderPackedDictionaryDecodeDecodesRowsCorrectly() {
    try (RootAllocator allocator = new RootAllocator();
        BigIntVector vector = new BigIntVector("ts", allocator)) {
      vector.allocateNew(2);
      vector.set(0, -1L);
      vector.set(1, -1L);

      VectorizedParquetDefinitionLevelReader definitionReader =
          new VectorizedParquetDefinitionLevelReader(1, 1, false);
      VectorizedDictionaryEncodedParquetValuesReader dictionaryReader =
          new VectorizedDictionaryEncodedParquetValuesReader(1, false);

      dictionaryReader.mode = BaseVectorizedParquetValuesReader.Mode.PACKED;
      dictionaryReader.currentCount = 2;
      dictionaryReader.packedValuesBuffer[0] = 0;
      dictionaryReader.packedValuesBuffer[1] = 1;

      Dictionary dictionary =
          new Dictionary(Encoding.PLAIN_DICTIONARY) {
            @Override
            public int getMaxId() {
              return 1;
            }

            @Override
            public Binary decodeToBinary(int id) {
              if (id == 0) {
                return int96Binary(111_111L);
              } else if (id == 1) {
                return int96Binary(222_222L);
              }

              throw new IllegalArgumentException("Unexpected dictionary id: " + id);
            }
          };

      VectorizedParquetDefinitionLevelReader.TimestampInt96Reader timestampReader =
          definitionReader.timestampInt96Reader();

      timestampReader.nextDictEncodedVal(
          vector,
          0,
          dictionaryReader,
          dictionary,
          BaseVectorizedParquetValuesReader.Mode.PACKED,
          1,
          null,
          Long.BYTES);
      timestampReader.nextDictEncodedVal(
          vector,
          1,
          dictionaryReader,
          dictionary,
          BaseVectorizedParquetValuesReader.Mode.PACKED,
          1,
          null,
          Long.BYTES);

      vector.setValueCount(2);

      assertThat(vector.get(0))
          .as("row 0 should receive the first decoded timestamp")
          .isEqualTo(111_111L);
      assertThat(vector.get(1))
          .as("row 1 should receive the second decoded timestamp")
          .isEqualTo(222_222L);
    }
  }

  private static Binary int96Binary(long micros) {
    long timeOfDayNanos = micros * 1_000L;
    byte[] bytes =
        ByteBuffer.allocate(12)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(timeOfDayNanos)
            .putInt(UNIX_EPOCH_JULIAN_DAY)
            .array();
    return Binary.fromConstantByteArray(bytes);
  }
}
