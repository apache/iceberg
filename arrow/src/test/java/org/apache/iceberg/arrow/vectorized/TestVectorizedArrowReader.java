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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.junit.jupiter.api.Test;

public class TestVectorizedArrowReader {

  @Test
  public void positionReaderWithSequentialRowsAcrossBatches() {
    VectorizedArrowReader reader = VectorizedArrowReader.positions();
    reader.setBatchSize(2);
    reader.setRowGroupInfo(pageReadStore(1_000L, 4L), Collections.emptyMap());

    assertPositions(reader.read(null, 2), 1_000L, 1_001L);
    assertPositions(reader.read(null, 2), 1_002L, 1_003L);
  }

  @Test
  public void positionReaderWithRowIndexesAcrossBatches() {
    VectorizedArrowReader reader = VectorizedArrowReader.positions();
    reader.setBatchSize(2);
    reader.setRowGroupInfo(pageReadStore(1_000L, 10L, 11L, 100L, 101L), Collections.emptyMap());

    assertPositions(reader.read(null, 2), 1_010L, 1_011L);
    assertPositions(reader.read(null, 2), 1_100L, 1_101L);
  }

  @Test
  public void positionReaderResetsForNewPageStore() {
    VectorizedArrowReader reader = VectorizedArrowReader.positions();
    reader.setBatchSize(2);

    reader.setRowGroupInfo(pageReadStore(100L, 10L, 20L), Collections.emptyMap());
    assertPositions(reader.read(null, 2), 110L, 120L);

    reader.setRowGroupInfo(pageReadStore(1_000L, 2L), Collections.emptyMap());
    assertPositions(reader.read(null, 2), 1_000L, 1_001L);
  }

  private static void assertPositions(VectorHolder holder, long... expectedPositions) {
    try (BigIntVector vector = (BigIntVector) holder.vector()) {
      assertThat(vector.getValueCount()).isEqualTo(expectedPositions.length);
      ArrowBuf dataBuffer = vector.getDataBuffer();
      for (int i = 0; i < expectedPositions.length; i += 1) {
        assertThat(dataBuffer.getLong((long) i * Long.BYTES))
            .as("Position at index %s", i)
            .isEqualTo(expectedPositions[i]);
      }
    }
  }

  private static PageReadStore pageReadStore(long rowIndexOffset, long rowCount) {
    return new TestPageReadStore(rowCount, rowIndexOffset, null);
  }

  private static PageReadStore pageReadStore(long rowIndexOffset, long... rowIndexes) {
    return new TestPageReadStore(rowIndexes.length, rowIndexOffset, rowIndexes);
  }

  private record TestPageReadStore(long rowCount, long rowIndexOffset, long[] rowIndexes)
      implements PageReadStore {

    @Override
    public PageReader getPageReader(ColumnDescriptor descriptor) {
      throw new UnsupportedOperationException("Page readers are not used by position reader tests");
    }

    @Override
    public long getRowCount() {
      return rowCount;
    }

    @Override
    public Optional<Long> getRowIndexOffset() {
      return Optional.of(rowIndexOffset);
    }

    @Override
    public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
      if (rowIndexes == null) {
        return Optional.empty();
      }

      return Optional.of(Arrays.stream(rowIndexes).iterator());
    }
  }
}
