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
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.junit.jupiter.api.Test;

class TestVectorizedArrowReader {

  private static final int BATCH_SIZE = 1024;
  private static final int NUM_BATCHES = 64;

  @Test
  void rowIdReaderReleasesMemoryOnClose() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.rowIds(100L, null);
    reader.setBatchSize(BATCH_SIZE);

    // callers like ColumnarBatchReader never close the returned holders' vectors; the
    // reader owns its result vector and must release it on close
    for (int batch = 0; batch < NUM_BATCHES; batch += 1) {
      VectorHolder holder = reader.read(null, BATCH_SIZE);
      BigIntVector rowIds = (BigIntVector) holder.vector();
      assertThat(rowIds.getValueCount()).isEqualTo(BATCH_SIZE);
      // nullability lives in the holder's NullabilityHolder rather than arrow validity bits,
      // so read the data buffer directly like the vector accessors do. Row id = firstRowId +
      // position; positions continue across batches
      assertThat(rowIds.getDataBuffer().getLong(0)).isEqualTo(100L + (long) batch * BATCH_SIZE);
      assertThat(rowIds.getDataBuffer().getLong((long) (BATCH_SIZE - 1) * Long.BYTES))
          .isEqualTo(100L + (long) batch * BATCH_SIZE + BATCH_SIZE - 1);
    }

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void lastUpdatedSeqReaderReleasesMemoryOnClose() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.lastUpdated(100L, 42L, null);
    reader.setBatchSize(BATCH_SIZE);

    for (int batch = 0; batch < NUM_BATCHES; batch += 1) {
      VectorHolder holder = reader.read(null, BATCH_SIZE);
      BigIntVector seqNumbers = (BigIntVector) holder.vector();
      assertThat(seqNumbers.getValueCount()).isEqualTo(BATCH_SIZE);
      // no materialized sequence numbers: every value inherits the file's sequence number
      assertThat(seqNumbers.getDataBuffer().getLong(0)).isEqualTo(42L);
      assertThat(seqNumbers.getDataBuffer().getLong((long) (BATCH_SIZE - 1) * Long.BYTES))
          .isEqualTo(42L);
    }

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void rowIdReaderReusesVectorWhenHolderIsPassedBack() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.rowIds(100L, null);
    reader.setBatchSize(BATCH_SIZE);

    // ColumnarBatchReader and ArrowBatchReader hand the previous holder back as reuse
    VectorHolder holder = reader.read(null, BATCH_SIZE);
    BigIntVector first = (BigIntVector) holder.vector();
    long allocatedAfterFirstBatch = ArrowAllocation.rootAllocator().getAllocatedMemory();

    for (int batch = 1; batch < NUM_BATCHES; batch += 1) {
      holder = reader.read(holder, BATCH_SIZE);
      assertThat(holder.vector()).as("reuse must not allocate a new vector").isSameAs(first);
      assertThat(((BigIntVector) holder.vector()).getValueCount()).isEqualTo(BATCH_SIZE);
      assertThat(holder.vector().getDataBuffer().getLong(0))
          .isEqualTo(100L + (long) batch * BATCH_SIZE);
    }

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory())
        .as("reused batches must not grow allocated memory")
        .isEqualTo(allocatedAfterFirstBatch);

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void rowIdReaderReusesBatchSizedVectorForShorterBatch() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.rowIds(100L, null);
    reader.setBatchSize(BATCH_SIZE);

    VectorHolder holder = reader.read(null, BATCH_SIZE);
    BigIntVector vector = (BigIntVector) holder.vector();

    // the last batch of a row group is usually shorter and must fit the batch-sized vector
    holder = reader.read(holder, BATCH_SIZE / 2);
    assertThat(holder.vector()).isSameAs(vector);
    assertThat(((BigIntVector) holder.vector()).getValueCount()).isEqualTo(BATCH_SIZE / 2);
    assertThat(holder.vector().getDataBuffer().getLong(0))
        .isEqualTo(100L + BATCH_SIZE); // positions continue from the previous batch

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void rowIdReaderReallocatesWhenBatchSizeGrows() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.rowIds(100L, null);
    reader.setBatchSize(BATCH_SIZE);

    VectorHolder holder = reader.read(null, BATCH_SIZE);
    BigIntVector small = (BigIntVector) holder.vector();
    assertThat(small.getValueCapacity()).isGreaterThanOrEqualTo(BATCH_SIZE);

    // vectors are allocated for a full batch, so growing the batch size must release the vector
    // and let the next read allocate one that fits
    reader.setBatchSize(4 * BATCH_SIZE);
    holder = reader.read(holder, 4 * BATCH_SIZE);
    assertThat(holder.vector()).isNotSameAs(small);
    assertThat(((BigIntVector) holder.vector()).getValueCapacity())
        .isGreaterThanOrEqualTo(4 * BATCH_SIZE);
    assertThat(((BigIntVector) holder.vector()).getValueCount()).isEqualTo(4 * BATCH_SIZE);

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void rowIdReaderResolvesZeroBatchSizeToTheDefault() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.rowIds(100L, null);
    // a zero batch size falls back to the default, which must size the vector and the nullability
    // holder alike: an empty holder next to a full sized vector fails every null check
    reader.setBatchSize(0);

    VectorHolder holder = reader.read(null, VectorizedArrowReader.DEFAULT_BATCH_SIZE);
    assertThat(((BigIntVector) holder.vector()).getValueCount())
        .isEqualTo(VectorizedArrowReader.DEFAULT_BATCH_SIZE);
    assertThat(holder.nullabilityHolder().size())
        .isGreaterThanOrEqualTo(VectorizedArrowReader.DEFAULT_BATCH_SIZE);
    assertThat(holder.nullabilityHolder().isNullAt(VectorizedArrowReader.DEFAULT_BATCH_SIZE - 1))
        .isEqualTo((byte) 0);

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void positionReaderResolvesZeroBatchSizeToTheDefault() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.positions();
    reader.setBatchSize(0);

    VectorHolder holder = reader.read(null, VectorizedArrowReader.DEFAULT_BATCH_SIZE);
    assertThat(holder.nullabilityHolder().size())
        .isGreaterThanOrEqualTo(VectorizedArrowReader.DEFAULT_BATCH_SIZE);
    assertThat(holder.nullabilityHolder().isNullAt(VectorizedArrowReader.DEFAULT_BATCH_SIZE - 1))
        .isEqualTo((byte) 0);

    // the position reader does not own its vector, its caller closes it
    holder.vector().close();
    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void lastUpdatedSeqReaderReusesVectorWhenHolderIsPassedBack() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    VectorizedArrowReader reader = VectorizedArrowReader.lastUpdated(100L, 42L, null);
    reader.setBatchSize(BATCH_SIZE);

    VectorHolder holder = reader.read(null, BATCH_SIZE);
    BigIntVector first = (BigIntVector) holder.vector();
    long allocatedAfterFirstBatch = ArrowAllocation.rootAllocator().getAllocatedMemory();

    for (int batch = 1; batch < NUM_BATCHES; batch += 1) {
      holder = reader.read(holder, BATCH_SIZE);
      assertThat(holder.vector()).as("reuse must not allocate a new vector").isSameAs(first);
      assertThat(holder.vector().getDataBuffer().getLong(0)).isEqualTo(42L);
    }

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory())
        .as("reused batches must not grow allocated memory")
        .isEqualTo(allocatedAfterFirstBatch);

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  @Test
  void positionReaderWithSequentialRowsAcrossBatches() {
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
