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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
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
  void rowBasedVariantReaderTracksNullsAcrossReusedBatches() {
    long allocatedBefore = ArrowAllocation.rootAllocator().getAllocatedMemory();

    Variant first = Variant.of(Variants.metadata("a"), Variants.of(1));
    Variant second = Variant.of(Variants.metadata(), Variants.of("x"));
    VectorizedArrowReader reader =
        new VectorizedArrowReader.RowBasedVariantReader(
            NestedField.optional(2, "data", VariantType.get()),
            variantReader(null, first, second, null, first),
            ArrowAllocation.rootAllocator());
    reader.setBatchSize(2);

    VectorHolder holder = reader.read(null, 2);
    assertVariants(holder, null, first);
    VarBinaryVector metadataVector =
        (VarBinaryVector) ((VectorHolder.VariantVectorHolder) holder).metadataHolder().vector();
    VarBinaryVector valueVector =
        (VarBinaryVector) ((VectorHolder.VariantVectorHolder) holder).valueHolder().vector();

    // a reused batch must overwrite every row's nullability, not keep the previous batch's
    holder = reader.read(holder, 2);
    assertVariants(holder, second, null);
    assertThat(((VectorHolder.VariantVectorHolder) holder).metadataHolder().vector())
        .as("reuse must not allocate a new vector")
        .isSameAs(metadataVector);
    assertThat(((VectorHolder.VariantVectorHolder) holder).valueHolder().vector())
        .as("reuse must not allocate a new vector")
        .isSameAs(valueVector);

    // the last batch of a row group is usually shorter than the batch size
    holder = reader.read(holder, 1);
    assertVariants(holder, first);

    reader.close();

    assertThat(ArrowAllocation.rootAllocator().getAllocatedMemory()).isEqualTo(allocatedBefore);
  }

  private static void assertVariants(VectorHolder holder, Variant... expected) {
    assertThat(holder).isInstanceOf(VectorHolder.VariantVectorHolder.class);
    VectorHolder.VariantVectorHolder variantHolder = (VectorHolder.VariantVectorHolder) holder;
    VectorHolder metadataHolder = variantHolder.metadataHolder();
    VectorHolder valueHolder = variantHolder.valueHolder();
    VarBinaryVector metadataVector = (VarBinaryVector) metadataHolder.vector();
    VarBinaryVector valueVector = (VarBinaryVector) valueHolder.vector();

    assertThat(variantHolder.numValues()).isEqualTo(expected.length);
    assertThat(metadataVector.getValueCount()).isEqualTo(expected.length);
    assertThat(valueVector.getValueCount()).isEqualTo(expected.length);
    int expectedNulls = 0;
    for (int i = 0; i < expected.length; i += 1) {
      if (expected[i] == null) {
        expectedNulls += 1;
        assertThat(metadataHolder.nullabilityHolder().isNullAt(i)).isEqualTo((byte) 1);
        assertThat(valueHolder.nullabilityHolder().isNullAt(i)).isEqualTo((byte) 1);
      } else {
        assertThat(metadataHolder.nullabilityHolder().isNullAt(i)).isEqualTo((byte) 0);
        assertThat(valueHolder.nullabilityHolder().isNullAt(i)).isEqualTo((byte) 0);
        assertThat(metadataVector.get(i)).isEqualTo(serialize(expected[i].metadata()));
        assertThat(valueVector.get(i)).isEqualTo(serialize(expected[i].value()));
      }
    }

    assertThat(metadataHolder.nullabilityHolder().numNulls()).isEqualTo(expectedNulls);
    assertThat(valueHolder.nullabilityHolder().numNulls()).isEqualTo(expectedNulls);
  }

  private static byte[] serialize(VariantMetadata metadata) {
    byte[] bytes = new byte[metadata.sizeInBytes()];
    metadata.writeTo(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }

  private static byte[] serialize(VariantValue value) {
    byte[] bytes = new byte[value.sizeInBytes()];
    value.writeTo(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }

  @SuppressWarnings("unchecked")
  private static ParquetValueReader<Variant> variantReader(Variant first, Variant... rest) {
    ParquetValueReader<Variant> reader = mock(ParquetValueReader.class);
    when(reader.read(any())).thenReturn(first, rest);
    return reader;
  }
}
