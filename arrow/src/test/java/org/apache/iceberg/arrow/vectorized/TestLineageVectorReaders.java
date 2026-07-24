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

import org.apache.arrow.vector.BigIntVector;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.junit.jupiter.api.Test;

public class TestLineageVectorReaders {

  private static final int BATCH_SIZE = 1024;
  private static final int NUM_BATCHES = 64;

  @Test
  public void testRowIdReaderReleasesMemoryOnClose() {
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
  public void testLastUpdatedSeqReaderReleasesMemoryOnClose() {
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
}
