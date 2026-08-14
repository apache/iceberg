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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the Arrow sizing contract that {@code VectorizedArrowReader} relies on when it allocates
 * variable-width vectors.
 *
 * <p>{@code setInitialCapacity(int)} takes a <em>value count</em>, not a byte count, and sizes the
 * data buffer at Arrow's default of 8 bytes per value. Passing {@code batchSize * averageWidth} to
 * it therefore reserves offsets for {@code averageWidth} times too many values. The density
 * overload, {@code setInitialCapacity(int, double)}, expresses what the reader actually means.
 */
public class TestVariableWidthInitialCapacity {

  private static final int BATCH_SIZE = 5000;
  private static final int AVERAGE_WIDTH = 10;

  private BufferAllocator allocator;

  @BeforeEach
  public void before() {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void after() {
    allocator.close();
  }

  @Test
  public void testDensityOverloadReservesOneSlotPerRow() {
    try (VarCharVector vector = new VarCharVector("v", allocator)) {
      vector.setInitialCapacity(BATCH_SIZE, AVERAGE_WIDTH);
      vector.allocateNewSafe();

      assertThat(vector.getValueCapacity())
          .as("offsets should be reserved for one batch of rows")
          .isGreaterThanOrEqualTo(BATCH_SIZE)
          .isLessThan(BATCH_SIZE * AVERAGE_WIDTH);
    }
  }

  @Test
  public void testFixedWidthVectorIsSizedByValueCount() {
    int byteWidth = 16; // a UUID column
    try (FixedSizeBinaryVector sizedByValues =
            new FixedSizeBinaryVector("v", allocator, byteWidth);
        FixedSizeBinaryVector sizedByBytes = new FixedSizeBinaryVector("v", allocator, byteWidth)) {
      sizedByValues.setInitialCapacity(BATCH_SIZE);
      sizedByValues.allocateNew();

      sizedByBytes.setInitialCapacity(BATCH_SIZE * byteWidth);
      sizedByBytes.allocateNew();

      assertThat(sizedByValues.getValueCapacity())
          .as("a batch worth of values is enough to hold the batch")
          .isGreaterThanOrEqualTo(BATCH_SIZE);

      assertThat(sizedByBytes.getDataBuffer().capacity())
          .as("passing a byte count over-allocates by the byte width")
          .isGreaterThan(sizedByValues.getDataBuffer().capacity() * (byteWidth - 1L));
    }
  }

  /**
   * The multiplication that used to be passed to the single argument overload. Kept as an explicit
   * contrast so the reason for the density overload is not lost again.
   */
  @Test
  public void testByteCountPassedAsValueCountOverReservesOffsets() {
    try (VarCharVector vector = new VarCharVector("v", allocator)) {
      vector.setInitialCapacity(BATCH_SIZE * AVERAGE_WIDTH);
      vector.allocateNewSafe();

      assertThat(vector.getValueCapacity())
          .as("a byte count used as a value count reserves offsets for far more rows than a batch")
          .isGreaterThanOrEqualTo(BATCH_SIZE * AVERAGE_WIDTH);
    }
  }
}
