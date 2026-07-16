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
import org.junit.jupiter.api.Test;

class TestVectorizedArrowReader {
  @Test
  void rowIdPreservesPhysicalReaderWithoutBase() {
    VectorizedArrowReader physicalReader = VectorizedArrowReader.nulls();

    assertThat(VectorizedArrowReader.rowIds(null, physicalReader)).isSameAs(physicalReader);
  }

  @Test
  void lastUpdatedPreservesPhysicalReaderWithoutDataSequence() {
    VectorizedArrowReader physicalReader = VectorizedArrowReader.nulls();

    assertThat(VectorizedArrowReader.lastUpdated(10L, null, physicalReader))
        .isSameAs(physicalReader);
  }

  @Test
  void lastUpdatedInheritsWithoutBase() {
    VectorizedArrowReader reader = VectorizedArrowReader.lastUpdated(null, 5L, null);
    reader.setBatchSize(2);

    VectorHolder holder = reader.read(null, 2);
    try (BigIntVector vector = (BigIntVector) holder.vector()) {
      assertThat(vector.getDataBuffer().getLong(0)).isEqualTo(5L);
      assertThat(vector.getDataBuffer().getLong(Long.BYTES)).isEqualTo(5L);
    }
  }
}
