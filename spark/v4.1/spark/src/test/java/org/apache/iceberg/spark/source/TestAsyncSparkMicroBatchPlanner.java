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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.Test;

class TestAsyncSparkMicroBatchPlanner {

  @Test
  void reachedAvailableNowCapReturnsTrueOnlyForExactCapSnapshot() {
    Snapshot capSnapshot = mockSnapshot(10L);
    Snapshot laterSnapshotWithHigherId = mockSnapshot(20L);
    Snapshot laterSnapshotWithLowerId = mockSnapshot(5L);
    StreamingOffset capOffset = new StreamingOffset(10L, 3L, false);

    assertThat(AsyncSparkMicroBatchPlanner.reachedAvailableNowCap(capSnapshot, capOffset)).isTrue();
    assertThat(
            AsyncSparkMicroBatchPlanner.reachedAvailableNowCap(
                laterSnapshotWithHigherId, capOffset))
        .isFalse();
    assertThat(
            AsyncSparkMicroBatchPlanner.reachedAvailableNowCap(laterSnapshotWithLowerId, capOffset))
        .isFalse();
  }

  @Test
  void reachedAvailableNowCapReturnsFalseWhenCapOrSnapshotIsMissing() {
    Snapshot readFrom = mockSnapshot(10L);
    StreamingOffset capOffset = new StreamingOffset(10L, 1L, false);

    assertThat(AsyncSparkMicroBatchPlanner.reachedAvailableNowCap(readFrom, null)).isFalse();
    assertThat(AsyncSparkMicroBatchPlanner.reachedAvailableNowCap(null, capOffset)).isFalse();
  }

  private Snapshot mockSnapshot(long snapshotId) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(snapshotId);
    return snapshot;
  }
}
