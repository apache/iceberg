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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestBasePartitionStatistics {

  private static final PartitionData PARTITION =
      new PartitionData(
          Types.StructType.of(Types.NestedField.required(1, "foo", Types.IntegerType.get())));

  @Test
  public void testAppendWithAllValues() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, 15L, 1625077800000L, 12345L);
    BasePartitionStatistics stats2 =
        createStats(200L, 7, 500L, 1L, 100, 0L, 50, 7L, 1625077900000L, 12346L);

    stats1.appendStats(stats2);

    validateStats(stats1, 300L, 22, 1500L, 3L, 600, 1L, 250, 22L, 1625077900000L, 12346L);
  }

  @Test
  public void testAppendWithThisNullOptionalField() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, null, null, null);
    BasePartitionStatistics stats2 =
        createStats(100L, 7, 500L, 1L, 100, 0L, 50, 7L, 1625077900000L, 12346L);

    stats1.appendStats(stats2);

    validateStats(stats1, 200L, 22, 1500L, 3L, 600, 1L, 250, 7L, 1625077900000L, 12346L);
  }

  @Test
  public void testAppendWithBothNullOptionalFields() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, null, null, null);
    BasePartitionStatistics stats2 = createStats(100L, 7, 500L, 1L, 100, 0L, 50, null, null, null);

    stats1.appendStats(stats2);

    validateStats(stats1, 200L, 22, 1500L, 3L, 600, 1L, 250, null, null, null);
  }

  @Test
  public void testAppendWithOtherNullOptionalFields() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, 15L, 1625077900000L, 12346L);
    BasePartitionStatistics stats2 = createStats(100L, 7, 500L, 1L, 100, 0L, 50, null, null, null);

    stats1.appendStats(stats2);

    validateStats(stats1, 200L, 22, 1500L, 3L, 600, 1L, 250, 15L, 1625077900000L, 12346L);
  }

  @Test
  public void testAppendEmptyStats() {
    BasePartitionStatistics stats1 = new BasePartitionStatistics(PARTITION, 1);
    BasePartitionStatistics stats2 = new BasePartitionStatistics(PARTITION, 1);

    stats1.appendStats(stats2);

    validateStats(stats1, 0L, 0, 0L, 0L, 0, 0L, 0, null, null, null);
  }

  @Test
  public void testAppendWithDifferentSpec() {
    BasePartitionStatistics stats1 = new BasePartitionStatistics(PARTITION, 1);
    BasePartitionStatistics stats2 = new BasePartitionStatistics(PARTITION, 2);

    assertThatThrownBy(() -> stats1.appendStats(stats2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Spec IDs must match");
  }

  private BasePartitionStatistics createStats(
      long dataRecordCount,
      int dataFileCount,
      long totalDataFileSizeInBytes,
      long positionDeleteRecordCount,
      int positionDeleteFileCount,
      long equalityDeleteRecordCount,
      int equalityDeleteFileCount,
      Long totalRecordCount,
      Long lastUpdatedAt,
      Long lastUpdatedSnapshotId) {

    BasePartitionStatistics stats = new BasePartitionStatistics(PARTITION, 1);
    stats.set(2, dataRecordCount);
    stats.set(3, dataFileCount);
    stats.set(4, totalDataFileSizeInBytes);
    stats.set(5, positionDeleteRecordCount);
    stats.set(6, positionDeleteFileCount);
    stats.set(7, equalityDeleteRecordCount);
    stats.set(8, equalityDeleteFileCount);
    stats.set(9, totalRecordCount);
    stats.set(10, lastUpdatedAt);
    stats.set(11, lastUpdatedSnapshotId);

    return stats;
  }

  private void validateStats(PartitionStatistics stats, Object... expectedValues) {
    // Spec id and partition data should be unchanged
    assertThat(stats.get(0, PartitionData.class)).isEqualTo(PARTITION);
    assertThat(stats.get(1, Integer.class)).isEqualTo(1);

    for (int i = 0; i < expectedValues.length; i++) {
      if (expectedValues[i] == null) {
        assertThat(stats.get(i + 2, Object.class)).isNull();
      } else {
        assertThat(stats.get(i + 2, Object.class)).isEqualTo(expectedValues[i]);
      }
    }
  }
}
