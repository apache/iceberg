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
package org.apache.iceberg.flink.sink.shuffle;

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SCHEMA;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SORT_ORDER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

public class TestRangePartitioner {
  private final int numPartitions = 4;

  @Test
  public void testRoundRobinRecordsBeforeStatisticsAvailable() {
    RangePartitioner partitioner = new RangePartitioner(SCHEMA, SORT_ORDER);
    Set<Integer> results = Sets.newHashSetWithExpectedSize(numPartitions);
    for (int i = 0; i < numPartitions; ++i) {
      results.add(
          partitioner.partition(
              StatisticsOrRecord.fromRecord(GenericRowData.of(StringData.fromString("a"), 1)),
              numPartitions));
    }

    // round-robin. every partition should get an assignment
    assertThat(results).containsExactlyInAnyOrder(0, 1, 2, 3);
  }

  @Test
  public void testRoundRobinStatisticsWrapper() {
    RangePartitioner partitioner = new RangePartitioner(SCHEMA, SORT_ORDER);
    Set<Integer> results = Sets.newHashSetWithExpectedSize(numPartitions);
    for (int i = 0; i < numPartitions; ++i) {
      GlobalStatistics statistics =
          GlobalStatistics.fromRangeBounds(1L, new SortKey[] {CHAR_KEYS.get("a")});
      results.add(
          partitioner.partition(StatisticsOrRecord.fromStatistics(statistics), numPartitions));
    }

    // round-robin. every partition should get an assignment
    assertThat(results).containsExactlyInAnyOrder(0, 1, 2, 3);
  }
}
