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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestAggregatedStatistics {
  private final Schema schema =
      new Schema(Types.NestedField.optional(1, "str", Types.StringType.get()));
  private final SortOrder sortOrder = SortOrder.builderFor(schema).asc("str").build();
  private final SortKey sortKey = new SortKey(schema, sortOrder);
  private final MapDataStatisticsSerializer statisticsSerializer =
      MapDataStatisticsSerializer.fromSortKeySerializer(new SortKeySerializer(schema, sortOrder));

  @Test
  public void mergeDataStatisticTest() {
    SortKey keyA = sortKey.copy();
    keyA.set(0, "a");
    SortKey keyB = sortKey.copy();
    keyB.set(0, "b");

    AggregatedStatistics<MapDataStatistics, Map<SortKey, Long>> aggregatedStatistics =
        new AggregatedStatistics<>(1, statisticsSerializer);
    MapDataStatistics mapDataStatistics1 = new MapDataStatistics();
    mapDataStatistics1.add(keyA);
    mapDataStatistics1.add(keyA);
    mapDataStatistics1.add(keyB);
    aggregatedStatistics.mergeDataStatistic("testOperator", 1, mapDataStatistics1);
    MapDataStatistics mapDataStatistics2 = new MapDataStatistics();
    mapDataStatistics2.add(keyA);
    aggregatedStatistics.mergeDataStatistic("testOperator", 1, mapDataStatistics2);
    assertThat(aggregatedStatistics.dataStatistics().statistics().get(keyA))
        .isEqualTo(
            mapDataStatistics1.statistics().get(keyA) + mapDataStatistics2.statistics().get(keyA));
    assertThat(aggregatedStatistics.dataStatistics().statistics().get(keyB))
        .isEqualTo(
            mapDataStatistics1.statistics().get(keyB)
                + mapDataStatistics2.statistics().getOrDefault(keyB, 0L));
  }
}
