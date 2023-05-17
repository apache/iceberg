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

import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestGlobalStatisticsAggregator {

  @Test
  public void mergeDataStatisticTest() {
    GlobalStatisticsAggregator<MapDataStatistics, Map<RowData, Long>> globalStatisticsAggregator =
        new GlobalStatisticsAggregator<>(
            1,
            MapDataStatisticsSerializer.fromKeySerializer(
                new RowDataSerializer(RowType.of(new VarCharType()))));
    DataStatistics<MapDataStatistics, Map<RowData, Long>> mapDataStatistics1 =
        new MapDataStatistics();
    mapDataStatistics1.add(GenericRowData.of(StringData.fromString("a")));
    mapDataStatistics1.add(GenericRowData.of(StringData.fromString("a")));
    mapDataStatistics1.add(GenericRowData.of(StringData.fromString("b")));
    globalStatisticsAggregator.mergeDataStatistic(
        1, new DataStatisticsEvent<>(1, mapDataStatistics1));
    MapDataStatistics mapDataStatistics2 = new MapDataStatistics();
    mapDataStatistics2.add(GenericRowData.of(StringData.fromString("a")));
    globalStatisticsAggregator.mergeDataStatistic(
        2, new DataStatisticsEvent<>(1, mapDataStatistics2));
    globalStatisticsAggregator.mergeDataStatistic(
        1, new DataStatisticsEvent<>(1, mapDataStatistics1));
    Assertions.assertEquals(
        3L,
        (long)
            globalStatisticsAggregator
                .dataStatistics()
                .statistics()
                .get(GenericRowData.of(StringData.fromString("a"))));
    Assertions.assertEquals(
        1L,
        (long)
            globalStatisticsAggregator
                .dataStatistics()
                .statistics()
                .get(GenericRowData.of(StringData.fromString("b"))));
  }
}
