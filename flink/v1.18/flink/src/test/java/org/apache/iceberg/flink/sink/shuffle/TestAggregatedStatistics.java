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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

public class TestAggregatedStatistics {

  @Test
  public void mergeDataStatisticTest() {
    GenericRowData rowDataA = GenericRowData.of(StringData.fromString("a"));
    GenericRowData rowDataB = GenericRowData.of(StringData.fromString("b"));

    AggregatedStatistics<MapDataStatistics, Map<RowData, Long>> aggregatedStatistics =
        new AggregatedStatistics<>(
            1,
            MapDataStatisticsSerializer.fromKeySerializer(
                new RowDataSerializer(RowType.of(new VarCharType()))));
    MapDataStatistics mapDataStatistics1 = new MapDataStatistics();
    mapDataStatistics1.add(rowDataA);
    mapDataStatistics1.add(rowDataA);
    mapDataStatistics1.add(rowDataB);
    aggregatedStatistics.mergeDataStatistic("testOperator", 1, mapDataStatistics1);
    MapDataStatistics mapDataStatistics2 = new MapDataStatistics();
    mapDataStatistics2.add(rowDataA);
    aggregatedStatistics.mergeDataStatistic("testOperator", 1, mapDataStatistics2);
    assertThat(aggregatedStatistics.dataStatistics().statistics().get(rowDataA))
        .isEqualTo(
            mapDataStatistics1.statistics().get(rowDataA)
                + mapDataStatistics2.statistics().get(rowDataA));
    assertThat(aggregatedStatistics.dataStatistics().statistics().get(rowDataB))
        .isEqualTo(
            mapDataStatistics1.statistics().get(rowDataB)
                + mapDataStatistics2.statistics().getOrDefault(rowDataB, 0L));
  }
}
