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
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** MapDataStatistics uses map to count key frequency */
@Internal
class MapDataStatistics implements DataStatistics<MapDataStatistics, Map<RowData, Long>> {
  private final Map<RowData, Long> statistics;

  MapDataStatistics() {
    this.statistics = Maps.newHashMap();
  }

  MapDataStatistics(Map<RowData, Long> statistics) {
    this.statistics = statistics;
  }

  @Override
  public boolean isEmpty() {
    return statistics.size() == 0;
  }

  @Override
  public void add(RowData key) {
    // increase count of occurrence by one in the dataStatistics map
    statistics.merge(key, 1L, Long::sum);
  }

  @Override
  public void merge(MapDataStatistics otherStatistics) {
    otherStatistics.statistics().forEach((key, count) -> statistics.merge(key, count, Long::sum));
  }

  @Override
  public Map<RowData, Long> statistics() {
    return statistics;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("statistics", statistics).toString();
  }
}
