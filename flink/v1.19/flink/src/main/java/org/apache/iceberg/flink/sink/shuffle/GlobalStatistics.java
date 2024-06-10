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
import org.apache.iceberg.SortKey;

/**
 * This is the globally aggregated statistics by the coordinator. It is what is sent to the operator
 * subtasks for range partitioning.
 */
class GlobalStatistics extends AggregatedStatistics {
  GlobalStatistics(
      long checkpointId,
      StatisticsType type,
      Map<SortKey, Long> keyFrequency,
      SortKey[] rangeBounds) {
    super(checkpointId, type, keyFrequency, rangeBounds);
  }

  SortKey[] rangeBounds() {
    return super.keys();
  }

  static GlobalStatistics fromKeyFrequency(long checkpointId, Map<SortKey, Long> stats) {
    return new GlobalStatistics(checkpointId, StatisticsType.Map, stats, null);
  }

  static GlobalStatistics fromRangeBounds(long checkpointId, SortKey[] rangeBounds) {
    return new GlobalStatistics(checkpointId, StatisticsType.Sketch, null, rangeBounds);
  }
}
