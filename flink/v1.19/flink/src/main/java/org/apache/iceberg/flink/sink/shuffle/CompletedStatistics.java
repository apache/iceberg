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
 * This is what {@link AggregatedStatisticsTracker} returns upon a completed statistics aggregation
 * from all subtasks.
 */
class CompletedStatistics extends AggregatedStatistics {

  CompletedStatistics(
      long checkpointId,
      StatisticsType type,
      Map<SortKey, Long> keyFrequency,
      SortKey[] keySamples) {
    super(checkpointId, type, keyFrequency, keySamples);
  }

  SortKey[] keySamples() {
    return super.keys();
  }

  static CompletedStatistics fromKeyFrequency(long checkpointId, Map<SortKey, Long> stats) {
    return new CompletedStatistics(checkpointId, StatisticsType.Map, stats, null);
  }

  static CompletedStatistics fromKeySamples(long checkpointId, SortKey[] keySamples) {
    return new CompletedStatistics(checkpointId, StatisticsType.Sketch, null, keySamples);
  }
}
