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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * DataStatisticsEvent is sent between data statistics coordinator and operator to transmit data
 * statistics in bytes
 */
@Internal
class StatisticsEvent implements OperatorEvent {

  private static final long serialVersionUID = 1L;
  private final long checkpointId;
  private final byte[] statisticsBytes;
  private final boolean applyImmediately;

  private StatisticsEvent(long checkpointId, byte[] statisticsBytes, boolean applyImmediately) {
    this.checkpointId = checkpointId;
    this.statisticsBytes = statisticsBytes;
    this.applyImmediately = applyImmediately;
  }

  static StatisticsEvent createTaskStatisticsEvent(
      long checkpointId,
      DataStatistics statistics,
      TypeSerializer<DataStatistics> statisticsSerializer) {
    // applyImmediately is really only relevant for coordinator to operator event.
    // task reported statistics is always merged immediately by the coordinator.
    return new StatisticsEvent(
        checkpointId,
        StatisticsUtil.serializeDataStatistics(statistics, statisticsSerializer),
        true);
  }

  static StatisticsEvent createAggregatedStatisticsEvent(
      AggregatedStatistics statistics,
      TypeSerializer<AggregatedStatistics> statisticsSerializer,
      boolean applyImmediately) {
    return new StatisticsEvent(
        statistics.checkpointId(),
        StatisticsUtil.serializeAggregatedStatistics(statistics, statisticsSerializer),
        applyImmediately);
  }

  long checkpointId() {
    return checkpointId;
  }

  byte[] statisticsBytes() {
    return statisticsBytes;
  }

  boolean applyImmediately() {
    return applyImmediately;
  }
}
