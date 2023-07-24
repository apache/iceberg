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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GlobalStatisticsTracker is used by {@link DataStatisticsCoordinator} to track the in progress
 * {@link GlobalStatistics} received from {@link DataStatisticsOperator} subtasks for specific
 * checkpoint.
 */
@Internal
class GlobalStatisticsTracker<D extends DataStatistics<D, S>, S> {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalStatisticsTracker.class);
  private static final double EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE = 90;
  private final String operatorName;
  private final TypeSerializer<DataStatistics<D, S>> statisticsSerializer;
  private final int parallelism;
  private volatile GlobalStatistics<D, S> inProgressStatistics;

  GlobalStatisticsTracker(
      String operatorName,
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer,
      int parallelism) {
    this.operatorName = operatorName;
    this.statisticsSerializer = statisticsSerializer;
    this.parallelism = parallelism;
  }

  GlobalStatistics<D, S> receiveDataStatisticEventAndCheckCompletion(
      int subtask, DataStatisticsEvent<D, S> event) {
    long checkpointId = event.checkpointId();

    if (inProgressStatistics != null && inProgressStatistics.checkpointId() > checkpointId) {
      LOG.debug(
          "Expect data statistics for operator {} checkpoint {}, but receive event from older checkpoint {}. Ignore it.",
          operatorName,
          inProgressStatistics.checkpointId(),
          checkpointId);
      return null;
    }

    GlobalStatistics<D, S> completedStatistics = null;
    if (inProgressStatistics != null && inProgressStatistics.checkpointId() < checkpointId) {
      if ((double) inProgressStatistics.aggregatedSubtasksCount() / parallelism * 100
          >= EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE) {
        completedStatistics = inProgressStatistics;
        LOG.info(
            "Received data statistics from {} operators {} out of total {} for checkpoint {}. "
                + "It's more than the expected percentage {}. Complete data statistics aggregation {}",
            inProgressStatistics.aggregatedSubtasksCount(),
            operatorName,
            parallelism,
            inProgressStatistics.checkpointId(),
            EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE,
            completedStatistics);
      } else {
        LOG.info(
            "Received data statistics from {} operators {} out of total {} for checkpoint {}. "
                + "It's less than the expected percentage {}. Dropping the incomplete aggregate "
                + "data statistics and starting collecting data statistics from new checkpoint {}",
            inProgressStatistics.aggregatedSubtasksCount(),
            operatorName,
            parallelism,
            inProgressStatistics.checkpointId(),
            EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE,
            checkpointId);
      }
      inProgressStatistics = null;
    }

    if (inProgressStatistics == null) {
      inProgressStatistics = new GlobalStatistics<>(checkpointId, statisticsSerializer);
    }

    inProgressStatistics.mergeDataStatistic(
        operatorName,
        subtask,
        event.checkpointId(),
        DataStatisticsUtil.deserializeDataStatistics(
            event.statisticsBytes(), statisticsSerializer));

    if (inProgressStatistics.aggregatedSubtasksCount() == parallelism) {
      completedStatistics = inProgressStatistics;
      LOG.info(
          "Received data statistics from all {} operators {} for checkpoint {}. Return last completed aggregator {}.",
          parallelism,
          operatorName,
          inProgressStatistics.checkpointId(),
          completedStatistics.dataStatistics());
      inProgressStatistics = new GlobalStatistics<>(checkpointId + 1, statisticsSerializer);
    }
    return completedStatistics;
  }

  @VisibleForTesting
  GlobalStatistics<D, S> inProgressStatistics() {
    return inProgressStatistics;
  }
}
