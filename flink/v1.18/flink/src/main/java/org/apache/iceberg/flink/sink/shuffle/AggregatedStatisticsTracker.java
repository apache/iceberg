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

import java.util.Set;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AggregatedStatisticsTracker is used by {@link DataStatisticsCoordinator} to track the in progress
 * {@link AggregatedStatistics} received from {@link DataStatisticsOperator} subtasks for specific
 * checkpoint.
 */
class AggregatedStatisticsTracker<D extends DataStatistics<D, S>, S> {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedStatisticsTracker.class);
  private static final double ACCEPT_PARTIAL_AGGR_THRESHOLD = 90;
  private final String operatorName;
  private final TypeSerializer<DataStatistics<D, S>> statisticsSerializer;
  private final int parallelism;
  private final Set<Integer> inProgressSubtaskSet;
  private volatile AggregatedStatistics<D, S> inProgressStatistics;

  AggregatedStatisticsTracker(
      String operatorName,
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer,
      int parallelism) {
    this.operatorName = operatorName;
    this.statisticsSerializer = statisticsSerializer;
    this.parallelism = parallelism;
    this.inProgressSubtaskSet = Sets.newHashSet();
  }

  AggregatedStatistics<D, S> updateAndCheckCompletion(
      int subtask, DataStatisticsEvent<D, S> event) {
    long checkpointId = event.checkpointId();

    if (inProgressStatistics != null && inProgressStatistics.checkpointId() > checkpointId) {
      LOG.info(
          "Expect data statistics for operator {} checkpoint {}, but receive event from older checkpoint {}. Ignore it.",
          operatorName,
          inProgressStatistics.checkpointId(),
          checkpointId);
      return null;
    }

    AggregatedStatistics<D, S> completedStatistics = null;
    if (inProgressStatistics != null && inProgressStatistics.checkpointId() < checkpointId) {
      if ((double) inProgressSubtaskSet.size() / parallelism * 100
          >= ACCEPT_PARTIAL_AGGR_THRESHOLD) {
        completedStatistics = inProgressStatistics;
        LOG.info(
            "Received data statistics from {} subtasks out of total {} for operator {} at checkpoint {}. "
                + "Complete data statistics aggregation at checkpoint {} as it is more than the threshold of {} percentage",
            inProgressSubtaskSet.size(),
            parallelism,
            operatorName,
            checkpointId,
            inProgressStatistics.checkpointId(),
            ACCEPT_PARTIAL_AGGR_THRESHOLD);
      } else {
        LOG.info(
            "Received data statistics from {} subtasks out of total {} for operator {} at checkpoint {}. "
                + "Aborting the incomplete aggregation for checkpoint {}",
            inProgressSubtaskSet.size(),
            parallelism,
            operatorName,
            checkpointId,
            inProgressStatistics.checkpointId());
      }

      inProgressStatistics = null;
      inProgressSubtaskSet.clear();
    }

    if (inProgressStatistics == null) {
      LOG.info("Starting a new data statistics for checkpoint {}", checkpointId);
      inProgressStatistics = new AggregatedStatistics<>(checkpointId, statisticsSerializer);
      inProgressSubtaskSet.clear();
    }

    if (!inProgressSubtaskSet.add(subtask)) {
      LOG.debug(
          "Ignore duplicated data statistics from operator {} subtask {} for checkpoint {}.",
          operatorName,
          subtask,
          checkpointId);
    } else {
      inProgressStatistics.mergeDataStatistic(
          operatorName,
          event.checkpointId(),
          DataStatisticsUtil.deserializeDataStatistics(
              event.statisticsBytes(), statisticsSerializer));
    }

    if (inProgressSubtaskSet.size() == parallelism) {
      completedStatistics = inProgressStatistics;
      LOG.info(
          "Received data statistics from all {} operators {} for checkpoint {}. Return last completed aggregator {}.",
          parallelism,
          operatorName,
          inProgressStatistics.checkpointId(),
          completedStatistics.dataStatistics());
      inProgressStatistics = new AggregatedStatistics<>(checkpointId + 1, statisticsSerializer);
      inProgressSubtaskSet.clear();
    }

    return completedStatistics;
  }

  @VisibleForTesting
  AggregatedStatistics<D, S> inProgressStatistics() {
    return inProgressStatistics;
  }
}
