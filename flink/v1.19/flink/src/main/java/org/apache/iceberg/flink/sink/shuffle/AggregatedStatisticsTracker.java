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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AggregatedStatisticsTracker is used by {@link DataStatisticsCoordinator} to track the in progress
 * {@link AggregatedStatistics} received from {@link DataStatisticsOperator} subtasks for specific
 * checkpoint.
 */
class AggregatedStatisticsTracker {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedStatisticsTracker.class);
  private static final double ACCEPT_PARTIAL_AGGR_THRESHOLD = 90;
  private final String operatorName;
  private final int parallelism;
  private final TypeSerializer<DataStatistics> statisticsSerializer;
  private final int downstreamParallelism;
  private final StatisticsType statisticsType;
  private final int switchToSketchThreshold;
  private final Comparator<StructLike> comparator;

  private final Set<Integer> inProgressSubtaskSet;
  private volatile long inProgressCheckpointId;
  private volatile StatisticsType coordinatorStatisticsType;
  private volatile Map<SortKey, Long> coordinatorMapStatistics;
  private volatile ReservoirItemsUnion<SortKey> coordinatorSketchStatistics;

  AggregatedStatisticsTracker(
      String operatorName,
      int parallelism,
      Schema schema,
      SortOrder sortOrder,
      int downstreamParallelism,
      StatisticsType statisticsType,
      int switchToSketchThreshold,
      @Nullable AggregatedStatistics restoredStatistics) {
    this.operatorName = operatorName;
    this.parallelism = parallelism;
    this.statisticsSerializer =
        new DataStatisticsSerializer(new SortKeySerializer(schema, sortOrder));
    this.downstreamParallelism = downstreamParallelism;
    this.statisticsType = statisticsType;
    this.switchToSketchThreshold = switchToSketchThreshold;

    this.comparator = SortOrderComparators.forSchema(schema, sortOrder);
    this.inProgressSubtaskSet = Sets.newHashSet();
    this.coordinatorStatisticsType = StatisticsUtil.collectType(statisticsType, restoredStatistics);
    this.inProgressCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
  }

  AggregatedStatistics updateAndCheckCompletion(int subtask, StatisticsEvent event) {
    long checkpointId = event.checkpointId();
    LOG.debug(
        "Handling statistics event from subtask {} of operator {} for checkpoint {}",
        subtask,
        operatorName,
        checkpointId);

    if (inProgressCheckpointId > checkpointId) {
      LOG.info(
          "Ignore stale statistics event from operator {} subtask {} for older checkpoint {}. Was expecting data statistics from checkpoint {}",
          operatorName,
          subtask,
          checkpointId,
          inProgressCheckpointId);
      return null;
    }

    AggregatedStatistics completedStatistics = null;
    if (inProgress() && inProgressCheckpointId < checkpointId) {
      LOG.info(
          "Received data statistics from new checkpoint {} with in-progress checkpoint as {}",
          checkpointId,
          inProgressCheckpointId);
      if ((double) inProgressSubtaskSet.size() / parallelism * 100
          >= ACCEPT_PARTIAL_AGGR_THRESHOLD) {
        completedStatistics = completedStatistics();
        LOG.info(
            "Received data statistics from {} subtasks out of total {} for operator {} from in-progress checkpoint {}. "
                + "Complete aggregation as more than {} percentage of subtasks reported statistics after receiving statistics report for new checkpoint {}.",
            inProgressSubtaskSet.size(),
            parallelism,
            operatorName,
            inProgressCheckpointId,
            ACCEPT_PARTIAL_AGGR_THRESHOLD,
            checkpointId);
      } else {
        resetAggregates();
        LOG.info(
            "Received data statistics from {} subtasks out of total {} for operator {} for in-progress checkpoint {}. "
                + "Aborting the incomplete aggregation after receiving statistics report for new checkpoint {}",
            inProgressSubtaskSet.size(),
            parallelism,
            operatorName,
            inProgressCheckpointId,
            checkpointId);
      }
    }

    DataStatistics dataStatistics =
        StatisticsUtil.deserializeDataStatistics(event.statisticsBytes(), statisticsSerializer);
    if (!inProgress()) {
      initializeAggregates(checkpointId, dataStatistics);
    }

    if (!inProgressSubtaskSet.add(subtask)) {
      LOG.debug(
          "Ignore duplicated data statistics from operator {} subtask {} for checkpoint {}.",
          operatorName,
          subtask,
          checkpointId);
    } else {
      merge(dataStatistics);
      LOG.debug(
          "Merge data statistics from operator {} subtask {} for checkpoint {}.",
          operatorName,
          subtask,
          checkpointId);
    }

    // This should be the happy path where all subtasks reports are received
    if (inProgressSubtaskSet.size() == parallelism) {
      completedStatistics = completedStatistics();
      resetAggregates();
      LOG.info(
          "Received data statistics from all {} operators {} for checkpoint {}. Return last completed aggregator.",
          parallelism,
          operatorName,
          inProgressCheckpointId);
    }

    return completedStatistics;
  }

  private boolean inProgress() {
    return inProgressCheckpointId != CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
  }

  private AggregatedStatistics completedStatistics() {
    if (coordinatorStatisticsType == StatisticsType.Map) {
      LOG.info(
          "Completed map statistics aggregation with {} keys", coordinatorMapStatistics.size());
      return AggregatedStatistics.fromKeyFrequency(
          inProgressCheckpointId, coordinatorMapStatistics);
    } else {
      ReservoirItemsSketch<SortKey> sketch = coordinatorSketchStatistics.getResult();
      LOG.info(
          "Completed sketch statistics aggregation: "
              + "reservoir size = {}, number of items seen = {}, number of samples = {}",
          sketch.getK(),
          sketch.getN(),
          sketch.getNumSamples());
      return AggregatedStatistics.fromRangeBounds(
          inProgressCheckpointId,
          SketchUtil.rangeBounds(downstreamParallelism, comparator, sketch));
    }
  }

  private void initializeAggregates(long checkpointId, DataStatistics taskStatistics) {
    LOG.info("Starting a new statistics aggregation for checkpoint {}", checkpointId);
    this.inProgressCheckpointId = checkpointId;
    this.coordinatorStatisticsType = taskStatistics.type();

    if (coordinatorStatisticsType == StatisticsType.Map) {
      this.coordinatorMapStatistics = Maps.newHashMap();
      this.coordinatorSketchStatistics = null;
    } else {
      this.coordinatorMapStatistics = null;
      this.coordinatorSketchStatistics =
          ReservoirItemsUnion.newInstance(
              SketchUtil.determineCoordinatorReservoirSize(downstreamParallelism));
    }
  }

  private void resetAggregates() {
    inProgressSubtaskSet.clear();
    this.inProgressCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
    this.coordinatorMapStatistics = null;
    this.coordinatorSketchStatistics = null;
  }

  @SuppressWarnings("unchecked")
  private void merge(DataStatistics taskStatistics) {
    if (taskStatistics.type() == StatisticsType.Map) {
      Map<SortKey, Long> taskMapStats = (Map<SortKey, Long>) taskStatistics.result();
      if (coordinatorStatisticsType == StatisticsType.Map) {
        taskMapStats.forEach((key, count) -> coordinatorMapStatistics.merge(key, count, Long::sum));
        if (statisticsType == StatisticsType.Auto
            && coordinatorMapStatistics.size() > switchToSketchThreshold) {
          convertCoordinatorToSketch();
        }
      } else {
        // convert task stats to sketch first
        ReservoirItemsSketch<SortKey> taskSketch =
            ReservoirItemsSketch.newInstance(
                SketchUtil.determineOperatorReservoirSize(parallelism, downstreamParallelism));
        SketchUtil.convertMapToSketch(taskMapStats, taskSketch::update);
        coordinatorSketchStatistics.update(taskSketch);
      }
    } else {
      ReservoirItemsSketch<SortKey> taskSketch =
          (ReservoirItemsSketch<SortKey>) taskStatistics.result();
      if (coordinatorStatisticsType == StatisticsType.Map) {
        // convert global stats to sketch first
        convertCoordinatorToSketch();
      }

      coordinatorSketchStatistics.update(taskSketch);
    }
  }

  private void convertCoordinatorToSketch() {
    this.coordinatorSketchStatistics =
        ReservoirItemsUnion.newInstance(
            SketchUtil.determineCoordinatorReservoirSize(downstreamParallelism));
    SketchUtil.convertMapToSketch(coordinatorMapStatistics, coordinatorSketchStatistics::update);
    this.coordinatorStatisticsType = StatisticsType.Sketch;
    this.coordinatorMapStatistics = null;
  }

  @VisibleForTesting
  Set<Integer> inProgressSubtaskSet() {
    return inProgressSubtaskSet;
  }

  @VisibleForTesting
  long inProgressCheckpointId() {
    return inProgressCheckpointId;
  }

  @VisibleForTesting
  StatisticsType coordinatorStatisticsType() {
    return coordinatorStatisticsType;
  }

  @VisibleForTesting
  Map<SortKey, Long> coordinatorMapStatistics() {
    return coordinatorMapStatistics;
  }

  @VisibleForTesting
  ReservoirItemsUnion<SortKey> coordinatorSketchStatistics() {
    return coordinatorSketchStatistics;
  }
}
