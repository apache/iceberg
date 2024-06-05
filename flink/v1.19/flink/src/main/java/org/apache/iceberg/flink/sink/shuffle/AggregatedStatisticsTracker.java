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
import java.util.NavigableMap;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
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

  private final String operatorName;
  private final int parallelism;
  private final TypeSerializer<DataStatistics> statisticsSerializer;
  private final int downstreamParallelism;
  private final StatisticsType statisticsType;
  private final int switchToSketchThreshold;
  private final Comparator<StructLike> comparator;
  private final NavigableMap<Long, Aggregation> aggregationsPerCheckpoint;

  private AggregatedStatistics completedStatistics;

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
    this.completedStatistics = restoredStatistics;

    this.comparator = SortOrderComparators.forSchema(schema, sortOrder);
    this.aggregationsPerCheckpoint = Maps.newTreeMap();
  }

  AggregatedStatistics updateAndCheckCompletion(int subtask, StatisticsEvent event) {
    long checkpointId = event.checkpointId();
    LOG.debug(
        "Handling statistics event from subtask {} of operator {} for checkpoint {}",
        subtask,
        operatorName,
        checkpointId);

    if (completedStatistics != null && completedStatistics.checkpointId() > checkpointId) {
      LOG.info(
          "Ignore stale statistics event from operator {} subtask {} for older checkpoint {}. "
              + "Was expecting data statistics from checkpoint higher than {}",
          operatorName,
          subtask,
          checkpointId,
          completedStatistics.checkpointId());
      return null;
    }

    Aggregation aggregation =
        aggregationsPerCheckpoint.computeIfAbsent(
            checkpointId,
            ignored ->
                new Aggregation(
                    parallelism,
                    downstreamParallelism,
                    switchToSketchThreshold,
                    comparator,
                    statisticsType,
                    StatisticsUtil.collectType(statisticsType, completedStatistics)));
    DataStatistics dataStatistics =
        StatisticsUtil.deserializeDataStatistics(event.statisticsBytes(), statisticsSerializer);
    if (!aggregation.merge(subtask, dataStatistics)) {
      LOG.debug(
          "Ignore duplicate data statistics from operator {} subtask {} for checkpoint {}.",
          operatorName,
          subtask,
          checkpointId);
    }

    if (aggregation.isComplete()) {
      this.completedStatistics = aggregation.completedStatistics(checkpointId);
      // clean up aggregations up to the completed checkpoint id
      aggregationsPerCheckpoint.headMap(checkpointId, true).clear();
      return completedStatistics;
    }

    return null;
  }

  @VisibleForTesting
  NavigableMap<Long, Aggregation> aggregationsPerCheckpoint() {
    return aggregationsPerCheckpoint;
  }

  static class Aggregation {
    private static final Logger LOG = LoggerFactory.getLogger(Aggregation.class);

    private final Set<Integer> subtaskSet;
    private final int parallelism;
    private final int downstreamParallelism;
    private final int switchToSketchThreshold;
    private final Comparator<StructLike> comparator;
    private final StatisticsType configuredType;
    private StatisticsType currentType;
    private Map<SortKey, Long> mapStatistics;
    private ReservoirItemsUnion<SortKey> sketchStatistics;

    Aggregation(
        int parallelism,
        int downstreamParallelism,
        int switchToSketchThreshold,
        Comparator<StructLike> comparator,
        StatisticsType configuredType,
        StatisticsType currentType) {
      this.subtaskSet = Sets.newHashSet();
      this.parallelism = parallelism;
      this.downstreamParallelism = downstreamParallelism;
      this.switchToSketchThreshold = switchToSketchThreshold;
      this.comparator = comparator;
      this.configuredType = configuredType;
      this.currentType = currentType;

      if (currentType == StatisticsType.Map) {
        this.mapStatistics = Maps.newHashMap();
        this.sketchStatistics = null;
      } else {
        this.mapStatistics = null;
        this.sketchStatistics =
            ReservoirItemsUnion.newInstance(
                SketchUtil.determineCoordinatorReservoirSize(downstreamParallelism));
      }
    }

    @VisibleForTesting
    Set<Integer> subtaskSet() {
      return subtaskSet;
    }

    @VisibleForTesting
    StatisticsType currentType() {
      return currentType;
    }

    @VisibleForTesting
    Map<SortKey, Long> mapStatistics() {
      return mapStatistics;
    }

    @VisibleForTesting
    ReservoirItemsUnion<SortKey> sketchStatistics() {
      return sketchStatistics;
    }

    private boolean isComplete() {
      return subtaskSet.size() == parallelism;
    }

    /** @return false if duplicate */
    private boolean merge(int subtask, DataStatistics taskStatistics) {
      if (subtaskSet.contains(subtask)) {
        return false;
      }

      subtaskSet.add(subtask);
      merge(taskStatistics);
      return true;
    }

    @SuppressWarnings("unchecked")
    private void merge(DataStatistics taskStatistics) {
      if (taskStatistics.type() == StatisticsType.Map) {
        Map<SortKey, Long> taskMapStats = (Map<SortKey, Long>) taskStatistics.result();
        if (currentType == StatisticsType.Map) {
          taskMapStats.forEach((key, count) -> mapStatistics.merge(key, count, Long::sum));
          if (configuredType == StatisticsType.Auto
              && mapStatistics.size() > switchToSketchThreshold) {
            convertCoordinatorToSketch();
          }
        } else {
          // convert task stats to sketch first
          ReservoirItemsSketch<SortKey> taskSketch =
              ReservoirItemsSketch.newInstance(
                  SketchUtil.determineOperatorReservoirSize(parallelism, downstreamParallelism));
          SketchUtil.convertMapToSketch(taskMapStats, taskSketch::update);
          sketchStatistics.update(taskSketch);
        }
      } else {
        ReservoirItemsSketch<SortKey> taskSketch =
            (ReservoirItemsSketch<SortKey>) taskStatistics.result();
        if (currentType == StatisticsType.Map) {
          // convert global stats to sketch first
          convertCoordinatorToSketch();
        }

        sketchStatistics.update(taskSketch);
      }
    }

    private void convertCoordinatorToSketch() {
      this.sketchStatistics =
          ReservoirItemsUnion.newInstance(
              SketchUtil.determineCoordinatorReservoirSize(downstreamParallelism));
      SketchUtil.convertMapToSketch(mapStatistics, sketchStatistics::update);
      this.currentType = StatisticsType.Sketch;
      this.mapStatistics = null;
    }

    private AggregatedStatistics completedStatistics(long checkpointId) {
      if (currentType == StatisticsType.Map) {
        LOG.info("Completed map statistics aggregation with {} keys", mapStatistics.size());
        return AggregatedStatistics.fromKeyFrequency(checkpointId, mapStatistics);
      } else {
        ReservoirItemsSketch<SortKey> sketch = sketchStatistics.getResult();
        LOG.info(
            "Completed sketch statistics aggregation: "
                + "reservoir size = {}, number of items seen = {}, number of samples = {}",
            sketch.getK(),
            sketch.getN(),
            sketch.getNumSamples());
        return AggregatedStatistics.fromRangeBounds(
            checkpointId, SketchUtil.rangeBounds(downstreamParallelism, comparator, sketch));
      }
    }
  }
}
