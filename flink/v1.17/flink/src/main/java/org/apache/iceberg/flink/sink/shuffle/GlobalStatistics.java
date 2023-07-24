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

import java.io.Serializable;
import java.util.Set;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GlobalStatistics is used by {@link DataStatisticsCoordinator} to collect {@link DataStatistics}
 * from {@link DataStatisticsOperator} subtasks for specific checkpoint. It stores the merged {@link
 * DataStatistics} result and uses set to keep a record of all reported subtasks.
 */
class GlobalStatistics<D extends DataStatistics<D, S>, S> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalStatistics.class);

  private final long checkpointId;
  private final DataStatistics<D, S> dataStatistics;
  private final Set<Integer> subtaskSet;

  GlobalStatistics(long checkpoint, TypeSerializer<DataStatistics<D, S>> statisticsSerializer) {
    this.checkpointId = checkpoint;
    this.dataStatistics = statisticsSerializer.createInstance();
    this.subtaskSet = Sets.newHashSet();
  }

  GlobalStatistics(long checkpoint, DataStatistics<D, S> dataStatistics, Set<Integer> subtaskSet) {
    this.checkpointId = checkpoint;
    this.dataStatistics = dataStatistics;
    this.subtaskSet = subtaskSet;
  }

  long checkpointId() {
    return checkpointId;
  }

  DataStatistics<D, S> dataStatistics() {
    return dataStatistics;
  }

  void mergeDataStatistic(
      String operatorName, int subtask, long eventCheckpointId, D eventDataStatistics) {
    Preconditions.checkArgument(
        checkpointId == eventCheckpointId,
        "Received unexpected event from operator %s checkpoint %s. Expected checkpoint %s",
        operatorName,
        eventCheckpointId,
        checkpointId);
    if (!subtaskSet.add(subtask)) {
      LOG.debug(
          "Ignore duplicated data statistics from operator {} subtask {} for checkpoint {}.",
          operatorName,
          subtask,
          checkpointId);
      return;
    }

    dataStatistics.merge(eventDataStatistics);
  }

  int aggregatedSubtasksCount() {
    return subtaskSet.size();
  }

  Set<Integer> subtaskSet() {
    return subtaskSet;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("checkpointId", checkpointId)
        .add("dataStatistics", dataStatistics)
        .add("subtaskSet", subtaskSet)
        .toString();
  }
}
