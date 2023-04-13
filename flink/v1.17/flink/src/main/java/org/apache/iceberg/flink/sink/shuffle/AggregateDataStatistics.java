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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

class AggregateDataStatistics<K> implements Serializable {

  private final long checkpointId;
  private final DataStatistics<K> dataStatistics;
  private final Set<Integer> subtaskSet = Sets.newHashSet();

  AggregateDataStatistics(long checkpoint, final DataStatisticsFactory<K> statisticsFactory) {
    this.checkpointId = checkpoint;
    this.dataStatistics = statisticsFactory.createDataStatistics();
  }

  long checkpointId() {
    return checkpointId;
  }

  DataStatistics<K> dataStatistics() {
    return dataStatistics;
  }

  void mergeDataStatistic(int subtask, DataStatisticsEvent<K> event) {
    Preconditions.checkArgument(
        checkpointId == event.checkpointId(),
        "Received unexpected event from checkpoint %s. Expected checkpoint %s",
        event.checkpointId(),
        checkpointId);
    if (!subtaskSet.add(subtask)) {
      return;
    }

    dataStatistics.merge(event.dataStatistics());
  }

  long aggregateSize() {
    return subtaskSet.size();
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
