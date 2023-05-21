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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GlobalStatisticsAggregator is used by {@link DataStatisticsCoordinator} to collect {@link
 * DataStatistics} from {@link DataStatisticsOperator} subtasks for specific checkpoint. It stores
 * the merged {@link DataStatistics} result and uses set to keep a record of all reported subtasks.
 */
@Internal
class GlobalStatisticsAggregatorTracker<D extends DataStatistics<D, S>, S> implements Serializable {
  private static final Logger LOG =
      LoggerFactory.getLogger(GlobalStatisticsAggregatorTracker.class);
  private static final double EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE = 90;
  private final transient TypeSerializer<DataStatistics<D, S>> statisticsSerializer;
  private final transient int parallelism;
  private transient volatile GlobalStatisticsAggregator<D, S> inProgressAggregator;
  private volatile GlobalStatisticsAggregator<D, S> lastCompletedAggregator;

  GlobalStatisticsAggregatorTracker(
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer, int parallelism) {
    this.statisticsSerializer = statisticsSerializer;
    this.parallelism = parallelism;
  }

  boolean receiveDataStatisticEventAndCheckCompletion(
      int subtask, DataStatisticsEvent<D, S> event) {
    long checkpointId = event.checkpointId();

    if (lastCompletedAggregator != null && lastCompletedAggregator.checkpointId() >= checkpointId) {
      LOG.debug(
          "Data statistics aggregation for checkpoint {} has completed. Ignore the event from subtask {} for checkpoint {}",
          lastCompletedAggregator.checkpointId(),
          subtask,
          checkpointId);
      return false;
    }

    if (inProgressAggregator == null) {
      inProgressAggregator = new GlobalStatisticsAggregator<>(checkpointId, statisticsSerializer);
    }

    if (inProgressAggregator.checkpointId() < checkpointId) {
      if ((double) inProgressAggregator.aggregatedSubtasksCount() / parallelism * 100
          >= EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE) {
        lastCompletedAggregator = inProgressAggregator;
        LOG.info(
            "Received data statistics from {} operators out of total {} for checkpoint {}. "
                + "It's more than the expected percentage {}. Update last completed aggregator to be "
                + " {}.",
            inProgressAggregator.aggregatedSubtasksCount(),
            parallelism,
            inProgressAggregator.checkpointId(),
            EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE,
            lastCompletedAggregator);
        inProgressAggregator = new GlobalStatisticsAggregator<>(checkpointId, statisticsSerializer);
        inProgressAggregator.mergeDataStatistic(
            subtask, event.checkpointId(), event.dataStatistics(statisticsSerializer));
        return true;
      } else {
        LOG.info(
            "Received data statistics from {} operators out of total {} for checkpoint {}. "
                + "It's less than the expected percentage {}. Dropping the incomplete aggregate "
                + "data statistics {} and starting collecting data statistics from new checkpoint {}",
            inProgressAggregator.aggregatedSubtasksCount(),
            parallelism,
            inProgressAggregator.checkpointId(),
            EXPECTED_DATA_STATISTICS_RECEIVED_PERCENTAGE,
            inProgressAggregator,
            checkpointId);
        inProgressAggregator = new GlobalStatisticsAggregator<>(checkpointId, statisticsSerializer);
      }
    } else if (inProgressAggregator.checkpointId() > checkpointId) {
      LOG.debug(
          "Expect data statistics for checkpoint {}, but receive event from older checkpoint {}. Ignore it.",
          inProgressAggregator.checkpointId(),
          checkpointId);
      return false;
    }

    inProgressAggregator.mergeDataStatistic(
        subtask, event.checkpointId(), event.dataStatistics(statisticsSerializer));

    if (inProgressAggregator.aggregatedSubtasksCount() == parallelism) {
      lastCompletedAggregator = inProgressAggregator;
      LOG.info(
          "Received data statistics from all {} operators for checkpoint {}. Update last completed aggregator to be {}.",
          parallelism,
          inProgressAggregator.checkpointId(),
          lastCompletedAggregator.dataStatistics());
      inProgressAggregator = null;
      return true;
    }
    return false;
  }

  @VisibleForTesting
  GlobalStatisticsAggregator<D, S> inProgressAggregator() {
    return inProgressAggregator;
  }

  GlobalStatisticsAggregator<D, S> lastCompletedAggregator() {
    return lastCompletedAggregator;
  }

  byte[] serializeLastCompletedAggregator() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bytes);

    DataOutputSerializer outSerializer = new DataOutputSerializer(64);
    out.writeLong(lastCompletedAggregator.checkpointId());
    statisticsSerializer.serialize(lastCompletedAggregator.dataStatistics(), outSerializer);
    byte[] statisticsBytes = outSerializer.getCopyOfBuffer();
    out.writeInt(statisticsBytes.length);
    out.write(statisticsBytes);
    out.writeObject(lastCompletedAggregator.subtaskSet());
    out.flush();

    return bytes.toByteArray();
  }

  @SuppressWarnings("unchecked")
  void deserializeLastCompletedAggregator(byte[] checkpointData)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bytes = new ByteArrayInputStream(checkpointData);
    ObjectInputStream in = new ObjectInputStream(bytes);

    long completedCheckpointId = in.readLong();
    int statisticsBytesLength = in.readInt();
    byte[] statisticsBytes = new byte[statisticsBytesLength];
    DataInputDeserializer input =
        new DataInputDeserializer(statisticsBytes, 0, statisticsBytesLength);
    final DataStatistics<D, S> dataStatistics = statisticsSerializer.deserialize(input);
    Set<Integer> subtaskSet = (Set<Integer>) in.readObject();

    lastCompletedAggregator =
        new GlobalStatisticsAggregator<>(completedCheckpointId, dataStatistics, subtaskSet);
  }
}
