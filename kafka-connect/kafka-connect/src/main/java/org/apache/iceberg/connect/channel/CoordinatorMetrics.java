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
package org.apache.iceberg.connect.channel;

import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Sensor;

class CoordinatorMetrics extends ChannelMetrics {

  private static final String GROUP = "coordinator-metrics";
  // The coordinator is a single per-connector task, so it reports a fixed task tag.
  private static final String TASK = "coordinator";
  private static final String FULL = "full";
  private static final String PARTIAL = "partial";

  // Commit timers are tagged by commitMode (partial vs full) so the two paths stay separable.
  private final Sensor fullCommitTime;
  private final Sensor partialCommitTime;
  private final Sensor startCommit;
  private final Sensor commitComplete;

  CoordinatorMetrics(
      String connector, Supplier<Long> commitBufferSize, Supplier<Long> readyBufferSize) {
    super(GROUP, connector, TASK);
    try {
      this.fullCommitTime =
          createTimerSensor(
              "commit-time",
              "Time spent in Coordinator.commit() in microseconds",
              metricTags(connector, TASK, FULL));
      this.partialCommitTime =
          createTimerSensor(
              "commit-time",
              "Time spent in Coordinator.commit() in microseconds",
              metricTags(connector, TASK, PARTIAL));
      // Counters are bumped only after send() succeeds, so they count events successfully emitted;
      // a failed send leaves them unmoved even though the commit is already in progress.
      this.startCommit =
          createCounterSensor(
              "start-commit",
              "Number of successfully emitted START_COMMIT events",
              metricTags(connector, TASK, null));
      this.commitComplete =
          createCounterSensor(
              "commit-complete",
              "Number of successfully emitted COMMIT_COMPLETE events",
              metricTags(connector, TASK, null));

      addGauge(
          "commit-buffer-size",
          "Current size of CommitState.commitBuffer",
          metricTags(connector, TASK, null),
          commitBufferSize);
      addGauge(
          "ready-buffer-size",
          "Current size of CommitState.readyBuffer",
          metricTags(connector, TASK, null),
          readyBufferSize);
    } catch (RuntimeException e) {
      closeQuietly(e);
      throw e;
    }
  }

  void recordCommit(boolean partialCommit, long elapsedMs) {
    (partialCommit ? partialCommitTime : fullCommitTime).record((double) elapsedMs);
  }

  void incStartCommit() {
    startCommit.record(1);
  }

  void incCommitComplete() {
    commitComplete.record(1);
  }
}
