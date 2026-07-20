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

import java.util.Map;
import org.apache.kafka.common.metrics.Sensor;

class WorkerMetrics extends ChannelMetrics {

  private static final String GROUP = "worker-metrics";

  private final Sensor saveTime;
  private final Sensor dataWritten;
  private final Sensor dataComplete;

  WorkerMetrics(String connector, String task) {
    super(GROUP, connector, task);
    Map<String, String> tags = metricTags(connector, task, null);
    try {
      this.saveTime =
          createTimerSensor("save-time", "Time spent in Worker.save() in microseconds", tags);
      // Counters are bumped only after send() succeeds, so they count events successfully emitted;
      // a failed send leaves them unmoved even though the files are already on disk.
      this.dataWritten =
          createCounterSensor(
              "data-written",
              "Number of data/delete files in successfully emitted DATA_WRITTEN events",
              tags);
      this.dataComplete =
          createCounterSensor(
              "data-complete", "Number of successfully emitted DATA_COMPLETE events", tags);
    } catch (RuntimeException e) {
      closeQuietly(e);
      throw e;
    }
  }

  void recordSave(long elapsedMs) {
    saveTime.record((double) elapsedMs);
  }

  void incDataWritten(long count) {
    dataWritten.record((double) count);
  }

  void incDataComplete() {
    dataComplete.record(1);
  }
}
