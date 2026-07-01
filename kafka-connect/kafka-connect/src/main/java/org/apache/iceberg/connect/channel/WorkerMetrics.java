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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WorkerMetrics implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerMetrics.class);
  private static final String GROUP = "worker-metrics";
  private static final String NAMESPACE = "iceberg-kafka-connect-metrics";

  private final Metrics metrics;
  private final Sensor saveTime;
  private final Sensor consumeTime;
  private final Sensor dataWritten;
  private final Sensor dataComplete;

  WorkerMetrics(String connector, String taskId) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("connector", connector);
    tags.put("task", taskId);

    Metrics newMetrics =
        new Metrics(
            new MetricConfig(),
            Collections.singletonList(new JmxReporter()),
            Time.SYSTEM,
            new KafkaMetricsContext(NAMESPACE));
    try {
      this.saveTime =
          createTimerSensor(newMetrics, "save-time", "Time spent in Worker.save() in ms", tags);
      this.consumeTime =
          createTimerSensor(
              newMetrics,
              "consume-available-time",
              "Time spent in Channel.consumeAvailable() (worker side) in ms",
              tags);
      this.dataWritten =
          createCounterSensor(
              newMetrics, "data-written", "Number of DATA_WRITTEN events emitted", tags);
      this.dataComplete =
          createCounterSensor(
              newMetrics, "data-complete", "Number of DATA_COMPLETE events emitted", tags);
    } catch (RuntimeException e) {
      try {
        newMetrics.close();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    this.metrics = newMetrics;
  }

  void recordSave(long elapsedMs) {
    saveTime.record((double) elapsedMs);
  }

  void recordConsume(long elapsedMs) {
    consumeTime.record((double) elapsedMs);
  }

  void incDataWritten(long count) {
    if (count > 0) {
      dataWritten.record((double) count);
    }
  }

  void incDataComplete() {
    dataComplete.record(1);
  }

  @Override
  public void close() {
    try {
      metrics.close();
    } catch (Exception e) {
      LOG.warn("Error closing WorkerMetrics", e);
    }
  }

  private Sensor createTimerSensor(
      Metrics registry, String baseName, String description, Map<String, String> tags) {
    Sensor sensor = registry.sensor(baseName);
    sensor.add(new MetricName(baseName + "-avg", GROUP, description + " (avg)", tags), new Avg());
    sensor.add(new MetricName(baseName + "-max", GROUP, description + " (max)", tags), new Max());
    sensor.add(
        new MetricName(baseName + "-total", GROUP, description + " (total)", tags),
        new CumulativeSum());
    return sensor;
  }

  private Sensor createCounterSensor(
      Metrics registry, String baseName, String description, Map<String, String> tags) {
    Sensor sensor = registry.sensor(baseName);
    sensor.add(new MetricName(baseName + "-total", GROUP, description, tags), new CumulativeSum());
    return sensor;
  }
}
