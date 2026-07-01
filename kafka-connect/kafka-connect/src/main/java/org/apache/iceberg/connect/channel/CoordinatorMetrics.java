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
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
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

class CoordinatorMetrics implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorMetrics.class);
  private static final String GROUP = "coordinator-metrics";
  private static final String NAMESPACE = "iceberg-kafka-connect-metrics";

  private final Metrics metrics;
  private final Sensor commitTime;
  private final Sensor consumeTime;
  private final Sensor startCommit;
  private final Sensor commitComplete;

  CoordinatorMetrics(
      String connector, Supplier<Long> commitBufferSize, Supplier<Long> readyBufferSize) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("connector", connector);

    Metrics newMetrics =
        new Metrics(
            new MetricConfig(),
            Collections.singletonList(new JmxReporter()),
            Time.SYSTEM,
            new KafkaMetricsContext(NAMESPACE));
    try {
      this.commitTime =
          createTimerSensor(
              newMetrics, "commit-time", "Time spent in Coordinator.commit() in ms", tags);
      this.consumeTime =
          createTimerSensor(
              newMetrics,
              "consume-available-time",
              "Time spent in Channel.consumeAvailable() (coordinator side) in ms",
              tags);
      this.startCommit =
          createCounterSensor(
              newMetrics, "start-commit", "Number of START_COMMIT events emitted", tags);
      this.commitComplete =
          createCounterSensor(
              newMetrics, "commit-complete", "Number of COMMIT_COMPLETE events emitted", tags);

      newMetrics.addMetric(
          new MetricName(
              "commit-buffer-size", GROUP, "Current size of CommitState.commitBuffer", tags),
          (Gauge<Long>) (config, now) -> commitBufferSize.get());
      newMetrics.addMetric(
          new MetricName(
              "ready-buffer-size", GROUP, "Current size of CommitState.readyBuffer", tags),
          (Gauge<Long>) (config, now) -> readyBufferSize.get());
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

  void recordCommit(long elapsedMs) {
    commitTime.record((double) elapsedMs);
  }

  void recordConsume(long elapsedMs) {
    consumeTime.record((double) elapsedMs);
  }

  void incStartCommit() {
    startCommit.record(1);
  }

  void incCommitComplete() {
    commitComplete.record(1);
  }

  @Override
  public void close() {
    try {
      metrics.close();
    } catch (Exception e) {
      LOG.warn("Error closing CoordinatorMetrics", e);
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
