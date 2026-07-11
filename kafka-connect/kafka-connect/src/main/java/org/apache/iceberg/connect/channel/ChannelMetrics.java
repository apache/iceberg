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
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for the per-task/per-connector JMX metric registries. Owns the {@link Metrics} registry, the
 * sensor-creation helpers shared by the worker and coordinator, and the two channel-level timers
 * that every {@link Channel} feeds while draining the control topic.
 */
abstract class ChannelMetrics implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelMetrics.class);

  /** JMX domain; dotted to sit alongside Kafka's own {@code kafka.connect.*} beans in jconsole. */
  static final String NAMESPACE = "iceberg.kafka.connect";

  private final Metrics metrics;
  private final String group;

  private final Sensor messageReadTime;
  private final Sensor messageProcessTime;

  ChannelMetrics(String group, String connector, String task) {
    this.group = group;
    this.metrics =
        new Metrics(
            new MetricConfig(),
            Collections.singletonList(new JmxReporter()),
            Time.SYSTEM,
            new KafkaMetricsContext(NAMESPACE));
    Map<String, String> tags = metricTags(connector, task, null);
    try {
      this.messageReadTime =
          createTimerSensor(
              "channel-message-read-time", "Time to Avro-decode one control message in ms", tags);
      this.messageProcessTime =
          createTimerSensor(
              "channel-message-process-time", "Time to process one control message in ms", tags);
    } catch (RuntimeException e) {
      closeQuietly(e);
      throw e;
    }
  }

  void recordMessageRead(long elapsedMs) {
    messageReadTime.record((double) elapsedMs);
  }

  void recordMessageProcess(long elapsedMs) {
    messageProcessTime.record((double) elapsedMs);
  }

  @Override
  public void close() {
    try {
      metrics.close();
    } catch (Exception e) {
      LOG.warn("Error closing {}", getClass().getSimpleName(), e);
    }
  }

  /**
   * Closes the registry while an in-flight constructor failure is unwinding, to avoid MBean leaks.
   */
  protected void closeQuietly(RuntimeException failure) {
    try {
      metrics.close();
    } catch (Exception suppressed) {
      failure.addSuppressed(suppressed);
    }
  }

  /** Registers a lazily-evaluated gauge that reads {@code supplier} each time JMX polls it. */
  protected void addGauge(
      String name, String description, Map<String, String> tags, Supplier<Long> supplier) {
    metrics.addMetric(
        new MetricName(name, group, description, tags),
        (Gauge<Long>) (config, now) -> supplier.get());
  }

  protected Sensor createTimerSensor(
      String baseName, String description, Map<String, String> tags) {
    Sensor sensor = metrics.sensor(sensorName(baseName, tags));
    sensor.add(new MetricName(baseName + "-avg", group, description + " (avg)", tags), new Avg());
    sensor.add(new MetricName(baseName + "-max", group, description + " (max)", tags), new Max());
    sensor.add(
        new MetricName(baseName + "-total", group, description + " (total)", tags),
        new CumulativeSum());
    sensor.add(
        new MetricName(baseName + "-count", group, description + " (count)", tags),
        new CumulativeCount());
    return sensor;
  }

  protected Sensor createCounterSensor(
      String baseName, String description, Map<String, String> tags) {
    Sensor sensor = metrics.sensor(sensorName(baseName, tags));
    sensor.add(new MetricName(baseName + "-total", group, description, tags), new CumulativeSum());
    return sensor;
  }

  /**
   * Builds the JMX tag map shared by worker and coordinator metrics. {@code commitMode} is only set
   * on the coordinator's per-commit sensors ({@code full}/{@code partial}); pass {@code null} to
   * omit it.
   */
  static Map<String, String> metricTags(String connector, String task, String commitMode) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("connector", connector);
    tags.put("task", task);
    if (commitMode != null) {
      tags.put("commitMode", commitMode);
    }
    return tags;
  }

  // Sensor registry names are global per Metrics instance, so distinct tag sets (e.g. the
  // partial/full commit timers) need distinct sensor names to avoid clobbering each other.
  private static String sensorName(String baseName, Map<String, String> tags) {
    StringBuilder sb = new StringBuilder(baseName);
    tags.forEach((k, v) -> sb.append('.').append(k).append('.').append(v));
    return sb.toString();
  }
}
