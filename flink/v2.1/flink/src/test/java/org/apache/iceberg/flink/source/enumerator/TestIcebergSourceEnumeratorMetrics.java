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
package org.apache.iceberg.flink.source.enumerator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ImmutableScanMetricsResult;
import org.apache.iceberg.metrics.ImmutableTimerResult;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.TimerResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestIcebergSourceEnumeratorMetrics {

  @Test
  public void testMetricsRegistration() {
    TestingEnumeratorMetricGroup metricGroup = new TestingEnumeratorMetricGroup();
    IcebergSourceEnumeratorMetrics enumeratorMetrics =
        new IcebergSourceEnumeratorMetrics(metricGroup, "test.db.table");

    assertThat(enumeratorMetrics).isNotNull();
    assertThat(metricGroup.registeredGauges()).hasSize(17);
    assertThat(metricGroup.registeredGauges())
        .containsKey("lastScanPlanningDurationMs")
        .containsKey("scannedDataManifests")
        .containsKey("skippedDataManifests")
        .containsKey("totalDataManifests")
        .containsKey("scannedDeleteManifests")
        .containsKey("skippedDeleteManifests")
        .containsKey("totalDeleteManifests")
        .containsKey("resultDataFiles")
        .containsKey("skippedDataFiles")
        .containsKey("resultDeleteFiles")
        .containsKey("skippedDeleteFiles")
        .containsKey("indexedDeleteFiles")
        .containsKey("equalityDeleteFiles")
        .containsKey("positionalDeleteFiles")
        .containsKey("dvs")
        .containsKey("totalFileSizeInBytes")
        .containsKey("totalDeleteFileSizeInBytes");
  }

  @Test
  public void testUpdateWithScanMetrics() {
    TestingEnumeratorMetricGroup metricGroup = new TestingEnumeratorMetricGroup();
    IcebergSourceEnumeratorMetrics enumeratorMetrics =
        new IcebergSourceEnumeratorMetrics(metricGroup, "test.db.table");

    ScanMetricsResult scanMetrics =
        ImmutableScanMetricsResult.builder()
            .totalPlanningDuration(timerResult(150))
            .scannedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 10))
            .skippedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 5))
            .totalDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 15))
            .scannedDeleteManifests(CounterResult.of(MetricsContext.Unit.COUNT, 3))
            .skippedDeleteManifests(CounterResult.of(MetricsContext.Unit.COUNT, 2))
            .totalDeleteManifests(CounterResult.of(MetricsContext.Unit.COUNT, 5))
            .resultDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 100))
            .skippedDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 50))
            .resultDeleteFiles(CounterResult.of(MetricsContext.Unit.COUNT, 7))
            .skippedDeleteFiles(CounterResult.of(MetricsContext.Unit.COUNT, 3))
            .indexedDeleteFiles(CounterResult.of(MetricsContext.Unit.COUNT, 4))
            .equalityDeleteFiles(CounterResult.of(MetricsContext.Unit.COUNT, 2))
            .positionalDeleteFiles(CounterResult.of(MetricsContext.Unit.COUNT, 1))
            .dvs(CounterResult.of(MetricsContext.Unit.COUNT, 3))
            .totalFileSizeInBytes(CounterResult.of(MetricsContext.Unit.BYTES, 1024000))
            .totalDeleteFileSizeInBytes(CounterResult.of(MetricsContext.Unit.BYTES, 512000))
            .build();

    enumeratorMetrics.updateWithScanMetrics(scanMetrics);

    assertGaugeValue(metricGroup, "lastScanPlanningDurationMs", 150L);
    assertGaugeValue(metricGroup, "scannedDataManifests", 10L);
    assertGaugeValue(metricGroup, "skippedDataManifests", 5L);
    assertGaugeValue(metricGroup, "totalDataManifests", 15L);
    assertGaugeValue(metricGroup, "scannedDeleteManifests", 3L);
    assertGaugeValue(metricGroup, "skippedDeleteManifests", 2L);
    assertGaugeValue(metricGroup, "totalDeleteManifests", 5L);
    assertGaugeValue(metricGroup, "resultDataFiles", 100L);
    assertGaugeValue(metricGroup, "skippedDataFiles", 50L);
    assertGaugeValue(metricGroup, "resultDeleteFiles", 7L);
    assertGaugeValue(metricGroup, "skippedDeleteFiles", 3L);
    assertGaugeValue(metricGroup, "indexedDeleteFiles", 4L);
    assertGaugeValue(metricGroup, "equalityDeleteFiles", 2L);
    assertGaugeValue(metricGroup, "positionalDeleteFiles", 1L);
    assertGaugeValue(metricGroup, "dvs", 3L);
    assertGaugeValue(metricGroup, "totalFileSizeInBytes", 1024000L);
    assertGaugeValue(metricGroup, "totalDeleteFileSizeInBytes", 512000L);
  }

  @Test
  public void testGaugesShowLatestScanValues() {
    TestingEnumeratorMetricGroup metricGroup = new TestingEnumeratorMetricGroup();
    IcebergSourceEnumeratorMetrics enumeratorMetrics =
        new IcebergSourceEnumeratorMetrics(metricGroup, "test.db.table");

    ScanMetricsResult firstScan =
        ImmutableScanMetricsResult.builder()
            .totalPlanningDuration(timerResult(100))
            .resultDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 3))
            .scannedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 5))
            .build();

    ScanMetricsResult secondScan =
        ImmutableScanMetricsResult.builder()
            .totalPlanningDuration(timerResult(200))
            .resultDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 7))
            .scannedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 2))
            .build();

    enumeratorMetrics.updateWithScanMetrics(firstScan);
    enumeratorMetrics.updateWithScanMetrics(secondScan);

    // Gauges show last-scan values, NOT accumulated
    assertGaugeValue(metricGroup, "lastScanPlanningDurationMs", 200L);
    assertGaugeValue(metricGroup, "resultDataFiles", 7L);
    assertGaugeValue(metricGroup, "scannedDataManifests", 2L);
  }

  @Test
  public void testUpdateWithNullMetricsIsNoOp() {
    TestingEnumeratorMetricGroup metricGroup = new TestingEnumeratorMetricGroup();
    IcebergSourceEnumeratorMetrics enumeratorMetrics =
        new IcebergSourceEnumeratorMetrics(metricGroup, "test.db.table");

    enumeratorMetrics.updateWithScanMetrics(null);

    assertGaugeValue(metricGroup, "resultDataFiles", 0L);
    assertGaugeValue(metricGroup, "scannedDataManifests", 0L);
    assertGaugeValue(metricGroup, "totalDataManifests", 0L);
  }

  @Test
  public void testPartialNullFieldsInScanMetrics() {
    TestingEnumeratorMetricGroup metricGroup = new TestingEnumeratorMetricGroup();
    IcebergSourceEnumeratorMetrics enumeratorMetrics =
        new IcebergSourceEnumeratorMetrics(metricGroup, "test.db.table");

    // Only some fields populated, rest are null
    ScanMetricsResult partialMetrics =
        ImmutableScanMetricsResult.builder()
            .resultDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 10))
            .totalDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 5))
            .build();

    enumeratorMetrics.updateWithScanMetrics(partialMetrics);

    assertGaugeValue(metricGroup, "resultDataFiles", 10L);
    assertGaugeValue(metricGroup, "totalDataManifests", 5L);
    // Null fields remain at zero
    assertGaugeValue(metricGroup, "skippedDataManifests", 0L);
    assertGaugeValue(metricGroup, "totalFileSizeInBytes", 0L);
    assertGaugeValue(metricGroup, "skippedDeleteFiles", 0L);
  }

  @SuppressWarnings("unchecked")
  private static void assertGaugeValue(
      TestingEnumeratorMetricGroup metricGroup, String name, Long expected) {
    Gauge<Long> gauge = (Gauge<Long>) metricGroup.registeredGauges().get(name);
    assertThat(gauge).as("Gauge '%s' should be registered", name).isNotNull();
    assertThat(gauge.getValue()).isEqualTo(expected);
  }

  private static TimerResult timerResult(long durationMs) {
    return ImmutableTimerResult.builder()
        .timeUnit(TimeUnit.NANOSECONDS)
        .totalDuration(Duration.ofMillis(durationMs))
        .count(1)
        .build();
  }

  /**
   * A testing metric group that captures registered gauges for verification. Follows the pattern
   * from {@code TestingMetricGroup} in the reader package.
   */
  static class TestingEnumeratorMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Counter> counters;
    private final Map<String, Gauge<?>> gauges;

    TestingEnumeratorMetricGroup() {
      this.counters = Maps.newHashMap();
      this.gauges = Maps.newHashMap();
    }

    private TestingEnumeratorMetricGroup(
        Map<String, Counter> counters, Map<String, Gauge<?>> gauges) {
      this.counters = counters;
      this.gauges = gauges;
    }

    Map<String, Counter> counters() {
      return counters;
    }

    Map<String, Gauge<?>> registeredGauges() {
      return gauges;
    }

    @Override
    public Counter counter(String name) {
      Counter counter = new SimpleCounter();
      counters.put(name, counter);
      return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }

    @Override
    public MetricGroup addGroup(String name) {
      return new TestingEnumeratorMetricGroup(counters, gauges);
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
      return new TestingEnumeratorMetricGroup(counters, gauges);
    }
  }
}
