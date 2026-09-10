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
package org.apache.iceberg.spark.source.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.junit.jupiter.api.Test;

public class TestNanoDurationMetric {

  private final CustomSumMetric metric = new WriteDuration();

  @Test
  public void testFormatsLargestMeaningfulUnit() {
    assertThat(metric.aggregateTaskMetrics(new long[] {300L, 400L}))
        .isEqualTo("total (min, med, max)\n700 ns (300 ns, 400 ns, 400 ns)");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_500L, 2_500L}))
        .isEqualTo("total (min, med, max)\n4 us (1 us, 2 us, 2 us)");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_000_000L, 500_000L}))
        .isEqualTo("total (min, med, max)\n1 ms (500 us, 1 ms, 1 ms)");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_500_000_000L, 1_000_000_000L}))
        .isEqualTo("total (min, med, max)\n2.5 s (1.0 s, 1.5 s, 1.5 s)");
  }

  @Test
  public void testUnitBoundaries() {
    assertThat(total(999L)).isEqualTo("999 ns");
    assertThat(total(1_000L)).isEqualTo("1 us");
    assertThat(total(999_999L)).isEqualTo("999 us");
    assertThat(total(1_000_000L)).isEqualTo("1 ms");
    assertThat(total(999_999_999L)).isEqualTo("999 ms");
    assertThat(total(1_000_000_000L)).isEqualTo("1.0 s");
  }

  @Test
  public void testAggregateTaskMetricsWithNoTasks() {
    assertThat(metric.aggregateTaskMetrics(new long[] {})).isEqualTo("0 ns");
  }

  @Test
  public void testAggregateTaskMetricsSurfacesStragglers() {
    // the total alone would hide that one task took the bulk of the time
    assertThat(metric.aggregateTaskMetrics(new long[] {1_000_000L, 1_000_000L, 8_000_000L}))
        .isEqualTo("total (min, med, max)\n10 ms (1 ms, 1 ms, 8 ms)");
  }

  @Test
  public void testAggregateTaskMetricsDoesNotMutateInput() {
    long[] taskMetrics = new long[] {3_000_000L, 1_000_000L, 2_000_000L};
    metric.aggregateTaskMetrics(taskMetrics);
    assertThat(taskMetrics).containsExactly(3_000_000L, 1_000_000L, 2_000_000L);
  }

  @Test
  public void testAggregateTaskMetricsIsLocaleIndependent() {
    Locale previous = Locale.getDefault();
    try {
      // a comma-decimal locale would render "2,5 s" without an explicit Locale.ROOT
      Locale.setDefault(Locale.GERMANY);
      assertThat(metric.aggregateTaskMetrics(new long[] {2_500_000_000L}))
          .isEqualTo("total (min, med, max)\n2.5 s (2.5 s, 2.5 s, 2.5 s)");
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  public void testMetricNames() {
    assertThat(new WriteDuration().name()).isEqualTo("writeDuration");
    assertThat(new WriteCloseDuration().name()).isEqualTo("writeCloseDuration");
  }

  private String total(long nanos) {
    String line = metric.aggregateTaskMetrics(new long[] {nanos});
    line = line.substring(line.indexOf('\n') + 1);
    return line.substring(0, line.indexOf(" ("));
  }
}
